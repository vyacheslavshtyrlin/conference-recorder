import { afterEach, describe, expect, it, vi } from "vitest";

import { AttemptManager, CapacityError, type BrowserHandle, type BrowserLauncher } from "./attemptManager.js";
import type { AttemptRequest } from "./contracts.js";
import type { RecordingSink } from "./recordingSink.js";

function request(recordingId = "rec_one", attemptId = "attempt-one"): AttemptRequest {
  return {
    attemptId,
    roomId: "room-one",
    recordingId,
    integrationId: "local-fixture",
    requestedAt: "2026-07-22T12:00:00.000Z",
    protocolVersion: "recording-bot.v1",
    signalingUrl: "ws://signaling:4000/ws",
    admission: "secret-admission",
    workerId: "worker-test",
    workerGenerationId: "generation-test",
  };
}

function fixture(sink?: RecordingSink, startTimeoutMs = 1000) {
  const stop = vi.fn<BrowserHandle["stop"]>().mockResolvedValue(undefined);
  let exit: ((error?: Error) => void) | undefined;
  const launcher: BrowserLauncher = {
    start: vi.fn(async ({ onExit }) => { exit = onExit; return { stop }; }),
  };
  const manager = new AttemptManager(launcher, { publicOrigin: "http://127.0.0.1:8090", maxSlots: 1, startTimeoutMs, stopTimeoutMs: 1000 }, sink);
  return { manager, launcher, stop, exit: (error?: Error) => exit?.(error) };
}

afterEach(() => vi.useRealTimers());

describe("AttemptManager", () => {
  it("starts an attempt idempotently without exposing admission in its snapshot", async () => {
    const { manager, launcher } = fixture();
    await expect(manager.create(request())).resolves.toEqual({ state: "starting" });
    await expect(manager.create(request())).resolves.toEqual({ state: "starting" });
    expect(launcher.start).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(manager.snapshot())).not.toContain("secret-admission");
  });

  it("enforces slot capacity and releases the slot after bounded stop", async () => {
    const { manager, stop } = fixture();
    await manager.create(request());
    await expect(manager.create(request("rec_two", "attempt-two"))).rejects.toBeInstanceOf(CapacityError);
    await expect(manager.stop("rec_one")).resolves.toEqual({ state: "stopped" });
    expect(stop).toHaveBeenCalledOnce();
    await expect(manager.create(request("rec_two", "attempt-two"))).resolves.toEqual({ state: "starting" });
  });

  it("fails and cleans up the sink immediately after an unexpected Chromium exit", async () => {
    const sink: RecordingSink = {
      begin: vi.fn().mockResolvedValue({ generation: 1 }),
      appendSegment: vi.fn().mockResolvedValue(undefined),
      finalize: vi.fn().mockResolvedValue({ path: "should-not-exist" }),
      fail: vi.fn().mockResolvedValue(undefined),
    };
    const { manager, exit } = fixture(sink);
    await manager.create(request());
    exit(new Error("crashed"));
    expect(manager.snapshot()).toEqual([expect.objectContaining({ recordingId: "rec_one", state: "failed", failureCode: "BOT_RUNTIME_EXITED" })]);
    await vi.waitFor(() => expect(sink.fail).toHaveBeenCalledWith("rec_one", "BOT_RUNTIME_EXITED", "crashed"));
    await manager.stop("rec_one");
    expect(sink.finalize).not.toHaveBeenCalled();
  });

  it("accepts runtime readiness only with the per-attempt secret", async () => {
    const { manager, launcher } = fixture();
    await manager.create(request());
    const runtimeUrl = new URL((launcher.start as ReturnType<typeof vi.fn>).mock.calls[0]![0].runtimeUrl);
    const token = runtimeUrl.searchParams.get("token")!;
    expect(manager.receiveRuntimeEvent("attempt-one", "wrong", { type: "runtime:ready", producerCount: 2 })).toBe(false);
    expect(manager.receiveRuntimeEvent("attempt-one", token, { type: "runtime:ready", producerCount: 2 })).toBe(true);
    expect(manager.snapshot()[0]?.state).toBe("capturing");
  });

  it("marks a failed chunk append incomplete and never finalizes its prefix", async () => {
    const sink: RecordingSink = {
      begin: vi.fn().mockResolvedValue({ generation: 1 }),
      appendSegment: vi.fn().mockRejectedValue(new Error("disk full")),
      finalize: vi.fn().mockResolvedValue({ path: "should-not-exist" }),
      fail: vi.fn().mockResolvedValue(undefined),
    };
    const { manager, launcher } = fixture(sink);
    await manager.create(request());
    const runtimeUrl = new URL((launcher.start as ReturnType<typeof vi.fn>).mock.calls[0]![0].runtimeUrl);
    const token = runtimeUrl.searchParams.get("token")!;
    manager.receiveRuntimeEvent("attempt-one", token, { type: "runtime:ready", producerCount: 1 });

    await expect(manager.appendRuntimeChunk("attempt-one", token, Buffer.from("partial"))).rejects.toThrow("disk full");
    expect(manager.snapshot()[0]).toMatchObject({ state: "failed", failureCode: "BOT_CAPTURE_INCOMPLETE" });
    expect(manager.receiveRuntimeEvent("attempt-one", token, { type: "runtime:ready", producerCount: 1 })).toBe(false);
    expect(manager.getRuntimeConfig("attempt-one", token)).toBeUndefined();
    await manager.stop("rec_one");
    expect(sink.finalize).not.toHaveBeenCalled();
    expect(sink.fail).toHaveBeenCalledWith("rec_one", "BOT_CAPTURE_INCOMPLETE", expect.any(String));
  });

  it("accepts the MediaRecorder tail chunk while a bounded stop is in progress", async () => {
    let releaseStop!: () => void;
    const sink: RecordingSink = {
      begin: vi.fn().mockResolvedValue({ generation: 1 }),
      appendSegment: vi.fn().mockResolvedValue(undefined),
      finalize: vi.fn().mockResolvedValue({ path: "final/rec_one.mp4" }),
      fail: vi.fn().mockResolvedValue(undefined),
    };
    const { manager, launcher, stop } = fixture(sink);
    stop.mockImplementationOnce(() => new Promise<void>((resolve) => { releaseStop = resolve; }));
    await manager.create(request());
    const runtimeUrl = new URL((launcher.start as ReturnType<typeof vi.fn>).mock.calls[0]![0].runtimeUrl);
    const token = runtimeUrl.searchParams.get("token")!;

    const stopping = manager.stop("rec_one");
    await expect(manager.appendRuntimeChunk("attempt-one", token, Buffer.from("tail"))).resolves.toBe(true);
    releaseStop();
    await expect(stopping).resolves.toEqual({ state: "stopped" });

    expect(sink.appendSegment).toHaveBeenCalledWith(
      "rec_one",
      expect.objectContaining({
        generation: 1,
        sequence: 0,
        bytes: Buffer.from("tail"),
      }),
    );
    expect(sink.finalize).toHaveBeenCalledWith("rec_one");
  });

  it("stops a launcher handle that resolves after the startup timeout", async () => {
    vi.useFakeTimers();
    const stop = vi.fn().mockResolvedValue(undefined);
    let resolveLaunch!: (handle: BrowserHandle) => void;
    const launcher: BrowserLauncher = {
      start: vi.fn(() => new Promise<BrowserHandle>((resolve) => { resolveLaunch = resolve; })),
    };
    const manager = new AttemptManager(launcher, { publicOrigin: "http://127.0.0.1:8090", maxSlots: 1, startTimeoutMs: 1000, stopTimeoutMs: 1000 });
    const creation = manager.create(request());
    const rejected = expect(creation).rejects.toThrow("Chromium start timed out");
    await vi.advanceTimersByTimeAsync(1000);
    await rejected;
    resolveLaunch({ stop });
    await Promise.resolve();
    await Promise.resolve();
    expect(stop).toHaveBeenCalledOnce();
  });

  it("retries finalization without failing durable segments between attempts", async () => {
    const sink: RecordingSink = {
      begin: vi.fn().mockResolvedValue({ generation: 1 }),
      appendSegment: vi.fn().mockResolvedValue(undefined),
      finalize: vi
        .fn()
        .mockRejectedValueOnce(new Error("ffmpeg transient failure"))
        .mockResolvedValueOnce({ path: "final/rec_one.mp4" }),
      fail: vi.fn().mockResolvedValue(undefined),
    };
    const { manager } = fixture(sink);
    await manager.create(request());
    await expect(manager.stop("rec_one")).resolves.toEqual({ state: "stopped" });
    expect(sink.finalize).toHaveBeenCalledTimes(2);
    expect(sink.fail).not.toHaveBeenCalled();
  });

  it("accepts a fresh admission as a new generation after Chromium exits", async () => {
    const sink: RecordingSink = {
      begin: vi
        .fn()
        .mockResolvedValueOnce({ generation: 1 })
        .mockResolvedValueOnce({ generation: 2 }),
      appendSegment: vi.fn().mockResolvedValue(undefined),
      finalize: vi.fn().mockResolvedValue({ path: "final/rec_one.mp4" }),
      fail: vi.fn().mockResolvedValue(undefined),
    };
    const { manager, exit, launcher } = fixture(sink);
    await manager.create(request());
    exit(new Error("Chromium killed"));
    await vi.waitFor(() =>
      expect(manager.snapshot()[0]).toMatchObject({
        state: "failed",
        recoverable: true,
      }),
    );
    await expect(manager.create(request("rec_one", "attempt-two"))).resolves.toEqual({
      state: "starting",
    });
    expect(sink.begin).toHaveBeenCalledTimes(2);
    expect(launcher.start).toHaveBeenCalledTimes(2);
    expect(manager.snapshot()[0]).toMatchObject({
      attemptId: "attempt-two",
      state: "starting",
      recoverable: false,
    });
  });

  it("fails closed after bounded persistent finalization failures", async () => {
    const sink: RecordingSink = {
      begin: vi.fn().mockResolvedValue({ generation: 1 }),
      appendSegment: vi.fn().mockResolvedValue(undefined),
      finalize: vi.fn().mockRejectedValue(new Error("ffmpeg permanently failed")),
      fail: vi.fn().mockResolvedValue(undefined),
    };
    const stop = vi.fn<BrowserHandle["stop"]>().mockResolvedValue(undefined);
    const launcher: BrowserLauncher = {
      start: vi.fn(async () => ({ stop })),
    };
    const manager = new AttemptManager(
      launcher,
      {
        publicOrigin: "http://127.0.0.1:8090",
        maxSlots: 1,
        startTimeoutMs: 1000,
        stopTimeoutMs: 1000,
        finalizeMaxAttempts: 2,
        finalizeRetryDelayMs: 0,
      },
      sink,
    );
    await manager.create(request());
    await expect(manager.stop("rec_one")).rejects.toThrow(
      "ffmpeg permanently failed",
    );
    expect(sink.finalize).toHaveBeenCalledTimes(2);
    expect(sink.fail).toHaveBeenCalledWith(
      "rec_one",
      "BOT_FINALIZATION_FAILED",
      expect.any(String),
    );
  });
});
