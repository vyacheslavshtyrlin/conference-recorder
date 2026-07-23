import { createHash } from "node:crypto";
import { mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { AttemptRequest } from "./contracts.js";
import { LocalRecordingSink, type ProcessRunner } from "./recordingSink.js";

const roots: string[] = [];
afterEach(async () => {
  const { rm } = await import("node:fs/promises");
  await Promise.all(
    roots.splice(0).map((root) => rm(root, { recursive: true, force: true })),
  );
});

async function fixture(probe = validProbe(), runnerOverride?: ProcessRunner) {
  const root = await mkdtemp(join(tmpdir(), "recording-bot-test-"));
  roots.push(root);
  const runner: ProcessRunner =
    runnerOverride ?? {
      run: vi.fn(async (command, args) => {
        if (command === "ffmpeg")
          await writeFile(args.at(-1)!, Buffer.from("verified-mp4"));
        return { stdout: command === "ffprobe" ? probe : "" };
      }),
    };
  return {
    root,
    runner,
    sink: new LocalRecordingSink(
      root,
      "ffmpeg",
      "ffprobe",
      1000,
      runner,
      undefined,
      owner(),
    ),
  };
}

describe("LocalRecordingSink segmented recovery", () => {
  it("checkpoints immutable checksummed segments and publishes one verified MP4", async () => {
    const { root, sink, runner } = await fixture();
    const { generation } = await sink.begin(request());
    await append(sink, generation, 0, "webm-one", 0, 1000);
    await append(sink, generation, 1, "webm-two", 1000, 2000);
    const manifestBefore = await manifest(root);
    expect(manifestBefore).toMatchObject({
      version: 2,
      status: "capturing",
      sourceBytes: 16,
      segments: [
        { generation: 1, sequence: 0, timelineStartMs: 0, timelineEndMs: 1000 },
        { generation: 1, sequence: 1, timelineStartMs: 1000, timelineEndMs: 2000 },
      ],
    });
    expect(manifestBefore.segments[0].sha256).toBe(
      createHash("sha256").update("webm-one").digest("hex"),
    );
    await expect(sink.finalize("rec_local")).resolves.toEqual({
      path: join(root, "final", "rec_local.mp4"),
    });
    expect(await readFile(join(root, "final", "rec_local.mp4"), "utf8")).toBe(
      "verified-mp4",
    );
    expect(await manifest(root)).toMatchObject({
      status: "ready",
      artifact: { width: 1280, height: 720, frameRate: 24 },
    });
    expect(runner.run).toHaveBeenCalledTimes(2);
  });

  it("recovers on disk without Redis and records a generation discontinuity", async () => {
    const { root, sink, runner } = await fixture();
    const first = await sink.begin(request());
    await append(sink, first.generation, 0, "generation-one", 0, 1000);
    await sink.fail("rec_local", "BOT_RUNTIME_EXITED", "Chromium killed");

    const restarted = new LocalRecordingSink(
      root,
      "ffmpeg",
      "ffprobe",
      1000,
      runner,
      undefined,
      owner(),
    );
    const second = await restarted.begin(request("attempt-recovered"));
    expect(second.generation).toBe(2);
    await append(restarted, second.generation, 0, "generation-two", 0, 1000);
    expect(await manifest(root)).toMatchObject({
      generations: [
        { generation: 1, recovered: false },
        { generation: 2, recovered: true },
      ],
      discontinuities: [{ afterGeneration: 1, beforeGeneration: 2 }],
    });
    await expect(restarted.finalize("rec_local")).resolves.toBeDefined();
  });

  it("adopts an exact orphan segment left between durable write and manifest checkpoint", async () => {
    const { root, sink } = await fixture();
    const { generation } = await sink.begin(request());
    const bytes = Buffer.from("durable-orphan");
    const path = join(
      root,
      "recordings",
      "rec_local",
      "segments",
      "g0001-s00000000.webm",
    );
    await writeFile(path, bytes);
    await sink.appendSegment("rec_local", {
      generation,
      sequence: 0,
      timelineStartMs: 0,
      timelineEndMs: 1000,
      bytes,
    });
    expect((await manifest(root)).segments).toHaveLength(1);
    await expect(
      sink.appendSegment("rec_local", {
        generation,
        sequence: 1,
        timelineStartMs: 1000,
        timelineEndMs: 2000,
        bytes: Buffer.from("next"),
      }),
    ).resolves.toBeUndefined();
  });

  it("preserves segments after FFmpeg failure and finalization retry succeeds", async () => {
    let ffmpegRuns = 0;
    const runner: ProcessRunner = {
      run: vi.fn(async (command, args) => {
        if (command === "ffmpeg" && ++ffmpegRuns === 1)
          throw new Error("ffmpeg crashed");
        if (command === "ffmpeg")
          await writeFile(args.at(-1)!, Buffer.from("verified-mp4"));
        return { stdout: command === "ffprobe" ? validProbe() : "" };
      }),
    };
    const { root, sink } = await fixture(validProbe(), runner);
    const { generation } = await sink.begin(request());
    await append(sink, generation, 0, "valid-segment", 0, 1000);
    await expect(sink.finalize("rec_local")).rejects.toThrow("ffmpeg crashed");
    await expect(
      stat(
        join(
          root,
          "recordings",
          "rec_local",
          "segments",
          "g0001-s00000000.webm",
        ),
      ),
    ).resolves.toBeDefined();
    await expect(sink.finalize("rec_local")).resolves.toBeDefined();
  });

  it("fails closed when a completed segment is corrupted", async () => {
    const { root, sink } = await fixture();
    const { generation } = await sink.begin(request());
    await append(sink, generation, 0, "valid-segment", 0, 1000);
    await writeFile(
      join(
        root,
        "recordings",
        "rec_local",
        "segments",
        "g0001-s00000000.webm",
      ),
      "corrupt",
    );
    await expect(sink.finalize("rec_local")).rejects.toThrow(
      "checksum mismatch",
    );
    await expect(stat(join(root, "final", "rec_local.mp4"))).rejects.toMatchObject({
      code: "ENOENT",
    });
  });

  it("keeps the total duration bound across recovered generations", async () => {
    const { root, runner } = await fixture();
    const sink = new LocalRecordingSink(
      root,
      "ffmpeg",
      "ffprobe",
      1000,
      runner,
      { maxSourceBytes: 1000, maxDurationMs: 1 },
      owner(),
    );
    const first = await sink.begin(request());
    await new Promise((resolve) => setTimeout(resolve, 5));
    await expect(
      append(sink, first.generation, 0, "late", 0, 1),
    ).rejects.toThrow("duration limit");
  });

  it("fences a stale worker after a new worker generation claims the recording", async () => {
    const { root, sink, runner } = await fixture();
    const first = await sink.begin(request());
    await append(sink, first.generation, 0, "owned-first", 0, 1000);
    const replacement = new LocalRecordingSink(
      root,
      "ffmpeg",
      "ffprobe",
      1000,
      runner,
      undefined,
      owner("generation-new"),
    );
    await replacement.begin(request("attempt-new", "generation-new"));
    await expect(
      append(sink, first.generation, 1, "stale-write", 1000, 2000),
    ).rejects.toThrow("fence lost");
    await expect(sink.finalize("rec_local")).rejects.toThrow("fence lost");
  });

  it("recovers a pending finalization after process restart without Chromium", async () => {
    const { root, sink, runner } = await fixture();
    const first = await sink.begin(request());
    await append(sink, first.generation, 0, "durable", 0, 1000);
    await sink.fail("rec_local", "BOT_FINALIZATION_FAILED", "worker restarted");
    const restarted = new LocalRecordingSink(
      root,
      "ffmpeg",
      "ffprobe",
      1000,
      runner,
      undefined,
      owner("generation-finalizer"),
    );
    await expect(restarted.listPendingFinalizations()).resolves.toEqual([
      "rec_local",
    ]);
    await expect(restarted.recoverFinalization("rec_local")).resolves.toEqual({
      path: join(root, "final", "rec_local.mp4"),
    });
    const repeated = new LocalRecordingSink(
      root,
      "ffmpeg",
      "ffprobe",
      1000,
      runner,
      undefined,
      owner("generation-repeat"),
    );
    await expect(repeated.recoverFinalization("rec_local")).resolves.toEqual({
      path: join(root, "final", "rec_local.mp4"),
    });
  });

  it("rejects a positive but truncated MP4 duration", async () => {
    const { sink } = await fixture(
      JSON.stringify({
        streams: [
          {
            codec_type: "video",
            width: 1280,
            height: 720,
            avg_frame_rate: "24/1",
          },
          { codec_type: "audio" },
        ],
        format: { duration: "1" },
      }),
    );
    const first = await sink.begin(request());
    await append(sink, first.generation, 0, "ten-seconds", 0, 10_000);
    await expect(sink.finalize("rec_local")).rejects.toThrow(
      "verification failed",
    );
  });

  it("removes untracked segment files during restart recovery", async () => {
    const { root, sink, runner } = await fixture();
    await sink.begin(request());
    await sink.fail("rec_local", "BOT_RUNTIME_EXITED", "restart");
    const orphanPath = join(
      root,
      "recordings",
      "rec_local",
      "segments",
      "untracked.webm",
    );
    await writeFile(orphanPath, Buffer.alloc(500));
    const restarted = new LocalRecordingSink(
      root,
      "ffmpeg",
      "ffprobe",
      1000,
      runner,
      undefined,
      owner("generation-cleanup"),
    );
    await restarted.begin(request("attempt-cleanup", "generation-cleanup"));
    await expect(stat(orphanPath)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("prevents a stale finalizer from publishing or cleaning the new generation", async () => {
    let releaseProbe!: () => void;
    let probeStarted!: () => void;
    const probeGate = new Promise<void>((resolve) => {
      releaseProbe = resolve;
    });
    const probeObserved = new Promise<void>((resolve) => {
      probeStarted = resolve;
    });
    let probeCalls = 0;
    const runner: ProcessRunner = {
      run: vi.fn(async (command, args) => {
        if (command === "ffmpeg")
          await writeFile(args.at(-1)!, Buffer.from("verified-mp4"));
        if (command === "ffprobe" && probeCalls++ === 0) {
          probeStarted();
          await probeGate;
        }
        return { stdout: command === "ffprobe" ? validProbe() : "" };
      }),
    };
    const { root, sink } = await fixture(validProbe(), runner);
    const first = await sink.begin(request());
    await append(sink, first.generation, 0, "durable", 0, 1000);
    const staleFinalization = sink.finalize("rec_local");
    await probeObserved;

    const replacement = new LocalRecordingSink(
      root,
      "ffmpeg",
      "ffprobe",
      1000,
      runner,
      undefined,
      owner("generation-race-winner"),
    );
    await replacement.begin(
      request("attempt-race-winner", "generation-race-winner"),
    );
    releaseProbe();
    await expect(staleFinalization).rejects.toThrow("fence lost");
    await expect(stat(join(root, "final", "rec_local.mp4"))).rejects.toMatchObject({
      code: "ENOENT",
    });
    await expect(replacement.finalize("rec_local")).resolves.toBeDefined();
  });
});

async function append(
  sink: LocalRecordingSink,
  generation: number,
  sequence: number,
  value: string,
  timelineStartMs: number,
  timelineEndMs: number,
): Promise<void> {
  return sink.appendSegment("rec_local", {
    generation,
    sequence,
    timelineStartMs,
    timelineEndMs,
    bytes: Buffer.from(value),
  });
}

async function manifest(root: string): Promise<any> {
  return JSON.parse(
    await readFile(
      join(root, "recordings", "rec_local", "manifest.json"),
      "utf8",
    ),
  );
}

function request(
  attemptId = "attempt-local",
  workerGenerationId = "generation-test",
): AttemptRequest {
  return {
    attemptId,
    roomId: "room-local",
    recordingId: "rec_local",
    integrationId: "fixture",
    requestedAt: "2026-07-22T12:00:00.000Z",
    protocolVersion: "recording-bot.v1",
    signalingUrl: "ws://signaling:4000/ws",
    admission: "secret",
    workerId: "worker-test",
    workerGenerationId,
  };
}

function validProbe(): string {
  return JSON.stringify({
    streams: [
      {
        codec_type: "video",
        width: 1280,
        height: 720,
        avg_frame_rate: "24/1",
      },
      { codec_type: "audio" },
    ],
    format: { duration: "2.5" },
  });
}

function owner(generationId = "generation-test") {
  return { workerId: "worker-test", generationId };
}
