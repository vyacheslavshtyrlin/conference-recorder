import { randomBytes } from "node:crypto";

import type { AttemptRequest, AttemptState, RuntimeEvent } from "./contracts.js";
import { RecordingAttempt } from "./recordingAttempt.js";
import type { RecordingSink } from "./recordingSink.js";

export type BrowserHandle = { stop(): Promise<void> };
export type BrowserLauncher = {
  start(input: { attemptId: string; runtimeUrl: string; signal: AbortSignal; onExit: (error?: Error) => void }): Promise<BrowserHandle>;
};

type Attempt = {
  request: AttemptRequest;
  runtimeToken: string;
  lifecycle: RecordingAttempt;
  handle?: BrowserHandle;
  operation: Promise<void>;
  generation: number;
  nextSequence: number;
  generationStartedAtMs: number;
  lastTimelineEndMs: number;
  recoveryDeadlineAt?: number;
};

export class AttemptManager {
  private readonly attempts = new Map<string, Attempt>();

  constructor(
    private readonly launcher: BrowserLauncher,
    private readonly options: {
      publicOrigin: string;
      maxSlots: number;
      startTimeoutMs: number;
      stopTimeoutMs: number;
      finalizeMaxAttempts?: number;
      finalizeRetryDelayMs?: number;
      recoveryTimeoutMs?: number;
    },
    private readonly sink?: RecordingSink,
  ) {}

  async create(request: AttemptRequest): Promise<{ state: AttemptState }> {
    const existing = this.attempts.get(request.recordingId);
    if (existing) {
      if (sameAttempt(existing.request, request)) return { state: existing.lifecycle.state };
      if (existing.lifecycle.state !== "failed")
        throw new AttemptConflictError("recordingId is already assigned to another attempt");
      if (
        existing.recoveryDeadlineAt !== undefined &&
        Date.now() > existing.recoveryDeadlineAt
      )
        throw new AttemptConflictError("recording recovery window expired");
      await existing.operation.catch(() => undefined);
      await existing.handle?.stop().catch(() => undefined);
      this.attempts.delete(request.recordingId);
    }
    if (this.activeCount() >= this.options.maxSlots) throw new CapacityError("recording bot capacity is exhausted");

    const generation = (await this.sink?.begin(request))?.generation ?? 1;
    const attempt: Attempt = {
      request,
      runtimeToken: randomBytes(32).toString("base64url"),
      lifecycle: new RecordingAttempt(),
      operation: Promise.resolve(),
      generation,
      nextSequence: 0,
      generationStartedAtMs: Date.now(),
      lastTimelineEndMs: 0,
    };
    this.attempts.set(request.recordingId, attempt);
    attempt.operation = this.start(attempt);
    await attempt.operation;
    return { state: attempt.lifecycle.state };
  }

  async stop(recordingId: string): Promise<{ state: AttemptState | "stopped" }> {
    const attempt = this.attempts.get(recordingId);
    if (!attempt) return { state: "stopped" };
    if (attempt.lifecycle.state === "stopping") {
      await attempt.operation;
      return { state: "stopped" };
    }
    const previousOperation = attempt.operation;
    attempt.lifecycle.transition({ type: "stop-requested" });
    attempt.operation = previousOperation.catch(() => undefined).then(() => this.stopAttempt(attempt));
    await attempt.operation;
    return { state: "stopped" };
  }

  getRuntimeConfig(attemptId: string, runtimeToken: string): AttemptRequest | undefined {
    const attempt = [...this.attempts.values()].find((candidate) => candidate.request.attemptId === attemptId);
    return attempt?.runtimeToken === runtimeToken && attempt.lifecycle.acceptsRuntimeAccess && attempt.lifecycle.state !== "stopping" ? attempt.request : undefined;
  }

  receiveRuntimeEvent(attemptId: string, runtimeToken: string, event: RuntimeEvent): boolean {
    const attempt = [...this.attempts.values()].find((candidate) => candidate.request.attemptId === attemptId);
    if (!attempt || attempt.runtimeToken !== runtimeToken || attempt.lifecycle.state === "failed") return false;
    if (attempt.lifecycle.state === "stopping" && event.type === "runtime:ready") return false;
    if (event.type === "runtime:ready") {
      if (!attempt.lifecycle.transition({ type: "runtime-ready" })) return false;
      console.info(JSON.stringify({ timestamp: new Date().toISOString(), severity: "info", service: "recording-bot", operation: "attempt.capturing", attemptId, recordingId: attempt.request.recordingId, producerCount: event.producerCount }));
    }
    if (event.type === "runtime:failed") {
      attempt.lifecycle.transition({ type: "failed", code: event.code });
      if (event.code === "BOT_RUNTIME_EXITED")
        attempt.recoveryDeadlineAt =
          Date.now() + (this.options.recoveryTimeoutMs ?? 30_000);
      attempt.operation = attempt.operation.catch(() => undefined).then(async () => {
        await this.sink?.fail(attempt.request.recordingId, event.code, event.message).catch(() => undefined);
        await attempt.handle?.stop().catch(() => undefined);
      });
    }
    return true;
  }

  snapshot(): Array<{ recordingId: string; roomId: string; attemptId: string; state: AttemptState; recoverable: boolean; failureCode?: string }> {
    return [...this.attempts.values()].map((attempt) => ({
      recordingId: attempt.request.recordingId,
      roomId: attempt.request.roomId,
      attemptId: attempt.request.attemptId,
      state: attempt.lifecycle.state,
      recoverable:
        attempt.lifecycle.state === "failed" &&
        attempt.lifecycle.failureCode === "BOT_RUNTIME_EXITED" &&
        (attempt.recoveryDeadlineAt ?? 0) >= Date.now(),
      ...(attempt.lifecycle.failureCode ? { failureCode: attempt.lifecycle.failureCode } : {}),
    }));
  }

  occupiedSlots(): number {
    return this.activeCount();
  }

  async recoverFinalization(recordingId: string): Promise<{ path: string }> {
    if (!this.sink?.recoverFinalization)
      throw new Error("recording sink is unavailable");
    return this.sink.recoverFinalization(recordingId);
  }

  async pendingFinalizations(): Promise<string[]> {
    return (await this.sink?.listPendingFinalizations?.()) ?? [];
  }

  async close(): Promise<void> {
    await Promise.allSettled([...this.attempts.keys()].map((recordingId) => this.stop(recordingId)));
  }

  async appendRuntimeChunk(attemptId: string, runtimeToken: string, chunk: Buffer): Promise<boolean> {
    const attempt = [...this.attempts.values()].find((candidate) => candidate.request.attemptId === attemptId);
    if (!attempt || attempt.runtimeToken !== runtimeToken || attempt.lifecycle.state === "failed") return false;
    try {
      const timelineEndMs = Date.now() - attempt.generationStartedAtMs;
      await this.sink?.appendSegment(attempt.request.recordingId, {
        generation: attempt.generation,
        sequence: attempt.nextSequence,
        timelineStartMs: attempt.lastTimelineEndMs,
        timelineEndMs,
        bytes: chunk,
      });
      attempt.nextSequence += 1;
      attempt.lastTimelineEndMs = timelineEndMs;
    } catch (error) {
      attempt.lifecycle.transition({ type: "failed", code: "BOT_CAPTURE_INCOMPLETE" });
      await this.sink?.fail(attempt.request.recordingId, attempt.lifecycle.failureCode!, String(error)).catch(() => undefined);
      void attempt.handle?.stop().catch(() => undefined);
      throw error;
    }
    console.info(JSON.stringify({ timestamp: new Date().toISOString(), severity: "info", service: "recording-bot", operation: "attempt.chunk", attemptId, recordingId: attempt.request.recordingId, bytes: chunk.length }));
    return true;
  }

  private async start(attempt: Attempt): Promise<void> {
    try {
      const runtimeUrl = new URL(`/runtime/${encodeURIComponent(attempt.request.attemptId)}`, this.options.publicOrigin);
      runtimeUrl.searchParams.set("token", attempt.runtimeToken);
      const controller = new AbortController();
      let timedOut = false;
      const launch = this.launcher.start({
        attemptId: attempt.request.attemptId,
        runtimeUrl: runtimeUrl.toString(),
        signal: controller.signal,
        onExit: (error) => this.onBrowserExit(attempt, error),
      });
      const timer = setTimeout(() => { timedOut = true; controller.abort(); }, this.options.startTimeoutMs);
      try {
        attempt.handle = await Promise.race([
          launch,
          new Promise<never>((_, reject) => controller.signal.addEventListener("abort", () => reject(new Error("Chromium start timed out")), { once: true })),
        ]);
      } finally {
        clearTimeout(timer);
        if (timedOut) void launch.then((handle) => handle.stop()).catch(() => undefined);
      }
    } catch (error) {
      attempt.lifecycle.transition({ type: "failed", code: "BOT_RUNTIME_EXITED" });
      await this.sink?.fail(attempt.request.recordingId, attempt.lifecycle.failureCode!, String(error)).catch(() => undefined);
      throw error;
    }
  }

  private async stopAttempt(attempt: Attempt): Promise<void> {
    try {
      await attempt.handle?.stop();
      if (attempt.lifecycle.failureCode) {
        await this.sink?.fail(attempt.request.recordingId, attempt.lifecycle.failureCode, "recording attempt failed");
      } else {
        attempt.lifecycle.transition({ type: "runtime-stopped" });
        await this.finalizeWithRetry(attempt.request.recordingId);
        attempt.lifecycle.transition({ type: "finalized" });
      }
    } catch (error) {
      attempt.lifecycle.transition({ type: "failed", code: "BOT_FINALIZATION_FAILED" });
      await this.sink?.fail(attempt.request.recordingId, "BOT_FINALIZATION_FAILED", String(error)).catch(() => undefined);
      throw error;
    } finally {
      this.attempts.delete(attempt.request.recordingId);
    }
  }

  private async finalizeWithRetry(recordingId: string): Promise<void> {
    const maxAttempts = this.options.finalizeMaxAttempts ?? 3;
    for (let number = 1; number <= maxAttempts; number += 1) {
      try {
        await this.sink?.finalize(recordingId);
        return;
      } catch (error) {
        if (number === maxAttempts) throw error;
        await new Promise<void>((resolve) =>
          setTimeout(resolve, this.options.finalizeRetryDelayMs ?? 250),
        );
      }
    }
  }

  private onBrowserExit(attempt: Attempt, error?: Error): void {
    if (this.attempts.get(attempt.request.recordingId) !== attempt || attempt.lifecycle.state === "stopping") return;
    attempt.lifecycle.transition({ type: "failed", code: "BOT_RUNTIME_EXITED" });
    attempt.recoveryDeadlineAt =
      Date.now() + (this.options.recoveryTimeoutMs ?? 30_000);
    delete attempt.handle;
    attempt.operation = attempt.operation.catch(() => undefined).then(async () => {
      await this.sink?.fail(attempt.request.recordingId, attempt.lifecycle.failureCode!, error?.message ?? "Chromium exited unexpectedly").catch(() => undefined);
    });
  }

  private activeCount(): number {
    return [...this.attempts.values()].filter((attempt) => attempt.lifecycle.occupiesSlot).length;
  }
}

export class AttemptConflictError extends Error {}
export class CapacityError extends Error {}

function sameAttempt(left: AttemptRequest, right: AttemptRequest): boolean {
  return left.attemptId === right.attemptId && left.recordingId === right.recordingId && left.roomId === right.roomId;
}
