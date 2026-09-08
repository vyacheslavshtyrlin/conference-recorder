import { createHash, randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, open, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { createServer } from "node:http";
import { readConfig } from "./config.js";
import {
  downloadBoundedSource,
  loadCaptureV2Manifest,
  outputEncodingArguments,
  verifyArtifactProbe,
  verifySourceProbe,
  verifyStagedSliceProbe,
  type ManifestPage,
  type ManifestSegment,
  RetryableSourceDownloadError,
} from "./compositionV2.js";
import { executeV2Composition } from "./compositionV2Executor.js";
import { ChildProcessRunner } from "./processRunner.js";

const config = readConfig();
const runner = new ChildProcessRunner();
const root = join(config.storageRoot, "composition");
const maximumOutputBytes = Math.min(config.maxSourceBytes, 20_000_000_000);
const maximumWorkspaceBytes = config.maxSourceBytes * 3 + maximumOutputBytes;
let workerState: "starting" | "idle" | "composing" = "starting";

type Claim = {
  job: {
    jobId: string;
    leaseToken: string;
    leaseExpiresAt: string;
    attempts: number;
    inputProtocolVersion: "recording-capture.v1" | "recording-capture.v2";
  } | null;
};
type V1Segment = {
  generation: number;
  sequence: number;
  timelineStartMs: number;
  timelineEndMs: number;
  bytes: number;
  sha256: string;
  download: { url: string };
};
type ArtifactMetadata = {
  bytes: number;
  sha256: string;
  durationSeconds: number;
  width: number;
  height: number;
  frameRate: number;
};
type CompositionOutput = { path: string; expectedDurationSeconds: number };

class TerminalApiError extends Error {
  constructor(readonly status: number) {
    super(`composition API failed: ${status}`);
  }
}

class RetryableCompositionError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
  }
}

class LeaseGuard {
  private readonly controller = new AbortController();
  private expiresAtMs = 0;
  private expiryTimer: NodeJS.Timeout | undefined;

  constructor(expiresAt: string) {
    this.renew(expiresAt);
  }

  get signal(): AbortSignal { return this.controller.signal; }
  get expiresAt(): number { return this.expiresAtMs; }
  healthy(): boolean { return !this.signal.aborted && Date.now() < this.expiresAtMs; }

  renew(expiresAt: string): void {
    const value = Date.parse(expiresAt);
    if (!Number.isFinite(value) || value <= Date.now()) {
      this.abort(new RetryableCompositionError("composition lease expiry is invalid"));
      return;
    }
    this.expiresAtMs = value;
    if (this.expiryTimer) clearTimeout(this.expiryTimer);
    this.expiryTimer = setTimeout(() => {
      this.abort(new RetryableCompositionError("composition lease expired locally"));
    }, Math.max(1, value - Date.now()));
    this.expiryTimer.unref();
  }

  abort(reason?: unknown): void {
    if (this.expiryTimer) clearTimeout(this.expiryTimer);
    this.expiryTimer = undefined;
    if (!this.signal.aborted) this.controller.abort(reason);
  }
}

async function apiRequest<T>(
  path: string,
  options: { method?: "GET" | "POST"; body?: unknown; retry?: boolean; signal?: AbortSignal } = {},
  lease?: string,
): Promise<T> {
  const method = options.method ?? "POST";
  const maximumAttempts = options.retry === false ? 1 : config.finalizeMaxAttempts;
  let lastError: unknown;
  for (let attempt = 0; attempt < maximumAttempts; attempt += 1) {
    try {
      const response = await fetch(`${config.recordingApiUrl}${path}`, {
        method,
        headers: {
          authorization: `Bearer ${config.compositionServiceSecret}`,
          ...(options.body === undefined ? {} : { "content-type": "application/json" }),
          ...(lease ? { "x-composition-lease": lease } : {}),
        },
        ...(options.body === undefined ? {} : { body: JSON.stringify(options.body) }),
        signal: options.signal
          ? AbortSignal.any([options.signal, AbortSignal.timeout(config.httpTimeoutMs)])
          : AbortSignal.timeout(config.httpTimeoutMs),
      });
      if (response.ok) return await response.json() as T;
      if (response.status < 500 && ![408, 425, 429].includes(response.status)) {
        throw new TerminalApiError(response.status);
      }
      lastError = new Error(`composition API retryable status: ${response.status}`);
    } catch (error) {
      if (error instanceof TerminalApiError) throw error;
      lastError = error;
    }
    if (attempt + 1 < maximumAttempts) {
      await delay(config.finalizeRetryDelayMs * (attempt + 1));
    }
  }
  throw new RetryableCompositionError("composition API retries exhausted", { cause: lastError });
}

async function once(): Promise<void> {
  const claim = await apiRequest<Claim>(
    "/internal/v1/composition-jobs/claim",
    { body: {}, retry: false },
  );
  if (!claim.job) return;
  workerState = "composing";
  const { jobId, leaseToken } = claim.job;
  const jobRoot = join(root, jobId);
  const directory = join(jobRoot, randomUUID());
  const lease = new LeaseGuard(claim.job.leaseExpiresAt);
  const heartbeatWake = new AbortController();
  let heartbeatStopped = false;
  const heartbeat = async () => {
    while (!heartbeatStopped) {
      await delayUntilAborted(heartbeatDelay(lease.expiresAt), heartbeatWake.signal);
      if (heartbeatStopped) return;
      try {
        const renewed = await apiRequest<{ leaseExpiresAt: string }>(
          `/internal/v1/composition-jobs/${jobId}/heartbeat`,
          { body: {}, retry: false, signal: lease.signal },
          leaseToken,
        );
        lease.renew(renewed.leaseExpiresAt);
      } catch (error) {
        lease.abort(error);
        return;
      }
    }
  };
  const heartbeatPromise = heartbeat();

  try {
    await mkdir(directory, { recursive: true, mode: 0o750 });
    const leaseHealthy = () => lease.healthy();
    const output = claim.job.inputProtocolVersion === "recording-capture.v2"
      ? await composeV2(jobId, leaseToken, directory, leaseHealthy, lease.signal)
      : await composeV1(jobId, leaseToken, directory, leaseHealthy, lease.signal);
    const metadata = await verifyOutput(output.path, output.expectedDurationSeconds, lease.signal);
    assertLease(leaseHealthy);
    await uploadArtifact(jobId, leaseToken, output.path, metadata, leaseHealthy, lease.signal);
    assertLease(leaseHealthy);
    await apiRequest(
      `/internal/v1/composition-jobs/${jobId}/complete`,
      { body: metadata, signal: lease.signal },
      leaseToken,
    );
  } catch (error) {
    const retryable = (error instanceof RetryableCompositionError ||
      error instanceof RetryableSourceDownloadError || lease.signal.aborted) &&
      claim.job.attempts < config.finalizeMaxAttempts;
    const endpoint = retryable ? "retry" : "fail";
    await apiRequest(
      `/internal/v1/composition-jobs/${jobId}/${endpoint}`,
      {
        body: {
          code: retryable ? "COMPOSITION_RETRYABLE" : "COMPOSITION_FAILED",
          message: sanitizeError(error),
        },
        retry: false,
      },
      leaseToken,
    ).catch(() => undefined);
    console.error(error);
  } finally {
    heartbeatStopped = true;
    heartbeatWake.abort();
    lease.abort();
    await heartbeatPromise;
    await rm(directory, { recursive: true, force: true });
    await rm(jobRoot, { recursive: false, force: true }).catch(() => undefined);
    workerState = "idle";
  }
}

async function composeV1(
  jobId: string,
  leaseToken: string,
  directory: string,
  leaseHealthy: () => boolean,
  signal: AbortSignal,
): Promise<CompositionOutput> {
  const segments: V1Segment[] = [];
  let cursor: string | undefined;
  for (let pages = 0; pages < 1_000; pages += 1) {
    assertLease(leaseHealthy);
    const page = await apiRequest<{ segments: V1Segment[]; nextCursor: string | null }>(
      `/internal/v1/composition-jobs/${jobId}/segments${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""}`,
      { method: "GET", signal },
      leaseToken,
    );
    if (!Array.isArray(page.segments) || page.segments.length > 100) {
      throw new Error("invalid v1 segment page");
    }
    segments.push(...page.segments);
    if (page.nextCursor === null) break;
    if (!page.nextCursor || page.nextCursor === cursor) throw new Error("v1 segment cursor did not advance");
    cursor = page.nextCursor;
    if (pages === 999) throw new Error("v1 segment page bound exceeded");
  }
  if (!segments.length) throw new Error("composition source is empty");

  const files: string[] = [];
  let totalBytes = 0;
  let expectedStartMs = 0;
  for (const [index, segment] of segments.entries()) {
    validateV1Segment(segment, expectedStartMs);
    totalBytes += segment.bytes;
    if (!Number.isSafeInteger(totalBytes) || totalBytes > config.maxSourceBytes) {
      throw new Error("v1 source byte bound exceeded");
    }
    assertLease(leaseHealthy);
    const response = await fetchSource(segment.download.url, signal);
    const path = join(directory, `v1-${String(index).padStart(6, "0")}.webm`);
    await downloadExact(response, path, segment.bytes, segment.sha256);
    files.push(path);
    expectedStartMs = segment.timelineEndMs;
  }

  const concat = join(directory, "input.txt");
  const output = join(directory, "output.mp4");
  await writeFile(concat, files.map((file) => `file '${escapeConcatPath(file)}'`).join("\n"), { mode: 0o640 });
  await runner.run(config.ffmpegPath, [
    "-hide_banner", "-loglevel", "error", "-y",
    "-f", "concat", "-safe", "0", "-i", concat,
    "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,fps=24",
    ...outputEncodingArguments(maximumOutputBytes), output,
  ], config.ffmpegTimeoutMs, signal);
  return { path: output, expectedDurationSeconds: expectedStartMs / 1_000 };
}

async function composeV2(
  jobId: string,
  leaseToken: string,
  directory: string,
  leaseHealthy: () => boolean,
  signal: AbortSignal,
): Promise<CompositionOutput> {
  const manifest = await loadCaptureV2Manifest(
    (cursor) => apiRequest<ManifestPage>(
      `/internal/v1/composition-jobs/${jobId}/capture-v2/manifest${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""}`,
      { method: "GET", signal },
      leaseToken,
    ),
    { maxSourceBytes: config.maxSourceBytes, maxDurationMs: config.maxDurationMs },
  );
  if (!manifest.segments.length) throw new Error("composition source is empty");
  const paths = new Map<string, string>();
  for (let offset = 0; offset < manifest.segments.length; offset += 100) {
    assertLease(leaseHealthy);
    const batch = manifest.segments.slice(offset, offset + 100);
    const response = await apiRequest<{ downloads: Array<{ uploadId: string; downloadUrl: string }> }>(
      `/internal/v1/composition-jobs/${jobId}/source-downloads`,
      { body: { uploadIds: batch.map((segment) => segment.uploadId) }, signal },
      leaseToken,
    );
    const byId = new Map(response.downloads.map((item) => [item.uploadId, item.downloadUrl]));
    if (byId.size !== batch.length) throw new Error("source download batch mismatch");
    for (const [index, segment] of batch.entries()) {
      assertLease(leaseHealthy);
      const url = byId.get(segment.uploadId);
      if (!url) throw new Error("source download is absent");
      const path = join(directory, `v2-${String(offset + index).padStart(6, "0")}.${segment.container === "matroska" ? "mkv" : "webm"}`);
      const response = await fetchSource(url, signal);
      await downloadBoundedSource(response, segment, path);
      assertLease(leaseHealthy);
      await probeSource(path, segment, signal);
      paths.set(segment.uploadId, path);
    }
  }
  assertLease(leaseHealthy);
  const output = await executeV2Composition({
    manifest,
    sourcePaths: paths,
    directory,
    ffmpegPath: config.ffmpegPath,
    runner,
    timeoutMs: config.ffmpegTimeoutMs,
    maximumIntermediateBytes: config.maxSourceBytes,
    maximumOutputBytes,
    maximumWorkspaceBytes,
    leaseHealthy,
    signal,
    verifySlice: async (path, expectedDurationSeconds) => {
      const probe = await runner.run(config.ffprobePath, [
        "-v", "error", "-show_streams", "-show_format", "-of", "json", path,
      ], config.ffmpegTimeoutMs, signal);
      verifyStagedSliceProbe(probe.stdout, expectedDurationSeconds);
    },
  });
  return { path: output, expectedDurationSeconds: manifest.timelineEndMs / 1_000 };
}

async function uploadArtifact(
  jobId: string,
  leaseToken: string,
  path: string,
  metadata: ArtifactMetadata,
  leaseHealthy: () => boolean,
  signal: AbortSignal,
): Promise<void> {
  let lastError: unknown;
  for (let attempt = 0; attempt < config.finalizeMaxAttempts; attempt += 1) {
    assertLease(leaseHealthy);
    const target = await apiRequest<{ uploadUrl: string; requiredHeaders?: Record<string, string> }>(
      `/internal/v1/composition-jobs/${jobId}/output-upload`,
      { body: metadata, signal },
      leaseToken,
    );
    const headers = validatedArtifactHeaders(target.requiredHeaders, metadata.bytes);
    try {
      const put = await fetch(target.uploadUrl, {
        method: "PUT",
        headers,
        body: createReadStream(path) as unknown as BodyInit,
        duplex: "half",
        signal: AbortSignal.any([signal, AbortSignal.timeout(config.artifactUploadTimeoutMs)]),
      } as RequestInit & { duplex: "half" });
      if (put.ok) return;
      if (put.status < 500 && ![408, 425, 429].includes(put.status)) {
        throw new TerminalApiError(put.status);
      }
      lastError = new Error(`artifact upload retryable status: ${put.status}`);
    } catch (error) {
      if (error instanceof TerminalApiError) throw error;
      lastError = error;
    }
    if (attempt + 1 < config.finalizeMaxAttempts) {
      await delay(config.finalizeRetryDelayMs * (attempt + 1));
    }
  }
  throw new RetryableCompositionError("artifact upload retries exhausted", { cause: lastError });
}

function validatedArtifactHeaders(required: Record<string, string> | undefined, bytes: number) {
  const headers = new Headers(required);
  if (headers.get("content-length") !== String(bytes) || headers.get("content-type") !== "video/mp4") {
    throw new Error("artifact upload headers do not match verified metadata");
  }
  return headers;
}

function validateV1Segment(segment: V1Segment, expectedStartMs: number): void {
  const valid = Number.isInteger(segment.generation) && segment.generation >= 0 &&
    Number.isInteger(segment.sequence) && segment.sequence >= 0 &&
    Number.isSafeInteger(segment.timelineStartMs) && segment.timelineStartMs === expectedStartMs &&
    Number.isSafeInteger(segment.timelineEndMs) && segment.timelineEndMs > segment.timelineStartMs &&
    Number.isSafeInteger(segment.bytes) && segment.bytes > 0 &&
    /^[0-9a-f]{64}$/.test(segment.sha256) && Boolean(segment.download?.url);
  if (!valid || segment.timelineEndMs > config.maxDurationMs) throw new Error("invalid v1 segment descriptor");
}

async function downloadExact(
  response: Response,
  path: string,
  expectedBytes: number,
  expectedSHA256: string,
): Promise<void> {
  if (!response.body) throw new RetryableCompositionError("segment download body is absent");
  const declared = response.headers.get("content-length");
  if (declared !== null && Number(declared) !== expectedBytes) throw new Error("segment content-length mismatch");
  const file = await open(path, "wx", 0o640);
  const digest = createHash("sha256");
  let written = 0;
  try {
    const reader = response.body.getReader();
    for (;;) {
      let chunk: ReadableStreamReadResult<Uint8Array>;
      try {
        chunk = await reader.read();
      } catch (error) {
        throw new RetryableCompositionError("segment response body interrupted", { cause: error });
      }
      if (chunk.done) break;
      if (written + chunk.value.byteLength > expectedBytes) {
        await reader.cancel();
        throw new Error("segment download exceeded byte bound");
      }
      written += chunk.value.byteLength;
      digest.update(chunk.value);
      await file.write(chunk.value);
    }
    if (written !== expectedBytes) {
      throw new RetryableCompositionError("segment response body ended early");
    }
    if (digest.digest("hex") !== expectedSHA256) throw new Error("segment checksum mismatch");
    await file.sync();
  } finally {
    await file.close();
  }
}

async function fetchSource(url: string, leaseSignal: AbortSignal): Promise<Response> {
  let response: Response;
  try {
    response = await fetch(url, {
      signal: AbortSignal.any([leaseSignal, AbortSignal.timeout(config.httpTimeoutMs)]),
    });
  } catch (error) {
    throw new RetryableCompositionError("segment download request failed", { cause: error });
  }
  if (!response.ok) {
    if (response.status >= 500 || [408, 425, 429].includes(response.status)) {
      throw new RetryableCompositionError(`segment download retryable status: ${response.status}`);
    }
    throw new Error(`segment download failed: ${response.status}`);
  }
  return response;
}

async function probeSource(path: string, segment: ManifestSegment, signal: AbortSignal): Promise<void> {
  const probe = await runner.run(config.ffprobePath, [
    "-v", "error", "-show_streams", "-show_format", "-of", "json", path,
  ], config.ffmpegTimeoutMs, signal);
  verifySourceProbe(probe.stdout, segment);
}

async function verifyOutput(
  path: string,
  expectedDurationSeconds: number,
  signal: AbortSignal,
): Promise<ArtifactMetadata> {
  const probe = await runner.run(config.ffprobePath, [
    "-v", "error", "-show_streams", "-show_format", "-of", "json", path,
  ], config.ffmpegTimeoutMs, signal);
  const metadata = verifyArtifactProbe(probe.stdout, expectedDurationSeconds);
  const info = await stat(path);
  if (!info.isFile() || info.size < 1 || info.size > maximumOutputBytes) {
    await rm(path, { force: true });
    throw new Error("final artifact byte bound exceeded");
  }
  try {
    const sha256 = await hashFileBounded(path, info.size);
    return { bytes: info.size, sha256, ...metadata };
  } catch (error) {
    await rm(path, { force: true });
    throw error;
  }
}

async function hashFileBounded(path: string, expectedBytes: number): Promise<string> {
  const hash = createHash("sha256");
  let bytes = 0;
  for await (const chunk of createReadStream(path)) {
    const value = chunk as Buffer;
    bytes += value.length;
    if (bytes > expectedBytes) throw new Error("final artifact changed during hashing");
    hash.update(value);
  }
  if (bytes !== expectedBytes) throw new Error("final artifact changed during hashing");
  return hash.digest("hex");
}

function heartbeatDelay(expiresAtMs: number): number {
  const remaining = expiresAtMs - Date.now();
  if (!Number.isFinite(remaining) || remaining <= 3_000) return 1_000;
  return Math.max(1_000, Math.min(30_000, Math.floor(remaining / 3)));
}

function assertLease(leaseHealthy: () => boolean): void {
  if (!leaseHealthy()) throw new RetryableCompositionError("composition lease heartbeat failed");
}

function escapeConcatPath(path: string): string {
  return path.replaceAll("'", "'\\''");
}

function sanitizeError(error: unknown): string {
  return String(error).replace(/[\r\n\t]+/g, " ").slice(0, 512);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function delayUntilAborted(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const finish = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, milliseconds);
    signal.addEventListener("abort", finish, { once: true });
    if (signal.aborted) finish();
  });
}

async function main(): Promise<void> {
  await rm(root, { recursive: true, force: true });
  await mkdir(root, { recursive: true, mode: 0o750 });
  startHealthServer();
  workerState = "idle";
  for (;;) {
    try {
      await once();
    } catch (error) {
      console.error("composition claim loop failed", error);
    }
    await delay(2_000);
  }
}

function startHealthServer(): void {
  createServer((request, response) => {
    if (request.url !== "/health/live" && request.url !== "/health/ready") {
      response.writeHead(404).end();
      return;
    }
    const ready = request.url === "/health/live" || workerState !== "starting";
    response.writeHead(ready ? 200 : 503, { "content-type": "application/json" });
    response.end(JSON.stringify({
      status: ready ? "ok" : "unavailable",
      service: "recording-composition",
      state: workerState,
    }));
  }).listen(8090, "0.0.0.0");
}

void main();
