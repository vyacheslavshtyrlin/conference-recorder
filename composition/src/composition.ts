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
} from "./compositionV2.js";
import { executeV2Composition } from "./compositionV2Executor.js";
import {
  CompositionFailure,
  failureReport,
  retryableFailure,
  terminalFailure,
} from "./compositionFailure.js";
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

class TerminalApiError extends CompositionFailure {
  constructor(readonly status: number) {
    super(
      "CONTROL_PLANE_REJECTED",
      "control-plane",
      false,
      `Control plane rejected the request with status ${status}`,
    );
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
      this.abort(retryableFailure(
        "COMPOSITION_LEASE_LOST",
        "lease",
        "Composition lease expiry is invalid",
      ));
      return;
    }
    this.expiresAtMs = value;
    if (this.expiryTimer) clearTimeout(this.expiryTimer);
    this.expiryTimer = setTimeout(() => {
      this.abort(retryableFailure(
        "COMPOSITION_LEASE_LOST",
        "lease",
        "Composition lease expired locally",
      ));
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
  throw retryableFailure(
    "CONTROL_PLANE_UNAVAILABLE",
    "control-plane",
    "Control plane request retries were exhausted",
    lastError,
  );
}

async function once(shutdownSignal?: AbortSignal): Promise<void> {
  const claim = await apiRequest<Claim>(
    "/internal/v1/composition-jobs/claim",
    { body: {}, retry: false, ...(shutdownSignal ? { signal: shutdownSignal } : {}) },
  );
  if (!claim.job) return;
  workerState = "composing";
  const { jobId, leaseToken } = claim.job;
  const jobRoot = join(root, jobId);
  const directory = join(jobRoot, randomUUID());
  const lease = new LeaseGuard(claim.job.leaseExpiresAt);
  const abortForShutdown = () => lease.abort(retryableFailure(
    "COMPOSITION_SHUTDOWN",
    "lease",
    "Composition worker is draining for shutdown",
  ));
  shutdownSignal?.addEventListener("abort", abortForShutdown, { once: true });
  if (shutdownSignal?.aborted) abortForShutdown();
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
    let metadata: ArtifactMetadata;
    try {
      metadata = await verifyOutput(output.path, output.expectedDurationSeconds, lease.signal);
    } catch (error) {
      if (lease.signal.aborted) {
        throw retryableFailure(
          "COMPOSITION_LEASE_LOST",
          "lease",
          "Composition lease was lost while verifying the artifact",
          error,
        );
      }
      throw terminalFailure(
        "ARTIFACT_VERIFICATION_FAILED",
        "artifact-verify",
        "Final artifact verification failed",
        error,
      );
    }
    assertLease(leaseHealthy);
    await uploadArtifact(jobId, leaseToken, output.path, metadata, leaseHealthy, lease.signal);
    assertLease(leaseHealthy);
    await apiRequest(
      `/internal/v1/composition-jobs/${jobId}/complete`,
      { body: metadata, signal: lease.signal },
      leaseToken,
    );
  } catch (error) {
    const report = failureReport(error);
    const retryable = report.code === "COMPOSITION_SHUTDOWN" ||
      ((report.retryable || lease.signal.aborted) && claim.job.attempts < config.finalizeMaxAttempts);
    const endpoint = retryable ? "retry" : "fail";
    await apiRequest(
      `/internal/v1/composition-jobs/${jobId}/${endpoint}`,
      {
        body: {
          code: report.code,
          message: report.message,
        },
        retry: false,
      },
      leaseToken,
    ).catch(() => undefined);
    reportFailure("composition.job.failed", report);
  } finally {
    shutdownSignal?.removeEventListener("abort", abortForShutdown);
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
  if (!segments.length) {
    throw terminalFailure(
      "CAPTURE_EMPTY",
      "source",
      "V1 capture contains no committed segments",
    );
  }

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
  try {
    await runner.run(config.ffmpegPath, [
      "-hide_banner", "-loglevel", "error", "-y",
      "-f", "concat", "-safe", "0", "-i", concat,
      "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,fps=24",
      ...outputEncodingArguments(maximumOutputBytes), output,
    ], config.ffmpegTimeoutMs, signal);
  } catch (error) {
    if (signal.aborted || !leaseHealthy()) {
      throw retryableFailure(
        "COMPOSITION_LEASE_LOST",
        "lease",
        "Composition lease was lost while rendering",
        error,
      );
    }
    throw terminalFailure("RENDER_FAILED", "render", "Media rendering failed", error);
  }
  return { path: output, expectedDurationSeconds: expectedStartMs / 1_000 };
}

async function composeV2(
  jobId: string,
  leaseToken: string,
  directory: string,
  leaseHealthy: () => boolean,
  signal: AbortSignal,
): Promise<CompositionOutput> {
  let manifest;
  try {
    manifest = await loadCaptureV2Manifest(
      (cursor) => apiRequest<ManifestPage>(
        `/internal/v1/composition-jobs/${jobId}/capture-v2/manifest${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""}`,
        { method: "GET", signal },
        leaseToken,
      ),
      { maxSourceBytes: config.maxSourceBytes, maxDurationMs: config.maxDurationMs },
    );
  } catch (error) {
    if (error instanceof CompositionFailure) {
      throw error;
    }
    throw terminalFailure(
      "CAPTURE_MANIFEST_INVALID",
      "source",
      "Capture manifest validation failed",
      error,
    );
  }
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
      try {
        await downloadBoundedSource(response, segment, path);
      } catch (error) {
        if (error instanceof CompositionFailure) {
          throw error;
        }
        throw terminalFailure(
          "SOURCE_VALIDATION_FAILED",
          "source",
          "Downloaded source did not match its descriptor",
          error,
        );
      }
      assertLease(leaseHealthy);
      try {
        await probeSource(path, segment, signal);
      } catch (error) {
        if (signal.aborted) {
          throw retryableFailure(
            "COMPOSITION_LEASE_LOST",
            "lease",
            "Composition lease was lost while probing a source",
            error,
          );
        }
        throw terminalFailure(
          "SOURCE_PROBE_FAILED",
          "source",
          "Downloaded source media probe failed",
          error,
        );
      }
      paths.set(segment.uploadId, path);
    }
  }
  assertLease(leaseHealthy);
  let output: string;
  try {
    output = await executeV2Composition({
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
  } catch (error) {
    if (signal.aborted || !leaseHealthy()) {
      throw retryableFailure(
        "COMPOSITION_LEASE_LOST",
        "lease",
        "Composition lease was lost while rendering",
        error,
      );
    }
    throw terminalFailure(
      "RENDER_FAILED",
      "render",
      "Media rendering failed",
      error,
    );
  }
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
  assertLease(leaseHealthy);
  const target = await apiRequest<{ uploadUrl: string; requiredHeaders?: Record<string, string> }>(
    `/internal/v1/composition-jobs/${jobId}/output-upload`,
    { body: metadata, retry: false, signal },
    leaseToken,
  );
  const headers = validatedArtifactHeaders(target.requiredHeaders, metadata.bytes);
  let put: Response;
  try {
    put = await fetch(target.uploadUrl, {
      method: "PUT",
      headers,
      body: createReadStream(path) as unknown as BodyInit,
      duplex: "half",
      signal: AbortSignal.any([signal, AbortSignal.timeout(config.artifactUploadTimeoutMs)]),
    } as RequestInit & { duplex: "half" });
  } catch (error) {
    throw retryableFailure(
      "ARTIFACT_UPLOAD_INTERRUPTED",
      "artifact-upload",
      "Final artifact upload was interrupted",
      error,
    );
  }
  if (put.ok) {
    return;
  }
  if (put.status >= 500 || [408, 425, 429].includes(put.status)) {
    throw retryableFailure(
      "ARTIFACT_STORAGE_UNAVAILABLE",
      "artifact-upload",
      `Artifact storage returned retryable status ${put.status}`,
    );
  }
  throw terminalFailure(
    "ARTIFACT_UPLOAD_REJECTED",
    "artifact-upload",
    `Artifact storage rejected the upload with status ${put.status}`,
  );
}

function validatedArtifactHeaders(required: Record<string, string> | undefined, bytes: number) {
  const headers = new Headers(required);
  if (headers.get("content-length") !== String(bytes) || headers.get("content-type") !== "video/mp4") {
    throw terminalFailure(
      "ARTIFACT_UPLOAD_CONTRACT_INVALID",
      "artifact-upload",
      "Artifact upload headers do not match verified metadata",
    );
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
  if (!valid || segment.timelineEndMs > config.maxDurationMs) {
    throw terminalFailure(
      "SOURCE_VALIDATION_FAILED",
      "source",
      "V1 source segment descriptor is invalid",
    );
  }
}

async function downloadExact(
  response: Response,
  path: string,
  expectedBytes: number,
  expectedSHA256: string,
): Promise<void> {
  if (!response.body) {
    throw retryableFailure(
      "SOURCE_DOWNLOAD_INTERRUPTED",
      "source",
      "Source download body is absent",
    );
  }
  const declared = response.headers.get("content-length");
  if (declared !== null && Number(declared) !== expectedBytes) {
    throw terminalFailure(
      "SOURCE_VALIDATION_FAILED",
      "source",
      "Source content length does not match its descriptor",
    );
  }
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
        throw retryableFailure(
          "SOURCE_DOWNLOAD_INTERRUPTED",
          "source",
          "Source response body was interrupted",
          error,
        );
      }
      if (chunk.done) break;
      if (written + chunk.value.byteLength > expectedBytes) {
        await reader.cancel();
        throw terminalFailure(
          "SOURCE_VALIDATION_FAILED",
          "source",
          "Source download exceeded its byte bound",
        );
      }
      written += chunk.value.byteLength;
      digest.update(chunk.value);
      await file.write(chunk.value);
    }
    if (written !== expectedBytes) {
      throw retryableFailure(
        "SOURCE_DOWNLOAD_INTERRUPTED",
        "source",
        "Source response body ended early",
      );
    }
    if (digest.digest("hex") !== expectedSHA256) {
      throw terminalFailure(
        "SOURCE_VALIDATION_FAILED",
        "source",
        "Source checksum does not match its descriptor",
      );
    }
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
    throw retryableFailure(
      "SOURCE_DOWNLOAD_INTERRUPTED",
      "source",
      "Source download request failed",
      error,
    );
  }
  if (!response.ok) {
    if (response.status >= 500 || [408, 425, 429].includes(response.status)) {
      throw retryableFailure(
        "SOURCE_STORAGE_UNAVAILABLE",
        "source",
        `Source storage returned retryable status ${response.status}`,
      );
    }
    throw terminalFailure(
      "SOURCE_DOWNLOAD_REJECTED",
      "source",
      `Source storage rejected the download with status ${response.status}`,
    );
  }
  return response;
}

async function probeSource(path: string, segment: ManifestSegment, signal: AbortSignal): Promise<void> {
  const probe = await runner.run(config.ffprobePath, [
    "-v", "error",
    "-show_entries", "stream=codec_type,codec_name:format=duration:packet=pts_time,duration_time",
    "-show_packets", "-of", "json", path,
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
  if (!leaseHealthy()) {
    throw retryableFailure(
      "COMPOSITION_LEASE_LOST",
      "lease",
      "Composition lease is no longer healthy",
    );
  }
}

function escapeConcatPath(path: string): string {
  return path.replaceAll("'", "'\\''");
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
  const healthServer = startHealthServer();
  const shutdown = new AbortController();
  let shutdownRequested = false;
  const requestShutdown = () => {
    shutdownRequested = true;
    workerState = "starting";
    shutdown.abort();
  };
  process.once("SIGINT", requestShutdown);
  process.once("SIGTERM", requestShutdown);
  workerState = "idle";
  while (!shutdownRequested) {
    try {
      await once(shutdown.signal);
    } catch (error) {
      reportFailure("composition.claim.failed", failureReport(error));
    }
    if (!shutdownRequested) await delay(2_000);
  }
  await new Promise<void>((resolve, reject) => {
    healthServer.close((error) => error ? reject(error) : resolve());
  });
}

function reportFailure(
  event: "composition.job.failed" | "composition.claim.failed",
  report: { code: string; message: string; retryable: boolean; diagnostic?: string },
): void {
  console.error(JSON.stringify({
    event,
    code: report.code.slice(0, 64),
    message: report.message.replace(/[\r\n\t]+/g, " ").slice(0, 512),
    ...(report.diagnostic ? { diagnostic: report.diagnostic } : {}),
    retryable: report.retryable,
    outcome: "failure",
  }));
}

function startHealthServer(): ReturnType<typeof createServer> {
  return createServer((request, response) => {
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

void main().catch((error) => {
  reportFailure("composition.claim.failed", failureReport(error));
  process.exitCode = 1;
});
