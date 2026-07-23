import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { link, mkdir, open, readFile, readdir, rename, rm, stat, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import type { AttemptRequest } from "./contracts.js";

type ManifestStatus = "capturing" | "finalizing" | "ready" | "failed";
export type RecordingSegment = {
  generation: number;
  sequence: number;
  timelineStartMs: number;
  timelineEndMs: number;
  bytes: number;
  sha256: string;
  path: string;
};
type Manifest = {
  version: 2;
  recordingId: string;
  roomId: string;
  attemptId: string;
  status: ManifestStatus;
  startedAt: string;
  updatedAt: string;
  sourceBytes: number;
  generations: Array<{
    generation: number;
    attemptId: string;
    startedAt: string;
    recovered: boolean;
  }>;
  segments: RecordingSegment[];
  discontinuities: Array<{
    afterGeneration: number;
    beforeGeneration: number;
    detectedAt: string;
  }>;
  artifact?: {
    path: string;
    bytes: number;
    sha256: string;
    durationSeconds: number;
    width: 1280;
    height: 720;
    frameRate: 24;
  };
  failure?: { code: string; message: string };
};

export type SegmentInput = {
  generation: number;
  sequence: number;
  timelineStartMs: number;
  timelineEndMs: number;
  bytes: Buffer;
};

export interface RecordingSink {
  begin(request: AttemptRequest): Promise<{ generation: number }>;
  appendSegment(recordingId: string, segment: SegmentInput): Promise<void>;
  finalize(recordingId: string): Promise<{ path: string }>;
  fail(recordingId: string, code: string, message: string): Promise<void>;
  recoverFinalization?(recordingId: string): Promise<{ path: string }>;
  listPendingFinalizations?(): Promise<string[]>;
}

export interface ProcessRunner {
  run(
    command: string,
    args: string[],
    timeoutMs: number,
  ): Promise<{ stdout: string; stderr?: string }>;
}

type Session = {
  manifest: Manifest;
  generation: number;
  tail: Promise<void>;
  directory: string;
  manifestPath: string;
  fencePath: string;
};

export class LocalRecordingSink implements RecordingSink {
  private readonly sessions = new Map<string, Session>();

  constructor(
    private readonly root: string,
    private readonly ffmpegPath: string,
    private readonly ffprobePath: string,
    private readonly timeoutMs: number,
    private readonly runner: ProcessRunner = new ChildProcessRunner(),
    private readonly limits: {
      maxSourceBytes: number;
      maxDurationMs: number;
    } = { maxSourceBytes: 4_000_000_000, maxDurationMs: 14_400_000 },
    private readonly owner: {
      workerId: string;
      generationId: string;
    } = { workerId: "standalone", generationId: "standalone" },
  ) {}

  async begin(request: AttemptRequest): Promise<{ generation: number }> {
    if (
      request.workerId !== this.owner.workerId ||
      request.workerGenerationId !== this.owner.generationId
    )
      throw new Error("recording worker fence mismatch");
    const active = this.sessions.get(request.recordingId);
    if (active) return { generation: active.generation };
    const directory = join(this.root, "recordings", request.recordingId);
    const manifestPath = join(directory, "manifest.json");
    const fencePath = join(directory, "owner.json");
    await mkdir(join(directory, "segments"), { recursive: true, mode: 0o750 });
    await this.claimFence(fencePath);
    let manifest: Manifest;
    try {
      manifest = JSON.parse(await readFile(manifestPath, "utf8")) as Manifest;
      await this.assertFence(fencePath);
      validateManifestIdentity(manifest, request);
      if (manifest.version !== 2) throw new Error("unsupported recording manifest");
      if (manifest.status === "ready") throw new Error("recording is already finalized");
      await validateSegments(directory, manifest.segments);
      await removeUntrackedSegments(
        directory,
        manifest.segments,
        () => this.assertFence(fencePath),
      );
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      const now = new Date().toISOString();
      manifest = {
        version: 2,
        recordingId: request.recordingId,
        roomId: request.roomId,
        attemptId: request.attemptId,
        status: "capturing",
        startedAt: now,
        updatedAt: now,
        sourceBytes: 0,
        generations: [],
        segments: [],
        discontinuities: [],
      };
    }
    await this.assertFence(fencePath);
    const generation =
      Math.max(0, ...manifest.generations.map((item) => item.generation)) + 1;
    const now = new Date().toISOString();
    const previous = manifest.generations.at(-1);
    if (previous && manifest.segments.some((item) => item.generation === previous.generation)) {
      manifest.discontinuities.push({
        afterGeneration: previous.generation,
        beforeGeneration: generation,
        detectedAt: now,
      });
    }
    manifest.attemptId = request.attemptId;
    manifest.status = "capturing";
    manifest.updatedAt = now;
    delete manifest.failure;
    manifest.generations.push({
      generation,
      attemptId: request.attemptId,
      startedAt: now,
      recovered: generation > 1,
    });
    await this.assertFence(fencePath);
    await writeJsonAtomic(manifestPath, manifest);
    this.sessions.set(request.recordingId, {
      manifest,
      generation,
      tail: Promise.resolve(),
      directory,
      manifestPath,
      fencePath,
    });
    return { generation };
  }

  async appendSegment(recordingId: string, input: SegmentInput): Promise<void> {
    if (input.bytes.length === 0) return;
    const session = this.require(recordingId);
    session.tail = session.tail.then(async () => {
      await this.assertFence(session.fencePath);
      if (input.generation !== session.generation)
        throw new Error("segment generation is not active");
      if (input.timelineEndMs < input.timelineStartMs)
        throw new Error("invalid segment timeline");
      if (session.manifest.sourceBytes + input.bytes.length > this.limits.maxSourceBytes)
        throw new Error("recording source size limit exceeded");
      if (Date.now() - Date.parse(session.manifest.startedAt) > this.limits.maxDurationMs)
        throw new Error("recording duration limit exceeded");
      const digest = sha256(input.bytes);
      const existing = session.manifest.segments.find(
        (item) =>
          item.generation === input.generation &&
          item.sequence === input.sequence,
      );
      if (existing) {
        if (existing.sha256 === digest && existing.bytes === input.bytes.length)
          return;
        throw new Error("segment sequence conflict");
      }
      const last = session.manifest.segments
        .filter((item) => item.generation === input.generation)
        .at(-1);
      if (input.sequence !== (last?.sequence ?? -1) + 1)
        throw new Error("segment sequence gap");
      const relativePath = `segments/g${String(input.generation).padStart(4, "0")}-s${String(input.sequence).padStart(8, "0")}.webm`;
      const path = join(session.directory, relativePath);
      try {
        await writeImmutable(path, input.bytes);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        const orphan = await readFile(path);
        if (orphan.length !== input.bytes.length || sha256(orphan) !== digest)
          throw new Error("orphan segment content conflict");
      }
      const segment: RecordingSegment = {
        generation: input.generation,
        sequence: input.sequence,
        timelineStartMs: input.timelineStartMs,
        timelineEndMs: input.timelineEndMs,
        bytes: input.bytes.length,
        sha256: digest,
        path: relativePath,
      };
      session.manifest.segments.push(segment);
      session.manifest.sourceBytes += input.bytes.length;
      session.manifest.updatedAt = new Date().toISOString();
      await this.assertFence(session.fencePath);
      await writeJsonAtomic(session.manifestPath, session.manifest);
    });
    await session.tail;
  }

  async finalize(recordingId: string): Promise<{ path: string }> {
    const session = this.require(recordingId);
    await session.tail;
    await this.assertFence(session.fencePath);
    if (session.manifest.segments.length === 0)
      throw new Error("recording source is empty");
    await validateSegments(session.directory, session.manifest.segments);
    session.manifest.status = "finalizing";
    session.manifest.updatedAt = new Date().toISOString();
    await this.assertFence(session.fencePath);
    await writeJsonAtomic(session.manifestPath, session.manifest);
    const operationId = `${createHash("sha256")
      .update(this.owner.generationId)
      .digest("hex")
      .slice(0, 12)}-${randomUUID()}`;
    const concatPath = join(
      session.directory,
      `segments.${operationId}.concat.txt`,
    );
    const stagingPath = join(
      session.directory,
      `final.${operationId}.mp4.part`,
    );
    const ordered = [...session.manifest.segments].sort(
      (left, right) =>
        left.generation - right.generation || left.sequence - right.sequence,
    );
    await writeFile(
      concatPath,
      ordered
        .map((segment) => `file '${join(session.directory, segment.path).replaceAll("'", "'\\''")}'`)
        .join("\n"),
      { mode: 0o640 },
    );
    try {
      await this.runner.run(
        this.ffmpegPath,
        [
          "-hide_banner", "-loglevel", "error", "-y",
          "-f", "concat", "-safe", "0", "-i", concatPath,
          "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,fps=24",
          "-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p",
          "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart",
          "-f", "mp4", stagingPath,
        ],
        this.timeoutMs,
      );
      const probe = await this.runner.run(
        this.ffprobePath,
        ["-v", "error", "-show_streams", "-show_format", "-of", "json", stagingPath],
        this.timeoutMs,
      );
      const expectedDurationSeconds =
        ordered.reduce(
          (sum, segment) =>
            sum + (segment.timelineEndMs - segment.timelineStartMs),
          0,
        ) / 1000;
      const verified = verifyProbe(probe.stdout, expectedDurationSeconds);
      const artifactPath = join(this.root, "final", `${recordingId}.mp4`);
      await mkdir(dirname(artifactPath), { recursive: true, mode: 0o750 });
      await this.assertFence(session.fencePath);
      try {
        await link(stagingPath, artifactPath);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        const [existing, staged] = await Promise.all([
          readFile(artifactPath),
          readFile(stagingPath),
        ]);
        if (sha256(existing) !== sha256(staged))
          throw new Error("published recording artifact conflict");
      }
      const artifactStat = await stat(artifactPath);
      session.manifest.status = "ready";
      session.manifest.updatedAt = new Date().toISOString();
      session.manifest.artifact = {
        path: `final/${recordingId}.mp4`,
        bytes: artifactStat.size,
        sha256: sha256(await readFile(artifactPath)),
        durationSeconds: verified.durationSeconds,
        width: 1280,
        height: 720,
        frameRate: 24,
      };
      delete session.manifest.failure;
      await this.assertFence(session.fencePath);
      await writeJsonAtomic(session.manifestPath, session.manifest);
      this.sessions.delete(recordingId);
      return { path: artifactPath };
    } catch (error) {
      session.manifest.status = "failed";
      session.manifest.updatedAt = new Date().toISOString();
      session.manifest.failure = {
        code: "BOT_FINALIZATION_FAILED",
        message: String(error).slice(0, 1024),
      };
      await this.assertFence(session.fencePath);
      await writeJsonAtomic(session.manifestPath, session.manifest);
      throw error;
    } finally {
      await Promise.all([
        rm(concatPath, { force: true }),
        rm(stagingPath, { force: true }),
      ]);
    }
  }

  async fail(recordingId: string, code: string, message: string): Promise<void> {
    const session = this.sessions.get(recordingId);
    if (!session) return;
    await session.tail.catch(() => undefined);
    await this.assertFence(session.fencePath);
    session.manifest.status = "failed";
    session.manifest.updatedAt = new Date().toISOString();
    session.manifest.failure = { code, message: message.slice(0, 1024) };
    await this.assertFence(session.fencePath);
    await writeJsonAtomic(session.manifestPath, session.manifest);
    this.sessions.delete(recordingId);
  }

  async recoverFinalization(recordingId: string): Promise<{ path: string }> {
    if (!this.sessions.has(recordingId)) {
      const directory = join(this.root, "recordings", recordingId);
      const manifestPath = join(directory, "manifest.json");
      const fencePath = join(directory, "owner.json");
      await this.claimFence(fencePath);
      const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as Manifest;
      await this.assertFence(fencePath);
      if (manifest.version === 2 && manifest.status === "ready" && manifest.artifact) {
        const artifactPath = join(this.root, manifest.artifact.path);
        const artifact = await readFile(artifactPath);
        if (
          artifact.length !== manifest.artifact.bytes ||
          sha256(artifact) !== manifest.artifact.sha256
        )
          throw new Error("ready recording artifact verification failed");
        return { path: artifactPath };
      }
      if (
        manifest.version !== 2 ||
        (manifest.status !== "finalizing" && manifest.status !== "failed") ||
        manifest.segments.length === 0
      )
        throw new Error("recording has no pending durable finalization");
      await validateSegments(directory, manifest.segments);
      await removeUntrackedSegments(
        directory,
        manifest.segments,
        () => this.assertFence(fencePath),
      );
      await this.assertFence(fencePath);
      this.sessions.set(recordingId, {
        manifest,
        generation: manifest.generations.at(-1)?.generation ?? 1,
        tail: Promise.resolve(),
        directory,
        manifestPath,
        fencePath,
      });
    }
    return this.finalize(recordingId);
  }

  async listPendingFinalizations(): Promise<string[]> {
    const recordingsRoot = join(this.root, "recordings");
    let entries;
    try {
      entries = await readdir(recordingsRoot, { withFileTypes: true });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
      throw error;
    }
    const pending: string[] = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      try {
        const manifest = JSON.parse(
          await readFile(join(recordingsRoot, entry.name, "manifest.json"), "utf8"),
        ) as Manifest;
        if (
          manifest.version === 2 &&
          manifest.segments.length > 0 &&
          (manifest.status === "finalizing" || manifest.status === "failed")
        )
          pending.push(manifest.recordingId);
      } catch {
        // A malformed/private manifest is never advertised as recoverable.
      }
    }
    return pending.sort();
  }

  private async claimFence(path: string): Promise<void> {
    await writeJsonAtomic(path, this.owner);
    await this.assertFence(path);
  }

  private async assertFence(path: string): Promise<void> {
    const current = JSON.parse(await readFile(path, "utf8")) as {
      workerId?: string;
      generationId?: string;
    };
    if (
      current.workerId !== this.owner.workerId ||
      current.generationId !== this.owner.generationId
    )
      throw new Error("recording ownership fence lost");
  }

  private require(recordingId: string): Session {
    const session = this.sessions.get(recordingId);
    if (!session) throw new Error("recording sink session not found");
    return session;
  }
}

export class ChildProcessRunner implements ProcessRunner {
  run(
    command: string,
    args: string[],
    timeoutMs: number,
  ): Promise<{ stdout: string; stderr: string }> {
    return new Promise((resolve, reject) => {
      const child = spawn(command, args, {
        stdio: ["ignore", "pipe", "pipe"],
        windowsHide: true,
      });
      const stdout: Buffer[] = [];
      const stderr: Buffer[] = [];
      child.stdout.on("data", (chunk) => stdout.push(Buffer.from(chunk)));
      child.stderr.on("data", (chunk) => stderr.push(Buffer.from(chunk)));
      let timedOut = false;
      const timer = setTimeout(() => {
        timedOut = true;
        child.kill("SIGKILL");
      }, timeoutMs);
      child.once("error", (error) => {
        clearTimeout(timer);
        reject(error);
      });
      child.once("exit", (code, signal) => {
        clearTimeout(timer);
        if (timedOut) return reject(new Error(`${command} timed out`));
        if (code !== 0)
          return reject(
            new Error(
              `${command} exited with ${String(code ?? signal)}: ${Buffer.concat(stderr).toString("utf8").slice(0, 2048)}`,
            ),
          );
        resolve({
          stdout: Buffer.concat(stdout).toString("utf8"),
          stderr: Buffer.concat(stderr).toString("utf8"),
        });
      });
    });
  }
}

function validateManifestIdentity(manifest: Manifest, request: AttemptRequest): void {
  if (manifest.recordingId !== request.recordingId || manifest.roomId !== request.roomId)
    throw new Error("recording manifest identity conflict");
}

async function validateSegments(directory: string, segments: RecordingSegment[]): Promise<void> {
  const keys = new Set<string>();
  const ordered = [...segments].sort(
    (left, right) =>
      left.generation - right.generation || left.sequence - right.sequence,
  );
  const lastByGeneration = new Map<
    number,
    { sequence: number; timelineEndMs: number }
  >();
  for (const segment of ordered) {
    const key = `${segment.generation}:${segment.sequence}`;
    if (keys.has(key)) throw new Error("duplicate segment sequence");
    keys.add(key);
    const previous = lastByGeneration.get(segment.generation);
    if (segment.sequence !== (previous?.sequence ?? -1) + 1)
      throw new Error(`recording segment sequence gap: ${key}`);
    if (
      segment.timelineStartMs < 0 ||
      segment.timelineEndMs < segment.timelineStartMs ||
      (previous && segment.timelineStartMs < previous.timelineEndMs)
    )
      throw new Error(`invalid recording segment timeline: ${key}`);
    const bytes = await readFile(join(directory, segment.path));
    if (bytes.length !== segment.bytes || sha256(bytes) !== segment.sha256)
      throw new Error(`recording segment checksum mismatch: ${key}`);
    lastByGeneration.set(segment.generation, {
      sequence: segment.sequence,
      timelineEndMs: segment.timelineEndMs,
    });
  }
}

async function writeImmutable(path: string, bytes: Buffer): Promise<void> {
  const handle = await open(path, "wx", 0o640);
  try {
    await handle.writeFile(bytes);
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function removeUntrackedSegments(
  directory: string,
  segments: RecordingSegment[],
  assertOwner: () => Promise<void>,
): Promise<void> {
  const tracked = new Set(segments.map((segment) => segment.path.replaceAll("\\", "/")));
  const segmentDirectory = join(directory, "segments");
  const entries = await readdir(segmentDirectory, { withFileTypes: true });
  for (const entry of entries) {
    if (!entry.isFile()) continue;
    const relative = `segments/${entry.name}`;
    if (!tracked.has(relative)) {
      await assertOwner();
      await rm(join(segmentDirectory, entry.name), { force: true });
    }
  }
}

function sha256(bytes: Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function verifyProbe(
  raw: string,
  expectedDurationSeconds: number,
): { durationSeconds: number } {
  const value = JSON.parse(raw) as {
    streams?: Array<{
      codec_type?: string;
      width?: number;
      height?: number;
      avg_frame_rate?: string;
    }>;
    format?: { duration?: string };
  };
  const video = value.streams?.find((stream) => stream.codec_type === "video");
  const audio = value.streams?.find((stream) => stream.codec_type === "audio");
  const durationSeconds = Number(value.format?.duration);
  if (
    !video ||
    !audio ||
    video.width !== 1280 ||
    video.height !== 720 ||
    !sameFrameRate(video.avg_frame_rate, 24) ||
    !Number.isFinite(durationSeconds) ||
    durationSeconds <= 0 ||
    Math.abs(durationSeconds - expectedDurationSeconds) >
      Math.max(1.5, expectedDurationSeconds * 0.15)
  )
    throw new Error("final artifact verification failed");
  return { durationSeconds };
}

function sameFrameRate(value: string | undefined, expected: number): boolean {
  const [numerator, denominator] = (value ?? "0/1").split("/").map(Number);
  return denominator !== 0 && Math.abs(numerator! / denominator! - expected) < 0.01;
}

async function writeJsonAtomic(path: string, value: unknown): Promise<void> {
  const temporary = `${path}.${randomUUID()}.tmp`;
  const handle = await open(temporary, "w", 0o640);
  try {
    await handle.writeFile(JSON.stringify(value, null, 2));
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temporary, path);
}
