import ffmpeg from "@ffmpeg-installer/ffmpeg";
import ffprobe from "@ffprobe-installer/ffprobe";
import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { verifyStagedSliceProbe, type CaptureV2Manifest, type ManifestSegment, type Source } from "./compositionV2.js";
import { executeV2Composition } from "./compositionV2Executor.js";
import { ChildProcessRunner, type ProcessRunner } from "./processRunner.js";

const hash = "a".repeat(64);
const directories: string[] = [];

afterEach(async () => {
  await Promise.all(directories.splice(0).map((directory) =>
    rm(directory, { recursive: true, force: true })));
});

const participant = (id: string) => ({ participantInstanceId: id, displayName: id });
const track = (id: string, owner: string, source: Source) => ({
  trackId: id,
  participantInstanceId: owner,
  source,
});
function segment(
  uploadId: string,
  trackId: string,
  owner: string,
  source: Source,
): ManifestSegment {
  const video = source !== "mic";
  return {
    uploadId,
    captureGeneration: 1,
    trackId,
    participantInstanceId: owner,
    source,
    sequence: 0,
    container: "webm",
    codec: video ? "vp8" : "opus",
    contentType: video ? "video/webm" : "audio/webm",
    expectedBytes: 1,
    sha256: hash,
    startMs: 0,
    endMs: 100,
  };
}
function manifest(overrides: Partial<CaptureV2Manifest>): CaptureV2Manifest {
  return {
    protocolVersion: "recording-capture.v2",
    manifestSha256: hash,
    timelineEndMs: 100,
    completeness: "complete",
    participantInstances: [],
    participantIntervals: [],
    tracks: [],
    trackIntervals: [],
    segments: [],
    gaps: [],
    ...overrides,
  };
}

class FileCreatingRunner implements ProcessRunner {
  readonly calls: string[][] = [];

  constructor(private readonly bytes = 1) {}

  async run(_command: string, args: string[]): Promise<{ stdout: string; stderr: string }> {
    this.calls.push(args);
    await writeFile(args.at(-1)!, Buffer.alloc(this.bytes, 1));
    return { stdout: "", stderr: "" };
  }
}

describe("capture-v2 staged executor", () => {
  it("keeps every FFmpeg command within 64 inputs and encodes AAC once", async () => {
    const directory = await temporaryDirectory();
    const participants = Array.from({ length: 130 }, (_, index) => participant(`pi-${index}`));
    const tracks = participants.flatMap((item, index) => [
      track(`camera-${index}`, item.participantInstanceId, "camera"),
      track(`mic-${index}`, item.participantInstanceId, "mic"),
    ]);
    const segments = tracks.map((item, index) =>
      segment(`upload-${index}`, item.trackId, item.participantInstanceId, item.source));
    const value = manifest({
      participantInstances: participants,
      participantIntervals: participants.map((item) => ({
        participantInstanceId: item.participantInstanceId,
        startMs: 0,
        endMs: 100,
      })),
      tracks,
      trackIntervals: tracks.map((item) => ({
        trackId: item.trackId,
        startMs: 0,
        endMs: 100,
        state: "active" as const,
      })),
      segments,
    });
    const paths = new Map(segments.map((item) => [item.uploadId, join(directory, item.uploadId)]));
    const runner = new FileCreatingRunner();
    await executeV2Composition({
      manifest: value,
      sourcePaths: paths,
      directory,
      ffmpegPath: "ffmpeg",
      runner,
      timeoutMs: 1_000,
      maximumIntermediateBytes: 10_000,
      maximumOutputBytes: 10_000,
      maximumWorkspaceBytes: 100_000,
      leaseHealthy: () => true,
      verifySlice: async () => undefined,
    });

    const inputCounts = runner.calls.map((args) => args.filter((item) => item === "-i").length);
    expect(Math.max(...inputCounts)).toBe(64);
    expect(runner.calls.flat().filter((item) => item === "aac")).toHaveLength(1);
    expect(runner.calls.every((args) => args.includes("-fs"))).toBe(true);
    expect(runner.calls.some((args) => args.includes("flac"))).toBe(true);
    expect(runner.calls.at(-1)).toEqual(expect.arrayContaining([
      "-f", "concat", "-vf", "fps=24,format=yuv420p", "-c:v", "libx264",
    ]));
    const remaining = await readdir(directory);
    expect(remaining).toEqual(["output.mp4"]);
  });

  it("fails closed when the lease is lost between stages", async () => {
    const directory = await temporaryDirectory();
    const runner = new FileCreatingRunner();
    let checks = 0;
    await expect(executeV2Composition({
      manifest: manifest({ participantInstances: [participant("pi-a")], participantIntervals: [{ participantInstanceId: "pi-a", startMs: 0, endMs: 100 }] }),
      sourcePaths: new Map(),
      directory,
      ffmpegPath: "ffmpeg",
      runner,
      timeoutMs: 1_000,
      maximumIntermediateBytes: 10_000,
      maximumOutputBytes: 10_000,
      maximumWorkspaceBytes: 100_000,
      leaseHealthy: () => ++checks < 4,
      verifySlice: async () => undefined,
    })).rejects.toThrow("lease heartbeat failed");
    expect(runner.calls.length).toBeLessThan(3);
  });

  it("fails when cumulative slices exceed the staging byte bound", async () => {
    const directory = await temporaryDirectory();
    const runner = new FileCreatingRunner(10);
    await expect(executeV2Composition({
      manifest: manifest({
        participantInstances: [participant("pi-a")],
        participantIntervals: [
          { participantInstanceId: "pi-a", startMs: 0, endMs: 50 },
          { participantInstanceId: "pi-a", startMs: 50, endMs: 100 },
        ],
      }),
      sourcePaths: new Map(),
      directory,
      ffmpegPath: "ffmpeg",
      runner,
      timeoutMs: 1_000,
      maximumIntermediateBytes: 10,
      maximumOutputBytes: 5,
      maximumWorkspaceBytes: 100,
      leaseHealthy: () => true,
      verifySlice: async () => undefined,
    })).rejects.toThrow("slice byte bound");
    expect(runner.calls.some((args) => args.includes("-fs") &&
      args[args.indexOf("-fs") + 1] === "5")).toBe(true);
  });

  it("enforces the hard intermediate max before accepting max plus one", async () => {
    const directory = await temporaryDirectory();
    const runner = new FileCreatingRunner(11);
    await expect(executeV2Composition({
      manifest: manifest({ participantInstances: [participant("pi-a")], participantIntervals: [{ participantInstanceId: "pi-a", startMs: 0, endMs: 100 }] }),
      sourcePaths: new Map(),
      directory,
      ffmpegPath: "ffmpeg",
      runner,
      timeoutMs: 1_000,
      maximumIntermediateBytes: 10,
      maximumOutputBytes: 10,
      maximumWorkspaceBytes: 100,
      leaseHealthy: () => true,
      verifySlice: async () => undefined,
    })).rejects.toThrow("intermediate byte bound");
    expect(runner.calls[0]).toEqual(expect.arrayContaining(["-fs", "10"]));
  });

  it("runs the tiny lossless staging and final concat on pinned FFmpeg", async () => {
    const directory = await temporaryDirectory();
    const runner = new ChildProcessRunner();
    const sliceDurations: number[] = [];
    const output = await executeV2Composition({
      manifest: manifest({
        participantInstances: [participant("pi-a")],
        participantIntervals: [
          { participantInstanceId: "pi-a", startMs: 0, endMs: 50 },
          { participantInstanceId: "pi-a", startMs: 50, endMs: 100 },
        ],
      }),
      sourcePaths: new Map(),
      directory,
      ffmpegPath: ffmpeg.path,
      runner,
      timeoutMs: 10_000,
      maximumIntermediateBytes: 10_000_000,
      maximumOutputBytes: 10_000_000,
      maximumWorkspaceBytes: 100_000_000,
      leaseHealthy: () => true,
      verifySlice: pinnedSliceVerifier(runner, sliceDurations),
    });
    const probe = await runner.run(ffprobe.path, [
      "-v", "error", "-show_streams", "-show_format", "-of", "json", output,
    ], 10_000);
    const parsed = JSON.parse(probe.stdout) as {
      streams: Array<{ codec_type: string; codec_name: string; width?: number; height?: number }>;
      format: { duration: string };
    };
    expect(parsed.streams).toEqual(expect.arrayContaining([
      expect.objectContaining({ codec_type: "video", codec_name: "h264", width: 1280, height: 720 }),
      expect.objectContaining({ codec_type: "audio", codec_name: "aac" }),
    ]));
    expect(Math.abs(Number(parsed.format.duration) - 0.1)).toBeLessThanOrEqual(0.25);
    expect(sliceDurations).toHaveLength(2);
  }, 20_000);

  it("runs pinned video overlay and 64-plus-one FLAC reduction", async () => {
    const directory = await temporaryDirectory();
    const runner = new ChildProcessRunner();
    const video = join(directory, "source-video.webm");
    const audio = join(directory, "source-audio.webm");
    await runner.run(ffmpeg.path, [
      "-hide_banner", "-loglevel", "error", "-y",
      "-f", "lavfi", "-i", "testsrc=size=160x90:rate=24:duration=0.1",
      "-an", "-c:v", "libvpx", "-pix_fmt", "yuv420p", video,
    ], 10_000);
    await runner.run(ffmpeg.path, [
      "-hide_banner", "-loglevel", "error", "-y",
      "-f", "lavfi", "-i", "sine=frequency=440:duration=0.1",
      "-vn", "-c:a", "libopus", audio,
    ], 10_000);

    const participants = Array.from({ length: 65 }, (_, index) => participant(`pi-${index}`));
    const camera = track("camera", "pi-0", "camera");
    const microphones = participants.map((item, index) =>
      track(`mic-${index}`, item.participantInstanceId, "mic"));
    const cameraSegment = segment("upload-camera", camera.trackId, camera.participantInstanceId, "camera");
    const microphoneSegments = microphones.map((item, index) =>
      segment(`upload-mic-${index}`, item.trackId, item.participantInstanceId, "mic"));
    const tracks = [camera, ...microphones];
    const segments = [cameraSegment, ...microphoneSegments];
    const paths = new Map<string, string>([[cameraSegment.uploadId, video]]);
    for (const item of microphoneSegments) paths.set(item.uploadId, audio);
    const output = await executeV2Composition({
      manifest: manifest({
        participantInstances: participants,
        participantIntervals: participants.map((item) => ({
          participantInstanceId: item.participantInstanceId,
          startMs: 0,
          endMs: 100,
        })),
        tracks,
        trackIntervals: tracks.map((item) => ({
          trackId: item.trackId,
          startMs: 0,
          endMs: 100,
          state: "active" as const,
        })),
        segments,
      }),
      sourcePaths: paths,
      directory,
      ffmpegPath: ffmpeg.path,
      runner,
      timeoutMs: 20_000,
      maximumIntermediateBytes: 50_000_000,
      maximumOutputBytes: 50_000_000,
      maximumWorkspaceBytes: 500_000_000,
      leaseHealthy: () => true,
      verifySlice: pinnedSliceVerifier(runner),
    });
    const probe = await runner.run(ffprobe.path, [
      "-v", "error", "-show_streams", "-show_format", "-of", "json", output,
    ], 10_000);
    const parsed = JSON.parse(probe.stdout) as {
      streams: Array<{ codec_type: string; codec_name: string }>;
      format: { duration: string };
    };
    expect(parsed.streams).toEqual(expect.arrayContaining([
      expect.objectContaining({ codec_type: "video", codec_name: "h264" }),
      expect.objectContaining({ codec_type: "audio", codec_name: "aac" }),
    ]));
    expect(Math.abs(Number(parsed.format.duration) - 0.1)).toBeLessThanOrEqual(0.25);
  }, 30_000);

  it("collapses ten one-millisecond scenes onto the authoritative global frame clock", async () => {
    const result = await runPinnedBoundaryFixture(Array.from({ length: 10 }, () => 1));
    expect(result.frames).toBe(1);
    expect(result.duration).toBeLessThanOrEqual(0.25);
  }, 30_000);

  it("does not accumulate timestamp drift across many scene boundaries", async () => {
    const result = await runPinnedBoundaryFixture(Array.from({ length: 20 }, () => 50));
    expect(result.frames).toBeGreaterThanOrEqual(23);
    expect(result.frames).toBeLessThanOrEqual(25);
    expect(Math.abs(result.duration - 1)).toBeLessThanOrEqual(0.25);
  }, 60_000);
});

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "conference-v2-staging-"));
  directories.push(directory);
  return directory;
}

function pinnedSliceVerifier(
  runner: ChildProcessRunner,
  durations: number[] = [],
): (path: string, expectedDurationSeconds: number) => Promise<void> {
  return async (path, expectedDurationSeconds) => {
    const probe = await runner.run(ffprobe.path, [
      "-v", "error", "-show_streams", "-show_format", "-of", "json", path,
    ], 10_000);
    verifyStagedSliceProbe(probe.stdout, expectedDurationSeconds);
    const parsed = JSON.parse(probe.stdout) as { format: { duration: string } };
    durations.push(Number(parsed.format.duration));
  };
}

async function runPinnedBoundaryFixture(
  durationsMs: number[],
): Promise<{ duration: number; frames: number }> {
  const directory = await temporaryDirectory();
  const runner = new ChildProcessRunner();
  let cursor = 0;
  const intervals = durationsMs.map((durationMs) => {
    const interval = {
      participantInstanceId: "pi-a",
      startMs: cursor,
      endMs: cursor + durationMs,
    };
    cursor += durationMs;
    return interval;
  });
  const output = await executeV2Composition({
    manifest: manifest({
      timelineEndMs: cursor,
      participantInstances: [participant("pi-a")],
      participantIntervals: intervals,
    }),
    sourcePaths: new Map(),
    directory,
    ffmpegPath: ffmpeg.path,
    runner,
    timeoutMs: 10_000,
    maximumIntermediateBytes: 50_000_000,
    maximumOutputBytes: 50_000_000,
    maximumWorkspaceBytes: 500_000_000,
    leaseHealthy: () => true,
    verifySlice: pinnedSliceVerifier(runner),
  });
  const probe = await runner.run(ffprobe.path, [
    "-v", "error", "-count_frames", "-select_streams", "v:0",
    "-show_entries", "stream=nb_read_frames", "-show_entries", "format=duration",
    "-of", "json", output,
  ], 10_000);
  const parsed = JSON.parse(probe.stdout) as {
    streams: Array<{ nb_read_frames: string }>;
    format: { duration: string };
  };
  return {
    duration: Number(parsed.format.duration),
    frames: Number(parsed.streams[0]!.nb_read_frames),
  };
}
