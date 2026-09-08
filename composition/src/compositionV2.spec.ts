import ffmpeg from "@ffmpeg-installer/ffmpeg";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  compileV2StagingPlan,
  downloadBoundedSource,
  loadCaptureV2Manifest,
  outputEncodingArguments,
  stagedOutputEncodingArguments,
  verifyArtifactProbe,
  verifySourceProbe,
  type CaptureV2Manifest,
  type ManifestPage,
  type ManifestSegment,
  type Source,
} from "./compositionV2.js";

const hash = "a".repeat(64);
const participant = (id: string, displayName = id) => ({
  participantInstanceId: id,
  displayName,
});
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
  startMs: number,
  endMs: number,
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
    expectedBytes: 10,
    sha256: hash,
    startMs,
    endMs,
  };
}

function manifest(overrides: Partial<CaptureV2Manifest>): CaptureV2Manifest {
  return {
    protocolVersion: "recording-capture.v2",
    manifestSha256: hash,
    timelineEndMs: 10_000,
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
function stagedFiltergraph(value: CaptureV2Manifest): string {
  return compileV2StagingPlan(value).scenes
    .flatMap((scene) => [
      scene.baseFiltergraph,
      ...scene.videoPasses.map((pass) => pass.filtergraph),
    ])
    .join(";");
}

describe("capture-v2 production staging goldens", () => {
  it("renders camera-only grid with a name overlay", () => {
    const value = manifest({
      participantInstances: [participant("pi-a", "Alex")],
      participantIntervals: [
        { participantInstanceId: "pi-a", startMs: 0, endMs: 10_000 },
      ],
      tracks: [track("trk-camera", "pi-a", "camera")],
      trackIntervals: [
        { trackId: "trk-camera", startMs: 0, endMs: 10_000, state: "active" },
      ],
      segments: [
        segment("upload-camera", "trk-camera", "pi-a", "camera", 0, 10_000),
      ],
    });

    const result = compileV2StagingPlan(value);
    const scene = result.scenes[0]!;

    expect(result.scenes).toHaveLength(1);
    expect(scene.baseFiltergraph).toContain("c=0x090f1c");
    expect(scene.baseFiltergraph).toContain("c=0x17223a80");
    expect(scene.baseFiltergraph).toContain("drawgrid=w=64:h=64");
    expect(scene.videoPasses[0]!.filtergraph).toContain("scale=603:664");
    expect(scene.videoPasses[0]!.filtergraph).toContain("overlay=x=28:y=28");
    expect(result).toMatchSnapshot();
  });

  it("renders muted camera as initials/name avatar", () => {
    const value = manifest({
      participantInstances: [participant("pi-a", "Alex")],
      participantIntervals: [
        { participantInstanceId: "pi-a", startMs: 0, endMs: 10_000 },
      ],
      tracks: [
        track("trk-camera", "pi-a", "camera"),
        track("trk-mic", "pi-a", "mic"),
      ],
      trackIntervals: [
        { trackId: "trk-camera", startMs: 0, endMs: 10_000, state: "paused" },
        { trackId: "trk-mic", startMs: 0, endMs: 10_000, state: "active" },
      ],
      segments: [segment("upload-mic", "trk-mic", "pi-a", "mic", 0, 10_000)],
    });

    const result = compileV2StagingPlan(value);
    const scene = result.scenes[0]!;
    const graph = scene.videoPasses[0]!.filtergraph;

    expect(graph).toContain("c=0x243b66");
    expect(graph).toContain("fontcolor=0x8fb4f8");
    expect(graph).toContain("text='A'");
    expect(graph).toContain("text='Alex'");
    expect(graph).toContain("boxcolor=0x030812d1");
    expect(scene.audioGroups).toHaveLength(1);
    expect(scene.audioGroups[0]![0]!.uploadId).toBe("upload-mic");
    expect(result).toMatchSnapshot();
  });
  it("splits a long camera source across another participant join and leave", () => {
    const value = manifest({
      participantInstances: [
        participant("pi-z", "Alex"),
        participant("pi-a", "Blair"),
      ],
      participantIntervals: [
        { participantInstanceId: "pi-z", startMs: 0, endMs: 10_000 },
        { participantInstanceId: "pi-a", startMs: 3_000, endMs: 7_000 },
      ],
      tracks: [
        track("trk-z", "pi-z", "camera"),
        track("trk-a", "pi-a", "camera"),
      ],
      trackIntervals: [
        { trackId: "trk-z", startMs: 0, endMs: 10_000, state: "active" },
        { trackId: "trk-a", startMs: 3_000, endMs: 7_000, state: "active" },
      ],
      segments: [
        segment("upload-z", "trk-z", "pi-z", "camera", 0, 10_000),
        segment("upload-a", "trk-a", "pi-a", "camera", 3_000, 7_000),
      ],
    });

    const result = compileV2StagingPlan(value);

    expect(result.scenes).toHaveLength(3);
    expect(result.scenes.map((scene) => [scene.startMs, scene.endMs])).toEqual([
      [0, 3_000],
      [3_000, 7_000],
      [7_000, 10_000],
    ]);
    expect(
      result.scenes[1]!.videoPasses[0]!.sourceSegments.map((item) => item.uploadId),
    ).toEqual(["upload-z", "upload-a"]);
    expect(result.scenes[1]!.videoPasses[0]!.filtergraph)
      .toMatch(/text='A'.*text='B'/);
    expect(result).toMatchSnapshot();
  });

  it("renders screen start stop and replacement with CanvasComposer stage layouts", () => {
    const value = manifest({
      participantInstances: [
        participant("pi-a", "Alex"),
        participant("pi-b", "Blair"),
      ],
      participantIntervals: [
        { participantInstanceId: "pi-a", startMs: 0, endMs: 10_000 },
        { participantInstanceId: "pi-b", startMs: 0, endMs: 10_000 },
      ],
      tracks: [
        track("trk-a", "pi-a", "camera"),
        track("trk-b", "pi-b", "camera"),
        track("trk-screen-a", "pi-a", "screen"),
        track("trk-screen-b", "pi-b", "screen"),
      ],
      trackIntervals: [
        { trackId: "trk-a", startMs: 0, endMs: 10_000, state: "active" },
        { trackId: "trk-b", startMs: 0, endMs: 10_000, state: "active" },
        { trackId: "trk-screen-a", startMs: 2_000, endMs: 5_000, state: "active" },
        { trackId: "trk-screen-b", startMs: 5_000, endMs: 8_000, state: "active" },
      ],
      segments: [
        segment("upload-a", "trk-a", "pi-a", "camera", 0, 10_000),
        segment("upload-b", "trk-b", "pi-b", "camera", 0, 10_000),
        segment("upload-screen-a", "trk-screen-a", "pi-a", "screen", 2_000, 5_000),
        segment("upload-screen-b", "trk-screen-b", "pi-b", "screen", 5_000, 8_000),
      ],
    });

    const result = compileV2StagingPlan(value);
    const screenA = result.scenes[1]!.videoPasses[0]!;
    const screenB = result.scenes[2]!.videoPasses[0]!;

    expect(result.scenes).toHaveLength(4);
    expect(screenA.sourceSegments.map((item) => item.uploadId))
      .toContain("upload-screen-a");
    expect(screenB.sourceSegments.map((item) => item.uploadId))
      .toContain("upload-screen-b");
    expect(screenA.filtergraph).toContain("c=0x080d16");
    expect(screenA.filtergraph).toContain("c=0x060c16f0");
    expect(screenA.filtergraph).toContain("c=0x7dd3fc");
    expect(screenA.filtergraph).toContain("scale=938:602");
    expect(screenA.filtergraph).toContain("overlay=x=1008");
    expect(result).toMatchSnapshot();
  });
  it("renders a partial terminal gap as avatar fallback", () => {
    const value = manifest({
      completeness: "partial",
      participantInstances: [participant("pi-a", "Alex")],
      participantIntervals: [
        { participantInstanceId: "pi-a", startMs: 0, endMs: 10_000 },
      ],
      tracks: [track("trk-a", "pi-a", "camera")],
      trackIntervals: [
        { trackId: "trk-a", startMs: 0, endMs: 10_000, state: "active" },
      ],
      segments: [segment("upload-a", "trk-a", "pi-a", "camera", 0, 6_000)],
      gaps: [{
        startMs: 6_000,
        endMs: 10_000,
        scope: "track",
        trackId: "trk-a",
        reason: "terminal_capture_loss",
      }],
    });

    const result = compileV2StagingPlan(value);

    expect(result.scenes).toHaveLength(2);
    expect(result.scenes[0]!.videoPasses[0]!.sourceSegments).toHaveLength(1);
    expect(result.scenes[1]!.videoPasses[0]!.sourceSegments).toHaveLength(0);
    expect(result.scenes[1]!.videoPasses[0]!.filtergraph).toContain("text='Alex'");
    expect(result).toMatchSnapshot();
  });
});

describe("capture-v2 layout compatibility", () => {
  it("renders deterministic overflow instead of hiding unbounded screen participants", () => {
    const participants = Array.from(
      { length: 10 },
      (_, index) => participant(`pi-${index}`, `Person ${index}`),
    );
    const value = manifest({
      participantInstances: participants,
      participantIntervals: participants.map((item) => ({
        participantInstanceId: item.participantInstanceId,
        startMs: 0,
        endMs: 10_000,
      })),
      tracks: [track("trk-screen", "pi-0", "screen")],
      trackIntervals: [
        { trackId: "trk-screen", startMs: 0, endMs: 10_000, state: "active" },
      ],
      segments: [
        segment("upload-screen", "trk-screen", "pi-0", "screen", 0, 10_000),
      ],
    });
    expect(stagedFiltergraph(value)).toContain("text='+2'");
  });

  it("escapes adversarial UTF-8 display labels without filter expansion", () => {
    const value = manifest({
      participantInstances: [participant("pi-a", "A:%{eif};[],'\\\nЖ")],
      participantIntervals: [
        { participantInstanceId: "pi-a", startMs: 0, endMs: 10_000 },
      ],
      tracks: [],
      trackIntervals: [],
    });
    const filtergraph = stagedFiltergraph(value);
    expect(filtergraph).toContain("expansion=none");
    expect(filtergraph).not.toContain("\nЖ");
  });
  it("uses locale-independent Unicode initials", () => {
    const value = manifest({
      participantInstances: [participant("pi-a", "ёлка юрий")],
      participantIntervals: [
        { participantInstanceId: "pi-a", startMs: 0, endMs: 10_000 },
      ],
      tracks: [],
      trackIntervals: [],
    });
    expect(stagedFiltergraph(value)).toContain("text='ЁЮ'");
  });
  it("orders a rejoined participant by the active interval start", () => {
    const value = manifest({
      participantInstances: [
        participant("pi-a", "Alex"),
        participant("pi-b", "Blair"),
      ],
      participantIntervals: [
        { participantInstanceId: "pi-a", startMs: 0, endMs: 2_000 },
        { participantInstanceId: "pi-b", startMs: 1_000, endMs: 10_000 },
        { participantInstanceId: "pi-a", startMs: 6_000, endMs: 10_000 },
      ],
      tracks: [],
      trackIntervals: [],
    });
    const lastScene = compileV2StagingPlan(value).scenes.at(-1)!;
    const filtergraph = lastScene.videoPasses[0]!.filtergraph;
    expect(filtergraph.indexOf("text='B'")).toBeLessThan(
      filtergraph.indexOf("text='A'"),
    );
  });
  it("parses the audio compatibility primitives with the bundled pinned FFmpeg", () => {
    const leaf = (input: number, label: string) =>
      `[${input}:a]atrim=duration=0.01,asetpts=PTS-STARTPTS,aformat=sample_rates=48000:channel_layouts=stereo,aresample=48000,adelay=0|0,apad=whole_len=480,atrim=duration=0.01[${label}]`;
    const graph = `${leaf(0, "a0")};${leaf(1, "a1")};[a0][a1]amix=inputs=2:duration=longest:dropout_transition=0,volume=2[aout]`;
    const result = spawnSync(ffmpeg.path, [
      "-hide_banner", "-loglevel", "error",
      "-f", "lavfi", "-i", "sine=d=0.01",
      "-f", "lavfi", "-i", "sine=d=0.01",
      "-filter_complex", graph,
      "-map", "[aout]",
      "-f", "null", "-",
    ], { encoding: "utf8" });
    expect(result.status, result.stderr).toBe(0);
  });
});

describe("capture-v2 manifest and source bounds", () => {
  it("aggregates stable pages and rejects a cursor loop", async () => {
    const base: Omit<ManifestPage, "participantInstances" | "nextCursor"> = {
      protocolVersion: "recording-capture.v2",
      manifestSha256: hash,
      timelineEndMs: 10,
      completeness: "partial",
      participantIntervals: [],
      tracks: [],
      trackIntervals: [],
      segments: [],
      gaps: [],
    };
    const pages: ManifestPage[] = [
      { ...base, participantInstances: [participant("pi-a")], nextCursor: "next" },
      { ...base, participantInstances: [], nextCursor: null },
    ];
    let index = 0;

    const result = await loadCaptureV2Manifest(async () => pages[index++]!);

    expect(result.participantInstances).toHaveLength(1);
    await expect(loadCaptureV2Manifest(async () => ({
      ...base,
      participantInstances: [participant("pi-loop")],
      nextCursor: "same",
    }))).rejects.toThrow("cursor");
  });

  it("streams exactly expected bytes and rejects max plus one", async () => {
    const bytes = Buffer.from("0123456789");
    const descriptor = {
      ...segment("upload", "trk", "pi", "camera", 0, 1),
      expectedBytes: bytes.length,
      sha256: createHash("sha256").update(bytes).digest("hex"),
    };
    const directory = process.env.TEMP ?? process.cwd();
    const exact = join(directory, `composition-v2-${Date.now()}-exact`);
    const excess = join(directory, `composition-v2-${Date.now()}-excess`);

    await downloadBoundedSource(new Response(bytes), descriptor, exact);

    expect(await readFile(exact)).toEqual(bytes);
    await expect(downloadBoundedSource(
      new Response(Buffer.concat([bytes, Buffer.from("x")])),
      descriptor,
      excess,
    )).rejects.toThrow("byte bound");
    const { rm } = await import("node:fs/promises");
    await Promise.all([
      rm(exact, { force: true }),
      rm(excess, { force: true }),
    ]);
  });

  it("rejects probe media that is materially shorter than its descriptor", () => {
    const descriptor = segment("upload", "trk", "pi", "camera", 0, 10_000);
    const probe = (duration: number) => JSON.stringify({
      streams: [{ codec_type: "video", codec_name: "vp8" }],
      format: { duration: String(duration) },
    });
    expect(() => verifySourceProbe(probe(10), descriptor)).not.toThrow();
    expect(() => verifySourceProbe(probe(1), descriptor)).toThrow("probe verification");
  });
  it("requires a full-duration audio stream in the verified v2 artifact", () => {
    const probe = (audioDuration?: string) => JSON.stringify({
      streams: [
        {
          codec_type: "video",
          codec_name: "h264",
          width: 1280,
          height: 720,
          avg_frame_rate: "24/1",
        },
        {
          codec_type: "audio",
          codec_name: "aac",
          ...(audioDuration === undefined ? {} : { duration: audioDuration }),
        },
      ],
      format: { duration: "10" },
    });
    expect(() => verifyArtifactProbe(probe("10"), 10)).not.toThrow();
    expect(() => verifyArtifactProbe(probe("1"), 10)).toThrow("artifact verification");
    expect(() => verifyArtifactProbe(probe(), 10)).toThrow("artifact verification");
  });
  it("keeps faststart and the exact FFmpeg output cap", () => {
    expect(outputEncodingArguments(1234)).toEqual(
      expect.arrayContaining(["+faststart", "-fs", "1234"]),
    );
  });
});

describe("capture-v2 bounded staging", () => {
  it("partitions four hours of five-second segments without a temporal input cap", () => {
    const count = 2_880;
    const duration = 5_000;
    const segments = Array.from({ length: count }, (_, index) => ({
      ...segment(
        `upload-${index}`,
        "trk-camera",
        "pi-a",
        "camera",
        index * duration,
        (index + 1) * duration,
      ),
      sequence: index,
    }));
    const value = manifest({
      timelineEndMs: count * duration,
      participantInstances: [participant("pi-a", "Alex")],
      participantIntervals: [{ participantInstanceId: "pi-a", startMs: 0, endMs: count * duration }],
      tracks: [track("trk-camera", "pi-a", "camera")],
      trackIntervals: [{ trackId: "trk-camera", startMs: 0, endMs: count * duration, state: "active" }],
      segments,
    });
    const staged = compileV2StagingPlan(value);
    expect(staged.scenes).toHaveLength(count);
    expect(staged.scenes.every((scene) =>
      scene.videoPasses.every((pass) => pass.sourceSegments.length <= 63))).toBe(true);
  });

  it("layers simultaneous video in 63-source passes without a participant cap", () => {
    const participants = Array.from({ length: 130 }, (_, index) =>
      participant(`pi-${String(index).padStart(3, "0")}`, `Person ${index}`));
    const tracks = participants.map((item, index) =>
      track(`trk-${index}`, item.participantInstanceId, "camera"));
    const segments = tracks.map((item, index) =>
      segment(`upload-${index}`, item.trackId, item.participantInstanceId, "camera", 0, 10_000));
    const staged = compileV2StagingPlan(manifest({
      participantInstances: participants,
      participantIntervals: participants.map((item) => ({
        participantInstanceId: item.participantInstanceId,
        startMs: 0,
        endMs: 10_000,
      })),
      tracks,
      trackIntervals: tracks.map((item) => ({
        trackId: item.trackId,
        startMs: 0,
        endMs: 10_000,
        state: "active" as const,
      })),
      segments,
    }));
    expect(staged.scenes).toHaveLength(1);
    expect(staged.scenes[0]!.videoPasses.map((pass) => pass.sourceSegments.length))
      .toEqual([63, 63, 4]);
  });

  it("groups simultaneous microphones in lossless batches", () => {
    const participants = Array.from({ length: 129 }, (_, index) => participant(`pi-${index}`));
    const tracks = participants.map((item, index) => track(`trk-${index}`, item.participantInstanceId, "mic"));
    const segments = tracks.map((item, index) =>
      segment(`upload-${index}`, item.trackId, item.participantInstanceId, "mic", 0, 10_000));
    const staged = compileV2StagingPlan(manifest({
      participantInstances: participants,
      participantIntervals: participants.map((item) => ({
        participantInstanceId: item.participantInstanceId,
        startMs: 0,
        endMs: 10_000,
      })),
      tracks,
      trackIntervals: tracks.map((item) => ({
        trackId: item.trackId,
        startMs: 0,
        endMs: 10_000,
        state: "active" as const,
      })),
      segments,
    }));
    expect(staged.scenes[0]!.audioGroups.map((group) => group.length))
      .toEqual([64, 64, 1]);
  });

  it("globally encodes staged H264 and AAC only in the final output", () => {
    const arguments_ = stagedOutputEncodingArguments(1234);
    expect(arguments_).toEqual(expect.arrayContaining(["-c:v", "libx264", "-c:a", "aac"]));
    expect(arguments_.filter((value) => value === "aac")).toHaveLength(1);
    expect(arguments_).toEqual(expect.arrayContaining(["-fs", "1234"]));
  });
});
