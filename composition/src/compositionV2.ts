import { createHash } from "node:crypto";
import { open } from "node:fs/promises";
import { CompositionFailure } from "./compositionFailure.js";
import {
  escapeDrawText,
  gridGeometry,
  initials,
  screenLayout,
  truncateLabel,
  type Geometry,
} from "./compositionV2Layout.js";

export type Source = "mic" | "camera" | "screen";
export type ManifestSegment = {
  uploadId: string;
  captureGeneration: number;
  trackId: string;
  participantInstanceId: string;
  source: Source;
  sequence: number;
  container: "webm" | "matroska";
  codec: "opus" | "vp8" | "h264";
  contentType: "audio/webm" | "video/webm" | "video/x-matroska";
  expectedBytes: number;
  sha256: string;
  startMs: number;
  endMs: number;
};
export type ManifestPage = {
  protocolVersion: "recording-capture.v2";
  manifestSha256: string;
  timelineEndMs: number;
  completeness: "complete" | "partial";
  participantInstances: Array<{
    participantInstanceId: string;
    displayName: string;
  }>;
  participantIntervals: Array<{
    participantInstanceId: string;
    startMs: number;
    endMs: number;
  }>;
  tracks: Array<{ trackId: string; participantInstanceId: string; source: Source }>;
  trackIntervals: Array<{
    trackId: string;
    startMs: number;
    endMs: number;
    state: "active" | "paused";
  }>;
  segments: ManifestSegment[];
  gaps: Array<{
    startMs: number;
    endMs: number;
    scope: "recording" | "track";
    trackId?: string;
    reason: string;
  }>;
  nextCursor: string | null;
};

export class RetryableSourceDownloadError extends CompositionFailure {
  constructor(_message: string, options?: ErrorOptions) {
    super(
      "SOURCE_DOWNLOAD_INTERRUPTED",
      "source",
      true,
      "Source download was interrupted",
      options,
    );
  }
}
export type CaptureV2Manifest = Omit<ManifestPage, "nextCursor">;

const categories = [
  "participantInstances",
  "participantIntervals",
  "tracks",
  "trackIntervals",
  "segments",
  "gaps",
] as const;
const maximumItems = 100_000;

export function outputEncodingArguments(maximumOutputBytes: number): string[] {
  if (!Number.isSafeInteger(maximumOutputBytes) || maximumOutputBytes < 1) {
    throw new Error("invalid composition output bound");
  }

  return [
    "-c:v", "libx264",
    "-preset", "fast",
    "-crf", "18",
    "-maxrate", "8M",
    "-bufsize", "16M",
    "-pix_fmt", "yuv420p",
    "-c:a", "aac",
    "-b:a", "192k",
    "-movflags", "+faststart",
    "-fs", String(maximumOutputBytes),
  ];
}

export async function loadCaptureV2Manifest(
  readPage: (cursor?: string) => Promise<ManifestPage>,
  limits: { maxSourceBytes: number; maxDurationMs: number } = {
    maxSourceBytes: 4_000_000_000,
    maxDurationMs: 14_400_000,
  },
): Promise<CaptureV2Manifest> {
  let cursor: string | undefined;
  let header: Pick<ManifestPage, "manifestSha256" | "timelineEndMs" | "completeness"> | undefined;
  const values = Object.fromEntries(
    categories.map((category) => [category, []]),
  ) as unknown as Pick<CaptureV2Manifest, typeof categories[number]>;
  const seenCursors = new Set<string>();
  for (let pageCount = 0; pageCount < 1_000; pageCount += 1) {
    const page = await readPage(cursor);
    validateManifestPage(page);
    const currentHeader = {
      manifestSha256: page.manifestSha256,
      timelineEndMs: page.timelineEndMs,
      completeness: page.completeness,
    };
    if (header && JSON.stringify(header) !== JSON.stringify(currentHeader)) {
      throw new Error("v2 manifest header changed across pages");
    }
    header ??= currentHeader;
    for (const category of categories) {
      values[category].push(...page[category] as never[]);
    }
    const itemCount = categories.reduce(
      (total, category) => total + values[category].length,
      0,
    );
    if (itemCount > maximumItems) {
      throw new Error("v2 manifest item bound exceeded");
    }
    if (page.nextCursor === null) {
      return validateCompleteManifest(
        { protocolVersion: "recording-capture.v2", ...header, ...values },
        limits,
      );
    }
    if (seenCursors.has(page.nextCursor)) {
      throw new Error("v2 manifest cursor did not advance");
    }
    seenCursors.add(page.nextCursor);
    cursor = page.nextCursor;
  }
  throw new Error("v2 manifest page bound exceeded");
}

function validateManifestPage(page: ManifestPage): void {
  const invalidEnvelope = page.protocolVersion !== "recording-capture.v2"
    || !isHash(page.manifestSha256)
    || !Number.isSafeInteger(page.timelineEndMs)
    || page.timelineEndMs <= 0
    || (page.completeness !== "complete" && page.completeness !== "partial");
  if (invalidEnvelope) {
    throw new Error("invalid v2 manifest envelope");
  }
  if (categories.some((category) => !Array.isArray(page[category]))) {
    throw new Error("invalid v2 manifest categories");
  }
  const count = categories.reduce((total, category) => total + page[category].length, 0);
  if (count > 100 || (count === 0 && page.nextCursor !== null)) {
    throw new Error("invalid v2 manifest page bound");
  }
}

function validateCompleteManifest(
  manifest: CaptureV2Manifest,
  limits: { maxSourceBytes: number; maxDurationMs: number },
): CaptureV2Manifest {
  if (manifest.timelineEndMs > limits.maxDurationMs) {
    throw new Error("v2 manifest duration bound exceeded");
  }
  unique(manifest.participantInstances, (item) => item.participantInstanceId);
  unique(manifest.tracks, (item) => item.trackId);
  unique(manifest.segments, (item) => `${item.captureGeneration}:${item.trackId}:${item.sequence}`);
  unique(manifest.segments, (item) => item.uploadId);
  unique(manifest.participantIntervals, (item) => `${item.participantInstanceId}:${item.startMs}:${item.endMs}`);
  unique(manifest.trackIntervals, (item) => `${item.trackId}:${item.startMs}:${item.endMs}:${item.state}`);
  unique(manifest.gaps, (item) => `${item.scope}:${item.trackId ?? ""}:${item.startMs}:${item.endMs}:${item.reason}`);
  const participants = new Set(manifest.participantInstances.map((item) => item.participantInstanceId));
  const tracks = new Map(manifest.tracks.map((item) => [item.trackId, item]));
  for (const item of manifest.participantInstances) {
    if (!item.displayName || [...item.displayName].length > 160) {
      throw new Error("invalid v2 participant metadata");
    }
  }
  for (const item of manifest.participantIntervals) {
    validateInterval(
      item.startMs,
      item.endMs,
      manifest.timelineEndMs,
      participants.has(item.participantInstanceId),
    );
  }
  for (const item of manifest.tracks) {
    const validOwner = participants.has(item.participantInstanceId);
    const validSource = ["mic", "camera", "screen"].includes(item.source);
    if (!validOwner || !validSource) {
      throw new Error("v2 track owner or source is invalid");
    }
  }
  for (const item of manifest.trackIntervals) {
    validateInterval(
      item.startMs,
      item.endMs,
      manifest.timelineEndMs,
      tracks.has(item.trackId),
    );
    if (item.state !== "active" && item.state !== "paused") {
      throw new Error("invalid v2 track state");
    }
  }
  for (const item of manifest.segments) {
    validateSegment(item, tracks.get(item.trackId), manifest.timelineEndMs);
  }
  for (const item of manifest.gaps) {
    const validScope = item.scope === "recording"
      || (item.scope === "track" && Boolean(item.trackId && tracks.has(item.trackId)));
    validateInterval(item.startMs, item.endMs, manifest.timelineEndMs, validScope);
    if (!item.reason) {
      throw new Error("invalid v2 gap");
    }
  }
  validateNonOverlapping(manifest.participantIntervals, (item) => item.participantInstanceId);
  validateNonOverlapping(manifest.trackIntervals, (item) => item.trackId);
  validateNonOverlapping(manifest.segments, (item) => item.trackId);
  validateSegmentPlacement(manifest);
  if (manifest.segments.length === 0) {
    validateZeroMediaTimeline(manifest);
  }
  const sourceBytes = manifest.segments.reduce(
    (total, segment) => total + segment.expectedBytes,
    0,
  );
  if (!Number.isSafeInteger(sourceBytes) || sourceBytes > limits.maxSourceBytes) {
    throw new Error("v2 manifest source byte bound exceeded");
  }
  return manifest;
}

function validateZeroMediaTimeline(manifest: CaptureV2Manifest): void {
  if (manifest.timelineEndMs <= 0 || manifest.participantIntervals.length === 0) {
    throw new Error("zero-media v2 manifest has no authoritative participant timeline");
  }
}

function validateInterval(
  startMs: number,
  endMs: number,
  timelineEndMs: number,
  identityExists: boolean,
): void {
  const invalidInterval = !identityExists
    || !Number.isSafeInteger(startMs)
    || !Number.isSafeInteger(endMs)
    || startMs < 0
    || endMs <= startMs
    || endMs > timelineEndMs;
  if (invalidInterval) {
    throw new Error("invalid v2 timeline interval");
  }
}

function validateSegment(
  segment: ManifestSegment,
  track: CaptureV2Manifest["tracks"][number] | undefined,
  timelineEndMs: number,
): void {
  validateInterval(segment.startMs, segment.endMs, timelineEndMs, Boolean(track));
  const invalidDescriptor = !track
    || segment.participantInstanceId !== track.participantInstanceId
    || segment.source !== track.source
    || !Number.isInteger(segment.captureGeneration)
    || segment.captureGeneration <= 0
    || !Number.isInteger(segment.sequence)
    || segment.sequence < 0
    || !Number.isSafeInteger(segment.expectedBytes)
    || segment.expectedBytes <= 0
    || !isHash(segment.sha256);
  if (invalidDescriptor) {
    throw new Error("invalid v2 segment descriptor");
  }

  const validMediaTuple = segment.source === "mic"
    ? segment.codec === "opus"
      && segment.container === "webm"
      && segment.contentType === "audio/webm"
    : (segment.codec === "vp8"
        && segment.container === "webm"
        && segment.contentType === "video/webm")
      || (segment.codec === "h264"
        && segment.container === "matroska"
        && segment.contentType === "video/x-matroska");
  if (!validMediaTuple) {
    throw new Error("unsupported v2 segment media tuple");
  }
}
function unique<T>(items: T[], key: (item: T) => string): void {
  const values = new Set<string>();
  for (const item of items) {
    const value = key(item);
    if (values.has(value)) {
      throw new Error("duplicate v2 manifest item");
    }
    values.add(value);
  }
}

function validateNonOverlapping<T extends { startMs: number; endMs: number }>(
  items: T[],
  identity: (item: T) => string,
): void {
  const ordered = [...items].sort((left, right) =>
    identity(left).localeCompare(identity(right)) ||
    left.startMs - right.startMs ||
    left.endMs - right.endMs);
  let previous: T | undefined;

  for (const item of ordered) {
    if (previous &&
      identity(previous) === identity(item) &&
      item.startMs < previous.endMs) {
      throw new Error("overlapping v2 manifest intervals");
    }
    previous = item;
  }
}

function validateSegmentPlacement(manifest: CaptureV2Manifest): void {
  const activeByTrack = new Map<string, Array<{ startMs: number; endMs: number }>>();
  for (const interval of manifest.trackIntervals) {
    if (interval.state !== "active") {
      continue;
    }
    const values = activeByTrack.get(interval.trackId) ?? [];
    values.push(interval);
    activeByTrack.set(interval.trackId, values);
  }

  for (const segment of manifest.segments) {
    const active = activeByTrack.get(segment.trackId) ?? [];
    const isInsideActiveInterval = active.some(
      (interval) => segment.startMs >= interval.startMs
        && segment.endMs <= interval.endMs,
    );
    if (!isInsideActiveInterval) {
      throw new Error("v2 segment is outside an active track interval");
    }
  }
}

function isHash(value: string): boolean {
  return /^[0-9a-f]{64}$/.test(value);
}

export async function downloadBoundedSource(
  response: Response,
  segment: ManifestSegment,
  path: string,
): Promise<void> {
  if (!response.ok) {
    throw new Error(`segment download failed: ${response.status}`);
  }
  if (!response.body) {
    throw new RetryableSourceDownloadError("segment download body is absent");
  }
  const declared = response.headers.get("content-length");
  if (declared !== null && Number(declared) !== segment.expectedBytes) {
    throw new Error("segment content-length mismatch");
  }
  const file = await open(path, "wx", 0o640);
  const hash = createHash("sha256");
  let written = 0;
  try {
    const reader = response.body.getReader();
    for (;;) {
      let chunk: ReadableStreamReadResult<Uint8Array>;
      try {
        chunk = await reader.read();
      } catch (error) {
        throw new RetryableSourceDownloadError("segment response body interrupted", { cause: error });
      }
      if (chunk.done) {
        break;
      }
      if (written + chunk.value.byteLength > segment.expectedBytes) {
        await reader.cancel();
        throw new Error("segment download exceeded byte bound");
      }
      written += chunk.value.byteLength;
      hash.update(chunk.value);
      await file.write(chunk.value);
    }
    if (written !== segment.expectedBytes) {
      throw new RetryableSourceDownloadError("segment response body ended early");
    }
    if (hash.digest("hex") !== segment.sha256) {
      throw new Error("segment checksum mismatch");
    }
    await file.sync();
  } finally {
    await file.close();
  }
}

export function verifySourceProbe(raw: string, segment: ManifestSegment, toleranceSeconds = 0.25): void {
  const parsed = JSON.parse(raw) as {
    streams?: Array<{ codec_type?: string; codec_name?: string }>;
    format?: { duration?: string };
    packets?: Array<{ pts_time?: string; duration_time?: string }>;
  };
  const expectedKind = segment.source === "mic" ? "audio" : "video";
  const stream = parsed.streams?.find((item) => item.codec_type === expectedKind);
  const formatDuration = Number(parsed.format?.duration);
  const packetBounds = (parsed.packets ?? []).map((packet) => {
    const start = Number(packet.pts_time);
    const packetDuration = Number(packet.duration_time ?? 0);
    return { start, end: start + packetDuration };
  }).filter(({ start, end }) => Number.isFinite(start) && Number.isFinite(end));
  const packetDuration = packetBounds.length > 0
    ? Math.max(...packetBounds.map(({ end }) => end)) - Math.min(...packetBounds.map(({ start }) => start))
    : Number.NaN;
  const hasContainerDuration = Number.isFinite(formatDuration);
  const duration = hasContainerDuration ? formatDuration : packetDuration;
  const expectedDuration = (segment.endMs - segment.startMs) / 1_000;
  const durationTolerance = hasContainerDuration
    ? toleranceSeconds
    : Math.max(toleranceSeconds, expectedDuration * 0.15);
  // Video RTP timestamps describe frames, not wall-clock source availability.
  // A screen share (or a camera that is about to stop) can legitimately emit
  // only a few frames and rely on the compositor's tpad clone for the rest of
  // the active interval. Audio is continuous, so a short audio source still
  // indicates missing capture data and must fail validation.
  const isMateriallyShort = expectedKind === "audio"
    && duration < expectedDuration - durationTolerance;
  const invalidProbe = !stream
    || stream.codec_name !== segment.codec
    || !Number.isFinite(duration)
    || duration <= 0
    || isMateriallyShort
    || duration > expectedDuration + durationTolerance;
  if (invalidProbe) {
    throw new Error("v2 source probe verification failed");
  }
}

export function verifyArtifactProbe(
  raw: string,
  expectedDurationSeconds?: number,
): { durationSeconds: number; width: number; height: number; frameRate: number } {
  const parsed = JSON.parse(raw) as {
    streams?: Array<{
      codec_type?: string;
      codec_name?: string;
      width?: number;
      height?: number;
      avg_frame_rate?: string;
      duration?: string;
    }>;
    format?: { duration?: string };
  };
  const video = parsed.streams?.find((stream) => stream.codec_type === "video");
  const audio = parsed.streams?.find((stream) => stream.codec_type === "audio");
  const durationSeconds = Number(parsed.format?.duration);
  const audioDuration = audio?.duration === undefined ? undefined : Number(audio.duration);
  const [numerator, denominator] = (video?.avg_frame_rate ?? "0/1").split("/").map(Number);
  const frameRate = numerator! / denominator!;
  const invalidShape = !video
    || !audio
    || video.codec_name !== "h264"
    || audio.codec_name !== "aac"
    || video.width !== 1280
    || video.height !== 720
    || !Number.isFinite(durationSeconds)
    || durationSeconds <= 0
    || !Number.isFinite(frameRate)
    || Math.abs(frameRate - 24) > 0.1;
  const invalidAudioDuration = audioDuration !== undefined
    && (!Number.isFinite(audioDuration)
      || audioDuration <= 0
      || Math.abs(audioDuration - durationSeconds) > 0.25);
  const invalidTimeline = expectedDurationSeconds !== undefined
    && (Math.abs(durationSeconds - expectedDurationSeconds) > 0.25
      || audioDuration === undefined
      || Math.abs(audioDuration - expectedDurationSeconds) > 0.25);
  if (invalidShape || invalidAudioDuration || invalidTimeline) {
    throw new Error("final artifact verification failed");
  }
  return { durationSeconds, width: video.width!, height: video.height!, frameRate };
}

export function verifyStagedSliceProbe(
  raw: string,
  expectedDurationSeconds: number,
  toleranceSeconds = 0.25,
): void {
  const parsed = JSON.parse(raw) as {
    streams?: Array<{ codec_type?: string; codec_name?: string }>;
    format?: { duration?: string };
  };
  const video = parsed.streams?.find((stream) => stream.codec_type === "video");
  const audio = parsed.streams?.find((stream) => stream.codec_type === "audio");
  const duration = Number(parsed.format?.duration);
  const invalidProbe = video?.codec_name !== "h264"
    || audio?.codec_name !== "flac"
    || !Number.isFinite(duration)
    || Math.abs(duration - expectedDurationSeconds) > toleranceSeconds;
  if (invalidProbe) {
    throw new Error("staged slice probe verification failed");
  }
}

export type ElementaryScene = {
  startMs: number;
  endMs: number;
  participants: CaptureV2Manifest["participantInstances"];
  cameraSegments: Map<string, ManifestSegment>;
  micSegments: ManifestSegment[];
  screen?: ManifestSegment;
};
export type VideoOverlayPass = {
  sourceSegments: ManifestSegment[];
  filtergraph: string;
};
export type StagedScenePlan = {
  startMs: number;
  endMs: number;
  baseFiltergraph: string;
  videoPasses: VideoOverlayPass[];
  audioGroups: ManifestSegment[][];
};
export type V2StagingPlan = { scenes: StagedScenePlan[] };

const maximumVideoSourcesPerPass = 63;
const maximumAudioSourcesPerGroup = 64;
export const videoSeekLookbackMs = 1_000;

export function compileV2StagingPlan(manifest: CaptureV2Manifest): V2StagingPlan {
  const segments = [...manifest.segments].sort(compareSegments);
  return {
    scenes: buildScenes(manifest, segments).map((scene) => ({
      startMs: scene.startMs,
      endMs: scene.endMs,
      baseFiltergraph: compileSceneBase(scene),
      videoPasses: compileSceneVideoPasses(manifest, scene),
      audioGroups: chunk(scene.micSegments, maximumAudioSourcesPerGroup),
    })),
  };
}

export function compileAudioGroupFilter(
  segments: ManifestSegment[],
  scene: Pick<StagedScenePlan, "startMs" | "endMs">,
): string {
  if (segments.length < 1 || segments.length > maximumAudioSourcesPerGroup) {
    throw new Error("invalid staged audio group size");
  }
  const durationMs = scene.endMs - scene.startMs;
  const duration = seconds(durationMs / 1_000);
  const samples = durationMs * 48;
  const filters: string[] = [];
  const leaves = segments.map((segment, index) => {
    const offset = (scene.startMs - segment.startMs) / 1_000;
    const label = `a${index}`;
    filters.push(
      `[${index}:a]atrim=start=${seconds(offset)}:duration=${duration},asetpts=PTS-STARTPTS,aformat=sample_rates=48000:channel_layouts=stereo,aresample=48000,apad=whole_len=${samples},atrim=duration=${duration}[${label}]`,
    );
    return label;
  });
  buildAudioMixTree(filters, leaves, duration);
  return filters.join(";");
}

export function compileAudioReduceFilter(durationMs: number): string {
  if (!Number.isSafeInteger(durationMs) || durationMs < 1) {
    throw new Error("invalid staged audio duration");
  }
  const duration = seconds(durationMs / 1_000);
  return `[0:a][1:a]amix=inputs=2:duration=longest:dropout_transition=0,volume=2,atrim=duration=${duration}[aout]`;
}

export function compileSilentAudioFilter(durationMs: number): string {
  if (!Number.isSafeInteger(durationMs) || durationMs < 1) {
    throw new Error("invalid staged audio duration");
  }
  return `anullsrc=r=48000:cl=stereo,atrim=duration=${seconds(durationMs / 1_000)}[aout]`;
}

export function stagedOutputEncodingArguments(maximumOutputBytes: number): string[] {
  if (!Number.isSafeInteger(maximumOutputBytes) || maximumOutputBytes < 1) {
    throw new Error("invalid composition output bound");
  }
  return [
    "-c:v", "libx264",
    "-preset", "fast",
    "-crf", "18",
    "-maxrate", "8M",
    "-bufsize", "16M",
    "-pix_fmt", "yuv420p",
    "-c:a", "aac",
    "-b:a", "192k",
    "-movflags", "+faststart",
    "-fs", String(maximumOutputBytes),
  ];
}

function compileSceneBase(scene: ElementaryScene): string {
  const duration = seconds((scene.endMs - scene.startMs) / 1_000);
  return `color=c=0x090f1c:s=1280x720:r=24:d=${duration},drawbox=x=0:y=0:w=460:h=720:c=0x17223a80:t=fill,drawbox=x=920:y=0:w=360:h=720:c=0x131b2c80:t=fill,drawgrid=w=64:h=64:t=1:c=0x94a3b80e,format=yuv420p[vout]`;
}

function compileSceneVideoPasses(
  manifest: CaptureV2Manifest,
  scene: ElementaryScene,
): VideoOverlayPass[] {
  const durationMs = scene.endMs - scene.startMs;
  const duration = seconds(durationMs / 1_000);
  // Sub-frame scenes cannot carry a distinct 24-fps visual state. Reusing the
  // already-rendered base avoids a header-only framesync output when even a
  // generated avatar overlay has no timestamp in this interval.
  if (durationMs < Math.ceil(1_000 / 24)) return [];
  const passes: VideoOverlayPass[] = [];
  let filters: string[] = [];
  let sources: ManifestSegment[] = [];
  let current = "canvas";
  let layer = 0;
  let passIndex = 0;

  const beginPass = (): void => {
    filters = [`[0:v]trim=duration=${duration},setpts=PTS-STARTPTS[canvas]`];
    sources = [];
    current = "canvas";
  };
  const finishPass = (): void => {
    filters.push(`[${current}]trim=duration=${duration},setpts=PTS-STARTPTS,format=yuv420p[vout]`);
    passes.push({ sourceSegments: sources, filtergraph: filters.join(";") });
    passIndex += 1;
    beginPass();
  };
  const staticOverlay = (source: string, x: number, y: number): void => {
    const next = `p${passIndex}layer${layer++}`;
    filters.push(`[${current}][${source}]overlay=x=${x}:y=${y}[${next}]`);
    current = next;
  };
  const mediaOverlay = (
    segment: ManifestSegment,
    geometry: Geometry,
    contain: boolean,
  ): void => {
    if (sources.length === maximumVideoSourcesPerPass) {
      finishPass();
    }
    sources.push(segment);
    const input = sources.length;
    const media = `p${passIndex}media${layer++}`;
    const next = `p${passIndex}layer${layer++}`;
    const offsetMs = scene.startMs - segment.startMs;
    const lookbackMs = Math.min(offsetMs, videoSeekLookbackMs);
    const offset = seconds(lookbackMs / 1_000);
    const padding = seconds((lookbackMs + durationMs) / 1_000);
    const scale = contain
      ? `scale=${geometry.width}:${geometry.height}:force_original_aspect_ratio=decrease,pad=${geometry.width}:${geometry.height}:(ow-iw)/2:(oh-ih)/2`
      : `scale=${geometry.width}:${geometry.height}:force_original_aspect_ratio=increase,crop=${geometry.width}:${geometry.height}`;
    filters.push(`[${input}:v]tpad=stop_mode=clone:stop_duration=${padding},trim=start=${offset}:duration=${duration},setpts=PTS-STARTPTS,${scale}[${media}]`);
    // The executor seeks to at most one second before the scene. Padding before
    // this residual trim preserves a final decoded frame across an accepted
    // terminal tail without making framesync scan the whole segment prefix.
    filters.push(`[${current}][${media}]overlay=x=${geometry.x}:y=${geometry.y}:eof_action=pass[${next}]`);
    current = next;
  };

  beginPass();
  const screenOwner = scene.screen
    ? manifest.tracks.find((item) => item.trackId === scene.screen!.trackId)!
      .participantInstanceId
    : undefined;
  const allVisible = screenOwner
    ? scene.participants.filter((item) => item.participantInstanceId !== screenOwner)
    : scene.participants;
  const screen = scene.screen ? screenLayout(allVisible.length) : undefined;
  const visible = screen ? allVisible.slice(0, screen.tiles.length) : allVisible;
  visible.forEach((participant, position) => {
    const geometry = screen ? screen.tiles[position]! : gridGeometry(position, visible.length);
    const avatar = `p${passIndex}avatar${layer++}`;
    filters.push(`color=c=0x172033:s=${geometry.width}x${geometry.height}:r=24:d=${duration},drawbox=x=0:y=0:w=iw:h=ih:c=0x243b66:t=fill,drawtext=expansion=none:text='${escapeDrawText(initials(participant.displayName))}':fontcolor=0x8fb4f8:fontsize=${Math.max(28, Math.floor(geometry.height * 0.18))}:x=(w-text_w)/2:y=(h-text_h)/2[${avatar}]`);
    staticOverlay(avatar, geometry.x, geometry.y);
    const camera = scene.cameraSegments.get(participant.participantInstanceId);
    if (camera) {
      mediaOverlay(camera, geometry, false);
    }
    const named = `p${passIndex}name${layer++}`;
    filters.push(`[${current}]drawtext=expansion=none:text='${escapeDrawText(truncateLabel(participant.displayName, 40))}':fontcolor=0xf8fafc:fontsize=${Math.max(14, Math.min(18, Math.floor(geometry.width * 0.09)))}:x=${geometry.x + 22}:y=${geometry.y + geometry.height - 36}:box=1:boxborderw=8:boxcolor=0x030812d1[${named}]`);
    current = named;
  });
  if (screen && allVisible.length > screen.tiles.length) {
    const geometry = screen.tiles.at(-1)!;
    const overflow = `p${passIndex}overflow${layer++}`;
    filters.push(`color=c=0x1e293b:s=${geometry.width}x${geometry.height}:r=24:d=${duration},drawtext=expansion=none:text='+${allVisible.length - screen.tiles.length + 1}':fontcolor=white:fontsize=28:x=(w-text_w)/2:y=(h-text_h)/2[${overflow}]`);
    staticOverlay(overflow, geometry.x, geometry.y);
  }
  if (scene.screen) {
    const content = {
      x: screen!.stage.x + 12,
      y: screen!.stage.y + 50,
      width: screen!.stage.width - 24,
      height: screen!.stage.height - 62,
    };
    const card = `p${passIndex}screencard${layer++}`;
    filters.push(`color=c=0x080d16:s=${screen!.stage.width}x${screen!.stage.height}:r=24:d=${duration}[${card}]`);
    staticOverlay(card, screen!.stage.x, screen!.stage.y);
    mediaOverlay(scene.screen, content, true);
    const track = manifest.tracks.find((item) => item.trackId === scene.screen!.trackId)!;
    const owner = manifest.participantInstances.find(
      (item) => item.participantInstanceId === track.participantInstanceId,
    )!;
    const named = `p${passIndex}screenname${layer++}`;
    filters.push(`[${current}]drawbox=x=${screen!.stage.x + 12}:y=${screen!.stage.y + 12}:w=${screen!.stage.width - 24}:h=28:c=0x060c16f0:t=fill,drawbox=x=${screen!.stage.x + 25}:y=${screen!.stage.y + 21}:w=10:h=10:c=0x7dd3fc:t=fill,drawtext=expansion=none:text='${escapeDrawText(`Screen share · ${truncateLabel(owner.displayName, 72)}`)}':fontcolor=0xf8fafc:fontsize=17:x=${screen!.stage.x + 44}:y=${screen!.stage.y + 18}[${named}]`);
    current = named;
  }
  finishPass();
  return passes;
}

function compareSegments(left: ManifestSegment, right: ManifestSegment): number {
  return left.startMs - right.startMs
    || left.captureGeneration - right.captureGeneration
    || left.sequence - right.sequence
    || left.uploadId.localeCompare(right.uploadId);
}

function chunk<T>(items: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    result.push(items.slice(index, index + size));
  }
  return result;
}

function buildAudioMixTree(filters: string[], leaves: string[], timelineSeconds: string): void {
  let level = [...leaves];
  let depth = 0;
  while (level.length > 1) {
    const next: string[] = [];
    for (let index = 0; index < level.length; index += 2) {
      const left = level[index]!;
      const right = level[index + 1];
      if (!right) {
        next.push(left);
        continue;
      }
      const output = `amix${depth}_${index / 2}`;
      filters.push(`[${left}][${right}]amix=inputs=2:duration=longest:dropout_transition=0,volume=2[${output}]`);
      next.push(output);
    }
    level = next;
    depth += 1;
  }
  filters.push(`[${level[0]}]atrim=duration=${timelineSeconds}[aout]`);
}

function seconds(value: number): string {
  return value.toFixed(3).replace(/\.000$/, "");
}

function buildScenes(manifest: CaptureV2Manifest, segments: ManifestSegment[]): ElementaryScene[] {
  const range = { startMs: 0, endMs: manifest.timelineEndMs };
  const boundaries = new Set([range.startMs, range.endMs]);
  for (const collection of [
    manifest.participantIntervals,
    manifest.trackIntervals,
    manifest.gaps,
    segments,
  ]) {
    for (const item of collection) {
      if (item.startMs > range.startMs && item.startMs < range.endMs) {
        boundaries.add(item.startMs);
      }
      if (item.endMs > range.startMs && item.endMs < range.endMs) {
        boundaries.add(item.endMs);
      }
    }
  }
  const ordered = [...boundaries].sort((a, b) => a - b);
  const participantById = new Map(
    manifest.participantInstances.map((item) => [item.participantInstanceId, item]),
  );
  const trackById = new Map(manifest.tracks.map((item) => [item.trackId, item]));
  const participantStarts = new Map<number, typeof manifest.participantIntervals>();
  const participantEnds = new Map<number, typeof manifest.participantIntervals>();
  const trackStarts = new Map<number, typeof manifest.trackIntervals>();
  const trackEnds = new Map<number, typeof manifest.trackIntervals>();
  const segmentStarts = new Map<number, ManifestSegment[]>();
  const segmentEnds = new Map<number, ManifestSegment[]>();
  const gapStarts = new Map<number, typeof manifest.gaps>();
  const gapEnds = new Map<number, typeof manifest.gaps>();
  const activeParticipants = new Map<string, number>();
  const activeTracks = new Map<string, "active" | "paused">();
  const activeSegments = new Map<string, ManifestSegment>();
  const trackGaps = new Map<string, number>();
  let recordingGaps = 0;

  for (const item of manifest.participantIntervals) {
    addAt(participantStarts, item.startMs, item);
    addAt(participantEnds, item.endMs, item);
    if (item.startMs <= range.startMs && item.endMs > range.startMs) {
      activeParticipants.set(item.participantInstanceId, item.startMs);
    }
  }
  for (const item of manifest.trackIntervals) {
    addAt(trackStarts, item.startMs, item);
    addAt(trackEnds, item.endMs, item);
    if (item.startMs <= range.startMs && item.endMs > range.startMs) {
      activeTracks.set(item.trackId, item.state);
    }
  }
  for (const item of segments) {
    addAt(segmentStarts, item.startMs, item);
    addAt(segmentEnds, item.endMs, item);
    if (item.startMs <= range.startMs && item.endMs > range.startMs) {
      activeSegments.set(item.uploadId, item);
    }
  }
  for (const item of manifest.gaps) {
    addAt(gapStarts, item.startMs, item);
    addAt(gapEnds, item.endMs, item);
    if (item.startMs <= range.startMs && item.endMs > range.startMs) {
      if (item.scope === "recording") {
        recordingGaps += 1;
      } else if (item.trackId) {
        adjustCount(trackGaps, item.trackId, 1);
      }
    }
  }

  const scenes: ElementaryScene[] = [];
  for (let index = 0; index < ordered.length - 1; index += 1) {
    const startMs = ordered[index]!;
    const endMs = ordered[index + 1]!;
    if (endMs <= startMs) {
      continue;
    }
    if (startMs > range.startMs) {
      for (const item of participantEnds.get(startMs) ?? []) {
        activeParticipants.delete(item.participantInstanceId);
      }
      for (const item of participantStarts.get(startMs) ?? []) {
        activeParticipants.set(item.participantInstanceId, item.startMs);
      }
      for (const item of trackEnds.get(startMs) ?? []) {
        activeTracks.delete(item.trackId);
      }
      for (const item of trackStarts.get(startMs) ?? []) {
        activeTracks.set(item.trackId, item.state);
      }
      for (const item of segmentEnds.get(startMs) ?? []) {
        activeSegments.delete(item.uploadId);
      }
      for (const item of segmentStarts.get(startMs) ?? []) {
        activeSegments.set(item.uploadId, item);
      }
      for (const item of gapEnds.get(startMs) ?? []) {
        if (item.scope === "recording") {
          recordingGaps -= 1;
        } else if (item.trackId) {
          adjustCount(trackGaps, item.trackId, -1);
        }
      }
      for (const item of gapStarts.get(startMs) ?? []) {
        if (item.scope === "recording") {
          recordingGaps += 1;
        } else if (item.trackId) {
          adjustCount(trackGaps, item.trackId, 1);
        }
      }
    }
    const participants = [...activeParticipants]
      .sort((left, right) => left[1] - right[1] || left[0].localeCompare(right[0]))
      .map(([id]) => participantById.get(id)!);
    const covering = recordingGaps > 0
      ? []
      : [...activeSegments.values()].filter((item) =>
        activeTracks.get(item.trackId) === "active" && !trackGaps.has(item.trackId));
    const screens = covering.filter((item) => item.source === "screen");
    if (screens.length > 1) {
      throw new Error("overlapping active screens in v2 manifest");
    }
    const cameraSegments = new Map<string, ManifestSegment>();
    for (const segment of covering.filter((item) => item.source === "camera")) {
      const owner = trackById.get(segment.trackId)!.participantInstanceId;
      if (cameraSegments.has(owner)) {
        throw new Error("overlapping camera segments in v2 manifest");
      }
      cameraSegments.set(owner, segment);
    }
    const micSegments = covering.filter((item) => item.source === "mic").sort(compareSegments);
    const scene: ElementaryScene = {
      startMs,
      endMs,
      participants,
      cameraSegments,
      micSegments,
    };
    if (screens[0]) {
      scene.screen = screens[0];
    }
    scenes.push(scene);
  }
  return scenes;
}

function addAt<T>(values: Map<number, T[]>, time: number, value: T): void {
  const bucket = values.get(time);
  if (bucket) {
    bucket.push(value);
  } else {
    values.set(time, [value]);
  }
}

function adjustCount(values: Map<string, number>, key: string, delta: number): void {
  const next = (values.get(key) ?? 0) + delta;
  if (next > 0) {
    values.set(key, next);
  } else {
    values.delete(key);
  }
}
