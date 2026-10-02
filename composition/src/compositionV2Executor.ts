import { readdir, stat, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  compileAudioGroupFilter,
  compileAudioReduceFilter,
  compileSilentAudioFilter,
  compileV2StagingPlan,
  stagedOutputEncodingArguments,
  videoSeekLookbackMs,
  type CaptureV2Manifest,
  type ManifestSegment,
} from "./compositionV2.js";
import type { ProcessRunner } from "./processRunner.js";

type ExecuteV2CompositionOptions = {
  manifest: CaptureV2Manifest;
  sourcePaths: Map<string, string>;
  directory: string;
  ffmpegPath: string;
  runner: ProcessRunner;
  timeoutMs: number;
  maximumIntermediateBytes: number;
  maximumOutputBytes: number;
  maximumWorkspaceBytes: number;
  leaseHealthy: () => boolean;
  signal?: AbortSignal;
  verifySlice: (path: string, expectedDurationSeconds: number) => Promise<void>;
};

const maximumFiltergraphBytes = 64 * 1024 * 1024;

export async function executeV2Composition(
  options: ExecuteV2CompositionOptions,
): Promise<string> {
  if (!Number.isSafeInteger(options.maximumIntermediateBytes) ||
    options.maximumIntermediateBytes < 1 ||
    !Number.isSafeInteger(options.maximumOutputBytes) ||
    options.maximumOutputBytes < 1 ||
    !Number.isSafeInteger(options.maximumWorkspaceBytes) ||
    options.maximumWorkspaceBytes < options.maximumOutputBytes)
    throw new Error("invalid staged composition byte bound");
  const maximumSliceBytes = options.maximumIntermediateBytes + options.maximumOutputBytes;
  if (!Number.isSafeInteger(maximumSliceBytes))
    throw new Error("invalid staged composition byte bound");
  const plan = compileV2StagingPlan(options.manifest);
  const slices: Array<{ path: string; durationMs: number }> = [];
  let sliceBytes = 0;
  for (const [sceneIndex, scene] of plan.scenes.entries()) {
    assertLease(options);
    const prefix = `scene-${String(sceneIndex).padStart(6, "0")}`;
    let canvas = join(options.directory, `${prefix}-base.mkv`);
    const baseLimit = await remainingFileLimit(options, options.maximumIntermediateBytes);
    await runFilterStage(
      options,
      `${prefix}-base`,
      [],
      scene.baseFiltergraph,
      [
        "-map", "[vout]", ...losslessH264VideoArguments(),
        ...hardFileLimit(baseLimit), canvas,
      ],
    );
    await assertIntermediate(canvas, options.maximumIntermediateBytes);

    for (const [passIndex, pass] of scene.videoPasses.entries()) {
      const next = join(
        options.directory,
        `${prefix}-video-${String(passIndex).padStart(4, "0")}.mkv`,
      );
      const inputs = [canvas, ...pass.sourceSegments.map((segment) =>
        requiredSourcePath(options.sourcePaths, segment))];
      if (inputs.length > 64) throw new Error("staged video input bound exceeded");
      const passLimit = await remainingFileLimit(options, options.maximumIntermediateBytes);
      await runFilterStage(
        options,
        `${prefix}-video-${passIndex}`,
        inputs,
        pass.filtergraph,
        [
          "-map", "[vout]",
          ...losslessH264VideoArguments(),
          ...hardFileLimit(passLimit), next,
        ],
        [
          undefined,
          ...pass.sourceSegments.map((segment) => Math.max(
            0,
            (scene.startMs - segment.startMs - videoSeekLookbackMs) / 1_000,
          )),
        ],
        true,
      );
      await assertIntermediate(next, options.maximumIntermediateBytes);
      await rm(canvas, { force: true });
      canvas = next;
    }

    const audio = await renderSceneAudio(options, prefix, scene);
    const slice = join(options.directory, `${prefix}-slice.mkv`);
    const remainingSliceBytes = maximumSliceBytes - sliceBytes;
    const sliceLimit = Math.min(options.maximumIntermediateBytes, remainingSliceBytes);
    if (!Number.isSafeInteger(sliceLimit) || sliceLimit < 1)
      throw new Error("staged slice byte bound exceeded");
    const boundedSliceLimit = await remainingFileLimit(options, sliceLimit);
    await runFFmpeg(options, [
      "-hide_banner", "-loglevel", "error", "-y",
      "-i", canvas,
      "-i", audio,
      "-map", "0:v:0", "-map", "1:a:0",
      "-c", "copy",
      "-t", seconds((scene.endMs - scene.startMs) / 1_000),
      ...hardFileLimit(boundedSliceLimit),
      slice,
    ]);
    const currentSliceBytes = await assertIntermediate(
      slice,
      sliceLimit,
      "staged slice byte bound exceeded",
    );
    assertLease(options);
    await options.verifySlice(slice, (scene.endMs - scene.startMs) / 1_000);
    assertLease(options);
    sliceBytes += currentSliceBytes;
    if (!Number.isSafeInteger(sliceBytes) || sliceBytes > maximumSliceBytes)
      throw new Error("staged slice byte bound exceeded");
    await Promise.all([rm(canvas, { force: true }), rm(audio, { force: true })]);
    slices.push({ path: slice, durationMs: scene.endMs - scene.startMs });
  }

  if (!slices.length) throw new Error("v2 staging plan is empty");
  const concat = join(options.directory, "v2-slices.txt");
  await writeFile(
    concat,
    slices.flatMap(({ path, durationMs }) => [
      `file '${escapeConcatPath(path)}'`,
      `duration ${seconds(durationMs / 1_000)}`,
    ]).join("\n"),
    { encoding: "utf8", mode: 0o640 },
  );
  const output = join(options.directory, "output.mp4");
  const outputLimit = await remainingFileLimit(options, options.maximumOutputBytes);
  await runFFmpeg(options, [
    "-hide_banner", "-loglevel", "error", "-y",
    "-f", "concat", "-safe", "0", "-i", concat,
    "-map", "0:v:0", "-map", "0:a:0",
    "-vf", "fps=24,format=yuv420p",
    "-t", seconds(options.manifest.timelineEndMs / 1_000),
    ...stagedOutputEncodingArguments(outputLimit),
    output,
  ]);
  await assertIntermediate(
    output,
    options.maximumOutputBytes,
    "final artifact byte bound exceeded",
  );
  await Promise.all([
    ...slices.map(({ path }) => rm(path, { force: true })),
    rm(concat, { force: true }),
  ]);
  return output;
}

async function renderSceneAudio(
  options: ExecuteV2CompositionOptions,
  prefix: string,
  scene: ReturnType<typeof compileV2StagingPlan>["scenes"][number],
): Promise<string> {
  const durationMs = scene.endMs - scene.startMs;
  if (!scene.audioGroups.length) {
    const output = join(options.directory, `${prefix}-audio-silent.flac`);
    const audioLimit = await remainingFileLimit(options, options.maximumIntermediateBytes);
    await runFilterStage(
      options,
      `${prefix}-audio-silent`,
      [],
      compileSilentAudioFilter(durationMs),
      [
        "-map", "[aout]", ...flacAudioArguments(),
        ...hardFileLimit(audioLimit), output,
      ],
    );
    await assertIntermediate(output, options.maximumIntermediateBytes);
    return output;
  }

  const levels: Array<string | undefined> = [];
  for (const [groupIndex, group] of scene.audioGroups.entries()) {
    if (group.length > 64) throw new Error("staged audio input bound exceeded");
    let current = join(
      options.directory,
      `${prefix}-audio-group-${String(groupIndex).padStart(4, "0")}.flac`,
    );
    const groupLimit = await remainingFileLimit(options, options.maximumIntermediateBytes);
    await runFilterStage(
      options,
      `${prefix}-audio-group-${groupIndex}`,
      group.map((segment) => requiredSourcePath(options.sourcePaths, segment)),
      compileAudioGroupFilter(group, scene),
      [
        "-map", "[aout]", ...flacAudioArguments(),
        ...hardFileLimit(groupLimit), current,
      ],
    );
    await assertIntermediate(current, options.maximumIntermediateBytes);
    let level = 0;
    while (levels[level]) {
      current = await reduceAudio(
        options,
        prefix,
        durationMs,
        levels[level]!,
        current,
        `carry-${level}-${groupIndex}`,
      );
      levels[level] = undefined;
      level += 1;
    }
    levels[level] = current;
  }

  let result: string | undefined;
  for (const [level, value] of levels.entries()) {
    if (!value) continue;
    result = result
      ? await reduceAudio(options, prefix, durationMs, value, result, `final-${level}`)
      : value;
  }
  if (!result) throw new Error("staged audio reduction is empty");
  return result;
}

async function reduceAudio(
  options: ExecuteV2CompositionOptions,
  prefix: string,
  durationMs: number,
  left: string,
  right: string,
  suffix: string,
): Promise<string> {
  const output = join(options.directory, `${prefix}-audio-${suffix}.flac`);
  const reductionLimit = await remainingFileLimit(options, options.maximumIntermediateBytes);
  await runFilterStage(
    options,
    `${prefix}-audio-${suffix}`,
    [left, right],
    compileAudioReduceFilter(durationMs),
    [
      "-map", "[aout]", ...flacAudioArguments(),
      ...hardFileLimit(reductionLimit), output,
    ],
  );
  await assertIntermediate(output, options.maximumIntermediateBytes);
  await Promise.all([rm(left, { force: true }), rm(right, { force: true })]);
  return output;
}

async function runFilterStage(
  options: ExecuteV2CompositionOptions,
  name: string,
  inputs: string[],
  filtergraph: string,
  outputArguments: string[],
  inputSeekSeconds?: Array<number | undefined>,
  loopFirstInput = false,
): Promise<void> {
  const bytes = Buffer.byteLength(filtergraph, "utf8");
  if (bytes > maximumFiltergraphBytes)
    throw new Error("v2 staged filtergraph byte bound exceeded");
  const script = join(options.directory, `${name}-filter.txt`);
  await writeFile(script, filtergraph, { encoding: "utf8", mode: 0o640 });
  try {
    await runFFmpeg(options, [
      "-hide_banner", "-loglevel", "error", "-y",
      ...inputs.flatMap((path, index) => {
        const seek = inputSeekSeconds?.[index];
        return [
          ...(loopFirstInput && index === 0 ? ["-stream_loop", "-1"] : []),
          ...(seek && seek > 0 ? ["-ss", seconds(seek)] : []),
          "-i", path,
        ];
      }),
      "-filter_complex_script", script,
      ...outputArguments,
    ]);
  } finally {
    await rm(script, { force: true });
  }
}

async function runFFmpeg(
  options: ExecuteV2CompositionOptions,
  arguments_: string[],
): Promise<void> {
  assertLease(options);
  await options.runner.run(options.ffmpegPath, arguments_, options.timeoutMs, options.signal);
  assertLease(options);
}

function assertLease(options: ExecuteV2CompositionOptions): void {
  if (!options.leaseHealthy()) throw new Error("composition lease heartbeat failed");
}

async function assertIntermediate(
  path: string,
  maximumBytes: number,
  message = "staged intermediate byte bound exceeded",
): Promise<number> {
  const info = await stat(path);
  if (!info.isFile() || info.size < 1 || info.size > maximumBytes)
    throw new Error(message);
  return info.size;
}

function requiredSourcePath(
  paths: Map<string, string>,
  segment: ManifestSegment,
): string {
  const path = paths.get(segment.uploadId);
  if (!path) throw new Error("missing downloaded v2 source");
  return path;
}

function losslessH264VideoArguments(): string[] {
  return [
    "-an", "-c:v", "libx264", "-preset", "ultrafast", "-qp", "0",
    "-pix_fmt", "yuv420p",
  ];
}

function flacAudioArguments(): string[] {
  return ["-vn", "-c:a", "flac", "-sample_fmt", "s32"];
}

function hardFileLimit(maximumBytes: number): string[] {
  return ["-fs", String(maximumBytes)];
}

async function remainingFileLimit(
  options: ExecuteV2CompositionOptions,
  desiredBytes: number,
): Promise<number> {
  let usedBytes = 0;
  for (const entry of await readdir(options.directory, { withFileTypes: true })) {
    if (!entry.isFile()) continue;
    usedBytes += (await stat(join(options.directory, entry.name))).size;
    if (!Number.isSafeInteger(usedBytes) || usedBytes >= options.maximumWorkspaceBytes) {
      throw new Error("composition workspace byte bound exceeded");
    }
  }
  const remaining = options.maximumWorkspaceBytes - usedBytes;
  const limit = Math.min(desiredBytes, remaining);
  if (!Number.isSafeInteger(limit) || limit < 1) {
    throw new Error("composition workspace byte bound exceeded");
  }
  return limit;
}

function seconds(value: number): string {
  return value.toFixed(3).replace(/\.000$/, "");
}

function escapeConcatPath(path: string): string {
  return path.replaceAll("'", "'\\''");
}
