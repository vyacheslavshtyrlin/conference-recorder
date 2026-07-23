import ffmpeg from "@ffmpeg-installer/ffmpeg";
import ffprobe from "@ffprobe-installer/ffprobe";
import { createHash } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { readConfig } from "./config.js";
import { ChildProcessRunner } from "./recordingSink.js";

const config = readConfig();
if (!config.recordingApiUrl || !config.compositionServiceSecret) throw new Error("composition requires RECORDING_API_INTERNAL_URL and RECORDING_COMPOSITION_SERVICE_SECRET");
const runner = new ChildProcessRunner();
const root = join(config.storageRoot, "composition");

async function request<T>(path: string, body: unknown, lease?: string): Promise<T> {
  const response = await fetch(`${config.recordingApiUrl}${path}`, { method: "POST", headers: { authorization: `Bearer ${config.compositionServiceSecret}`, "content-type": "application/json", ...(lease ? { "x-composition-lease": lease } : {}) }, body: JSON.stringify(body) });
  if (!response.ok) throw new Error(`composition API failed: ${response.status}`);
  return response.json() as Promise<T>;
}
async function once(): Promise<void> {
  const claim = await request<{ job: { jobId: string; recordingId: string; leaseToken: string } | null }>("/internal/v1/composition-jobs/claim", {});
  if (!claim.job) return;
  const { jobId, leaseToken } = claim.job;
  const directory = join(root, jobId);
  const heartbeat = setInterval(() => void request(`/internal/v1/composition-jobs/${jobId}/heartbeat`, {}, leaseToken).catch(() => undefined), 30_000);
  try {
    await mkdir(directory, { recursive: true, mode: 0o750 });
    const manifest = await request<{ segments: Array<{ generation: number; sequence: number; timelineStartMs: number; timelineEndMs: number; sha256: string; download: { url: string } }> }>(`/internal/v1/composition-jobs/${jobId}/segments`, {}, leaseToken);
    if (!manifest.segments.length) throw new Error("composition source is empty");
    const files: string[] = [];
    for (const segment of manifest.segments) {
      const response = await fetch(segment.download.url);
      if (!response.ok) throw new Error(`segment download failed: ${response.status}`);
      const bytes = Buffer.from(await response.arrayBuffer());
      if (createHash("sha256").update(bytes).digest("hex") !== segment.sha256) throw new Error("segment checksum mismatch");
      const path = join(directory, `${segment.generation}-${segment.sequence}.webm`);
      await writeFile(path, bytes, { mode: 0o640 }); files.push(path);
    }
    const concat = join(directory, "input.txt"); const output = join(directory, "output.mp4");
    await writeFile(concat, files.map((file) => `file '${file.replaceAll("'", "'\\''")}'`).join("\n"), { mode: 0o640 });
    await runner.run(config.ffmpegPath ?? ffmpeg.path, ["-hide_banner", "-loglevel", "error", "-y", "-f", "concat", "-safe", "0", "-i", concat, "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,fps=24", "-c:v", "libx264", "-preset", "fast", "-crf", "18", "-maxrate", "8M", "-bufsize", "16M", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", output], config.ffmpegTimeoutMs);
    const probe = await runner.run(config.ffprobePath ?? ffprobe.path, ["-v", "error", "-show_streams", "-show_format", "-of", "json", output], config.ffmpegTimeoutMs);
    const metadata = verifyProbe(probe.stdout);
    const bytes = await readFile(output); const sha256 = createHash("sha256").update(bytes).digest("hex");
    const meta = { bytes: bytes.length, sha256, ...metadata };
    const target = await request<{ uploadUrl: string }>(`/internal/v1/composition-jobs/${jobId}/output-upload`, meta, leaseToken);
    const put = await fetch(target.uploadUrl, { method: "PUT", headers: { "content-type": "video/mp4", "content-length": String(bytes.length) }, body: bytes });
    if (!put.ok) {
      const detail = (await put.text()).replace(/\s+/g, " ").slice(0, 512);
      throw new Error(`artifact upload failed: ${put.status}${detail ? ` ${detail}` : ""}`);
    }
    await request(`/internal/v1/composition-jobs/${jobId}/complete`, meta, leaseToken);
  } catch (error) { await request(`/internal/v1/composition-jobs/${jobId}/fail`, { code: "COMPOSITION_FAILED", message: String(error).replace(/[\r\n\t]+/g, " ").slice(0, 512) }, leaseToken).catch(() => undefined); console.error(error); }
  finally { clearInterval(heartbeat); await rm(directory, { recursive: true, force: true }); }
}
function verifyProbe(raw: string): { durationSeconds: number; width: number; height: number; frameRate: number } {
  const parsed = JSON.parse(raw) as { streams?: Array<{ codec_type?: string; codec_name?: string; width?: number; height?: number; avg_frame_rate?: string }>; format?: { duration?: string } };
  const video = parsed.streams?.find((stream) => stream.codec_type === "video");
  const audio = parsed.streams?.find((stream) => stream.codec_type === "audio");
  const durationSeconds = Number(parsed.format?.duration); const [n, d] = (video?.avg_frame_rate ?? "0/1").split("/").map(Number); const frameRate = n! / d!;
  if (!video || !audio || video.codec_name !== "h264" || audio.codec_name !== "aac" || video.width !== 1280 || video.height !== 720 || !Number.isFinite(durationSeconds) || durationSeconds <= 0 || !Number.isFinite(frameRate) || Math.abs(frameRate - 24) > 0.1) throw new Error("final artifact verification failed");
  return { durationSeconds, width: video.width, height: video.height, frameRate };
}
void (async () => { for (;;) { await once(); await new Promise((resolve) => setTimeout(resolve, 2_000)); } })();
