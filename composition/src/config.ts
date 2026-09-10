import { z } from "zod";

const insecureSecretMarkerPattern =
  /(^|[^a-z0-9])(change[-_]me|example|local|replace[-_]with|placeholder)($|[^a-z0-9])/i;

const schema = z.object({
  RECORDING_RUNTIME_ENV: z.enum(["development", "production"]).default("production"),
  RECORDING_STORAGE_ROOT: z.string().min(1).default("/var/lib/conference-recording"),
  RECORDING_API_INTERNAL_URL: z.string().url(),
  RECORDING_COMPOSITION_SERVICE_SECRET: z.string().min(32),
  RECORDING_FFMPEG_PATH: z.string().min(1).default("ffmpeg"),
  RECORDING_FFPROBE_PATH: z.string().min(1).default("ffprobe"),
  RECORDING_COMPOSITION_FFMPEG_TIMEOUT_MS: z.coerce.number().int().min(1_000).max(86_400_000).default(21_600_000),
  RECORDING_COMPOSITION_HTTP_TIMEOUT_MS: z.coerce.number().int().min(1_000).max(600_000).default(120_000),
  RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS: z.coerce.number().int().min(1).max(3_600),
  RECORDING_FINALIZE_MAX_ATTEMPTS: z.coerce.number().int().min(1).max(10).default(3),
  RECORDING_FINALIZE_RETRY_DELAY_MS: z.coerce.number().int().min(0).max(30_000).default(250),
  RECORDING_MAX_SOURCE_BYTES: z.coerce.number().int().min(1_000_000).max(1_000_000_000_000).default(4_000_000_000),
  RECORDING_MAX_DURATION_SECONDS: z.coerce.number().int().min(1).max(86_400).default(14_400),
}).superRefine((value, context) => {
  if (value.RECORDING_RUNTIME_ENV === "production" &&
    insecureSecretMarkerPattern.test(value.RECORDING_COMPOSITION_SERVICE_SECRET)) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      message: "must not use a placeholder value",
      path: ["RECORDING_COMPOSITION_SERVICE_SECRET"],
    });
  }
});

export function readConfig(environment: NodeJS.ProcessEnv = process.env) {
  const value = schema.parse(environment);
  const recordingApiUrl = new URL(value.RECORDING_API_INTERNAL_URL);
  if (!["http:", "https:"].includes(recordingApiUrl.protocol) || recordingApiUrl.username ||
    recordingApiUrl.password || recordingApiUrl.search || recordingApiUrl.hash) {
    throw new Error("RECORDING_API_INTERNAL_URL must be a credential-free HTTP(S) base URL");
  }
  return {
    storageRoot: value.RECORDING_STORAGE_ROOT,
    recordingApiUrl: recordingApiUrl.toString().replace(/\/$/, ""),
    compositionServiceSecret: value.RECORDING_COMPOSITION_SERVICE_SECRET,
    ffmpegPath: value.RECORDING_FFMPEG_PATH,
    ffprobePath: value.RECORDING_FFPROBE_PATH,
    ffmpegTimeoutMs: value.RECORDING_COMPOSITION_FFMPEG_TIMEOUT_MS,
    httpTimeoutMs: value.RECORDING_COMPOSITION_HTTP_TIMEOUT_MS,
    artifactUploadTimeoutMs: value.RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS * 1_000,
    finalizeMaxAttempts: value.RECORDING_FINALIZE_MAX_ATTEMPTS,
    finalizeRetryDelayMs: value.RECORDING_FINALIZE_RETRY_DELAY_MS,
    maxSourceBytes: value.RECORDING_MAX_SOURCE_BYTES,
    maxDurationMs: value.RECORDING_MAX_DURATION_SECONDS * 1_000,
  };
}
