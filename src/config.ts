import { z } from "zod";
import { hostname } from "node:os";

const schema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  RECORDING_BOT_BIND_HOST: z.string().default("0.0.0.0"),
  RECORDING_BOT_PORT: z.coerce.number().int().min(1).max(65535).default(8090),
  RECORDING_BOT_PUBLIC_ORIGIN: z.string().url().default("http://127.0.0.1:8090"),
  RECORDING_BOT_MAX_SLOTS: z.coerce.number().int().min(1).max(32).default(1),
  RECORDING_BOT_WORKER_ID: z.string().min(1).max(128).default(hostname()),
  RECORDING_BOT_ENABLE_VERIFICATION: z.enum(["true", "false"]).default("false").transform((value) => value === "true"),
  RECORDING_BOT_START_TIMEOUT_MS: z.coerce.number().int().min(1000).max(120000).default(30000),
  RECORDING_BOT_STOP_TIMEOUT_MS: z.coerce.number().int().min(1000).max(30000).default(5000),
  RECORDING_BOT_RECOVERY_TIMEOUT_MS: z.coerce.number().int().min(1000).max(300000).default(30000),
  RECORDING_STORAGE_ROOT: z.string().min(1).default("/var/lib/conference-recording"),
  RECORDING_API_INTERNAL_URL: z.string().url().optional(),
  RECORDING_CAPTURE_SERVICE_SECRET: z.string().min(32).optional(),
  RECORDING_COMPOSITION_SERVICE_SECRET: z.string().min(32).optional(),
  RECORDING_FFMPEG_PATH: z.string().min(1).optional(),
  RECORDING_FFPROBE_PATH: z.string().min(1).optional(),
  RECORDING_FFMPEG_TIMEOUT_MS: z.coerce.number().int().min(1000).max(600000).default(120000),
  RECORDING_FINALIZE_MAX_ATTEMPTS: z.coerce.number().int().min(1).max(10).default(3),
  RECORDING_FINALIZE_RETRY_DELAY_MS: z.coerce.number().int().min(0).max(30_000).default(250),
  RECORDING_MAX_SOURCE_BYTES: z.coerce.number().int().min(1_000_000).default(4_000_000_000),
  RECORDING_MAX_DURATION_SECONDS: z.coerce.number().int().min(1).max(86_400).default(14_400),
  RECORDING_BOT_CONTROL_JWT_HS256_SECRET: z.string().min(32),
  CHROMIUM_EXECUTABLE_PATH: z.string().min(1).optional(),
});

export type RecordingBotConfig = ReturnType<typeof readConfig>;

export function readConfig(environment: NodeJS.ProcessEnv = process.env) {
  const value = schema.parse(environment);
  return {
    nodeEnv: value.NODE_ENV,
    host: value.RECORDING_BOT_BIND_HOST,
    port: value.RECORDING_BOT_PORT,
    publicOrigin: value.RECORDING_BOT_PUBLIC_ORIGIN.replace(/\/$/, ""),
    maxSlots: value.RECORDING_BOT_MAX_SLOTS,
    workerId: value.RECORDING_BOT_WORKER_ID,
    enableVerification: value.RECORDING_BOT_ENABLE_VERIFICATION,
    startTimeoutMs: value.RECORDING_BOT_START_TIMEOUT_MS,
    stopTimeoutMs: value.RECORDING_BOT_STOP_TIMEOUT_MS,
    recoveryTimeoutMs: value.RECORDING_BOT_RECOVERY_TIMEOUT_MS,
    storageRoot: value.RECORDING_STORAGE_ROOT,
    ...(value.RECORDING_API_INTERNAL_URL ? { recordingApiUrl: value.RECORDING_API_INTERNAL_URL.replace(/\/$/, "") } : {}),
    ...(value.RECORDING_CAPTURE_SERVICE_SECRET ? { captureServiceSecret: value.RECORDING_CAPTURE_SERVICE_SECRET } : {}),
    ...(value.RECORDING_COMPOSITION_SERVICE_SECRET ? { compositionServiceSecret: value.RECORDING_COMPOSITION_SERVICE_SECRET } : {}),
    ...(value.RECORDING_FFMPEG_PATH ? { ffmpegPath: value.RECORDING_FFMPEG_PATH } : {}),
    ...(value.RECORDING_FFPROBE_PATH ? { ffprobePath: value.RECORDING_FFPROBE_PATH } : {}),
    ffmpegTimeoutMs: value.RECORDING_FFMPEG_TIMEOUT_MS,
    finalizeMaxAttempts: value.RECORDING_FINALIZE_MAX_ATTEMPTS,
    finalizeRetryDelayMs: value.RECORDING_FINALIZE_RETRY_DELAY_MS,
    maxSourceBytes: value.RECORDING_MAX_SOURCE_BYTES,
    maxDurationMs: value.RECORDING_MAX_DURATION_SECONDS * 1_000,
    ...(value.CHROMIUM_EXECUTABLE_PATH ? { chromiumExecutablePath: value.CHROMIUM_EXECUTABLE_PATH } : {}),
    controlJwt: {
      issuer: "conference-signaling",
      audience: "conference-recording-bot-control",
      secret: value.RECORDING_BOT_CONTROL_JWT_HS256_SECRET,
      maxTtlSeconds: 300,
    },
  };
}
