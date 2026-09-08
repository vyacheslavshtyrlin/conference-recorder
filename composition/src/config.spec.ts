import { describe, expect, it } from "vitest";
import { readConfig } from "./config.js";

const valid = {
  RECORDING_API_INTERNAL_URL: "http://api:3000",
  RECORDING_COMPOSITION_SERVICE_SECRET: "composition-service-secret-at-least-32-bytes",
  RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS: "300",
};

describe("composition config", () => {
  it("preserves bounded existing RECORDING variables", () => {
    expect(readConfig({
      ...valid,
      RECORDING_FFMPEG_PATH: "/usr/bin/ffmpeg",
      RECORDING_FFPROBE_PATH: "/usr/bin/ffprobe",
      RECORDING_COMPOSITION_FFMPEG_TIMEOUT_MS: "600000",
      RECORDING_COMPOSITION_HTTP_TIMEOUT_MS: "300000",
      RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS: "600",
      RECORDING_FINALIZE_MAX_ATTEMPTS: "10",
      RECORDING_FINALIZE_RETRY_DELAY_MS: "30000",
      RECORDING_MAX_SOURCE_BYTES: "1000000",
      RECORDING_MAX_DURATION_SECONDS: "86400",
    })).toMatchObject({
      recordingApiUrl: "http://api:3000",
      ffmpegPath: "/usr/bin/ffmpeg",
      ffprobePath: "/usr/bin/ffprobe",
      ffmpegTimeoutMs: 600000,
      httpTimeoutMs: 300000,
      artifactUploadTimeoutMs: 600000,
      finalizeMaxAttempts: 10,
      finalizeRetryDelayMs: 30000,
      maxSourceBytes: 1000000,
      maxDurationMs: 86400000,
    });
  });

  it("requires the API URL and composition-only secret", () => {
    expect(() => readConfig({})).toThrow();
    expect(() => readConfig({ ...valid, RECORDING_COMPOSITION_SERVICE_SECRET: "short" })).toThrow();
  });
});
