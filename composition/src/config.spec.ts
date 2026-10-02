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

  it.each([
    "change-me-recording-composition-secret-32-bytes",
    "example-recording-composition-secret-32-bytes",
    "local-recording-composition-secret-32-bytes",
    "replace-with-at-least-32-random-characters",
    "composition-placeholder-secret-at-least-32-bytes",
  ])("rejects placeholder composition secret %s", (secret) => {
    expect(() => readConfig({
      ...valid,
      RECORDING_COMPOSITION_SERVICE_SECRET: secret,
    })).toThrow("must not use a placeholder value");
  });

  it("allows a random secret that merely contains example", () => {
    expect(readConfig({
      ...valid,
      RECORDING_COMPOSITION_SERVICE_SECRET: "randomexamplevalue-with-at-least-32-bytes",
    }).compositionServiceSecret).toBe("randomexamplevalue-with-at-least-32-bytes");
  });

  it("allows the root-local change-me secret in development", () => {
    expect(readConfig({
      ...valid,
      RECORDING_RUNTIME_ENV: "development",
      RECORDING_COMPOSITION_SERVICE_SECRET: "change-me-recording-composition-secret-32-bytes",
    }).compositionServiceSecret).toBe("change-me-recording-composition-secret-32-bytes");
  });

  it("keeps structural secret validation enabled in development", () => {
    expect(() => readConfig({
      ...valid,
      RECORDING_RUNTIME_ENV: "development",
      RECORDING_COMPOSITION_SERVICE_SECRET: "short",
    })).toThrow();
  });

  it("defaults to production security independently of NODE_ENV", () => {
    expect(() => readConfig({
      ...valid,
      NODE_ENV: "development",
      RECORDING_COMPOSITION_SERVICE_SECRET: "change-me-recording-composition-secret-32-bytes",
    })).toThrow("must not use a placeholder value");
  });

  it("rejects an invalid recording runtime environment", () => {
    expect(() => readConfig({
      ...valid,
      RECORDING_RUNTIME_ENV: "staging",
    })).toThrow();
  });
});
