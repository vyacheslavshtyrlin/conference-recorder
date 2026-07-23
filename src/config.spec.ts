import { describe, expect, it } from "vitest";

import { readConfig } from "./config.js";

describe("readConfig", () => {
  it("exposes an explicitly configured stable worker id and slot capacity", () => {
    const config = readConfig({
      RECORDING_BOT_CONTROL_JWT_HS256_SECRET: "x".repeat(32),
      RECORDING_BOT_WORKER_ID: "recording-worker-a",
      RECORDING_BOT_MAX_SLOTS: "3",
    });
    expect(config.workerId).toBe("recording-worker-a");
    expect(config.maxSlots).toBe(3);
  });
});
