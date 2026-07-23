import { describe, expect, it } from "vitest";

import { RecordingAttempt } from "./recordingAttempt.js";

describe("RecordingAttempt", () => {
  it("owns the complete successful lifecycle", () => {
    const attempt = new RecordingAttempt();
    expect(attempt.state).toBe("starting");
    expect(attempt.transition({ type: "runtime-ready" })).toBe(true);
    expect(attempt.state).toBe("capturing");
    expect(attempt.transition({ type: "runtime-ready" })).toBe(false);
    expect(attempt.transition({ type: "stop-requested" })).toBe(true);
    expect(attempt.state).toBe("stopping");
    expect(attempt.transition({ type: "runtime-stopped" })).toBe(true);
    expect(attempt.state).toBe("finalizing");
    expect(attempt.transition({ type: "finalized" })).toBe(true);
    expect(attempt.state).toBe("ready");
    expect(attempt.transition({ type: "failed", code: "TOO_LATE" })).toBe(false);
  });

  it("makes failure terminal and records its authoritative code", () => {
    const attempt = new RecordingAttempt();
    expect(attempt.transition({ type: "failed", code: "BOT_RUNTIME_EXITED" })).toBe(true);
    expect(attempt.state).toBe("failed");
    expect(attempt.failureCode).toBe("BOT_RUNTIME_EXITED");
    expect(attempt.transition({ type: "stop-requested" })).toBe(false);
    expect(attempt.transition({ type: "runtime-ready" })).toBe(false);
  });

  it("rejects finalization transitions out of order", () => {
    const attempt = new RecordingAttempt();
    expect(attempt.transition({ type: "runtime-stopped" })).toBe(false);
    expect(attempt.transition({ type: "finalized" })).toBe(false);
    expect(attempt.state).toBe("starting");
  });
});
