import { describe, expect, it } from "vitest";

import { failureReport, terminalFailure } from "./compositionFailure.js";

describe("failureReport", () => {
  it("keeps the public failure safe while exposing a bounded single-line diagnostic", () => {
    const report = failureReport(terminalFailure(
      "RENDER_FAILED",
      "render",
      "Media rendering failed",
      new Error(`ffmpeg failed\n${"x".repeat(3_000)}`),
    ));

    expect(report.message).toBe("render: Media rendering failed");
    expect(report.diagnostic).toMatch(/^ffmpeg failed x+$/);
    expect(report.diagnostic).toHaveLength(2_048);
  });

  it("does not leak diagnostics for an unknown failure", () => {
    expect(failureReport(new Error("database password"))).toEqual({
      code: "COMPOSITION_FAILED",
      message: "composition: Composition failed",
      retryable: false,
    });
  });
});
