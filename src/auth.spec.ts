import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";

import { verifyControlToken } from "./auth.js";

const config = { issuer: "conference-signaling", audience: "conference-recording-bot-control", secret: "a-secret-that-is-at-least-thirty-two-bytes", maxTtlSeconds: 300 };

describe("verifyControlToken", () => {
  it("accepts the dedicated short-lived control token", () => {
    expect(() => verifyControlToken(`Bearer ${sign({ iat: 1000, exp: 1060 })}`, config, () => 1_010_000)).not.toThrow();
  });

  it.each([
    { iat: 1000, exp: 1000 },
    { iat: 1000, exp: 1400 },
    { iat: 1020, exp: 1030 },
  ])("rejects invalid time bounds %#", (times) => {
    expect(() => verifyControlToken(`Bearer ${sign(times)}`, config, () => 1_010_000)).toThrow("invalid authorization");
  });

  it("rejects a token signed for another audience", () => {
    expect(() => verifyControlToken(`Bearer ${sign({ iat: 1000, exp: 1060, aud: "wrong" })}`, config, () => 1_010_000)).toThrow("invalid authorization");
  });
});

function sign(input: { iat: number; exp: number; aud?: string }): string {
  const header = encode({ alg: "HS256", typ: "recording-bot-control+jwt" });
  const payload = encode({ iss: config.issuer, aud: input.aud ?? config.audience, sub: "conference-signaling", iat: input.iat, exp: input.exp });
  const signature = createHmac("sha256", config.secret).update(`${header}.${payload}`).digest("base64url");
  return `${header}.${payload}.${signature}`;
}

function encode(value: unknown): string { return Buffer.from(JSON.stringify(value)).toString("base64url"); }
