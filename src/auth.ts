import { createHmac, timingSafeEqual } from "node:crypto";

type JwtConfig = {
  issuer: string;
  audience: string;
  secret: string;
  maxTtlSeconds: number;
};

export function verifyControlToken(
  header: string | undefined,
  config: JwtConfig,
  now = Date.now,
): void {
  const token = header?.startsWith("Bearer ") ? header.slice(7) : "";
  const parts = token.split(".");
  if (parts.length !== 3) throw new Error("invalid authorization");
  const [encodedHeader, encodedPayload, signature] = parts as [
    string,
    string,
    string,
  ];
  const expected = createHmac("sha256", config.secret)
    .update(`${encodedHeader}.${encodedPayload}`)
    .digest();
  const actual = decodeBase64Url(signature);
  if (actual.length !== expected.length || !timingSafeEqual(actual, expected))
    throw new Error("invalid authorization");
  const jwtHeader = parseJson(encodedHeader) as Record<string, unknown>;
  const payload = parseJson(encodedPayload) as Record<string, unknown>;
  const current = Math.floor(now() / 1000);
  if (
    jwtHeader.alg !== "HS256" ||
    jwtHeader.typ !== "recording-bot-control+jwt"
  )
    throw new Error("invalid authorization");
  if (
    payload.iss !== config.issuer ||
    payload.aud !== config.audience ||
    payload.sub !== "conference-signaling"
  )
    throw new Error("invalid authorization");
  if (
    typeof payload.iat !== "number" ||
    typeof payload.exp !== "number" ||
    payload.iat > current + 5 ||
    payload.exp <= current ||
    payload.exp <= payload.iat ||
    payload.exp - payload.iat > config.maxTtlSeconds
  )
    throw new Error("invalid authorization");
}

function parseJson(value: string): unknown {
  return JSON.parse(decodeBase64Url(value).toString("utf8"));
}

function decodeBase64Url(value: string): Buffer {
  return Buffer.from(value.replace(/-/g, "+").replace(/_/g, "/"), "base64");
}
