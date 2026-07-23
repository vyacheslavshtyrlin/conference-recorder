import { once } from "node:events";
import { createHmac } from "node:crypto";

import { afterEach, describe, expect, it, vi } from "vitest";

import type { AttemptManager } from "./attemptManager.js";
import { readConfig } from "./config.js";
import { createRecordingBotServer } from "./httpServer.js";

const servers: ReturnType<typeof createRecordingBotServer>[] = [];

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => server.close(() => resolve())),
    ),
  );
});

describe("recording bot readiness", () => {
  it("publishes worker generation and current slot load", async () => {
    const manager = {
      occupiedSlots: vi.fn().mockReturnValue(2),
      snapshot: vi.fn().mockReturnValue([{ recordingId: "rec-1" }]),
      pendingFinalizations: vi.fn().mockResolvedValue(["rec-finalize"]),
    } as unknown as AttemptManager;
    const config = readConfig({
      RECORDING_BOT_CONTROL_JWT_HS256_SECRET: "x".repeat(32),
      RECORDING_BOT_WORKER_ID: "worker-a",
      RECORDING_BOT_MAX_SLOTS: "4",
    });
    const server = createRecordingBotServer(manager, config, {
      generationId: "generation-1",
    });
    servers.push(server);
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");

    const response = await fetch(
      `http://127.0.0.1:${address.port}/health/ready`,
    );
    await expect(response.json()).resolves.toEqual({
      status: "ready",
      workerId: "worker-a",
      generationId: "generation-1",
      capacity: 4,
      occupiedSlots: 2,
      attempts: [{ recordingId: "rec-1" }],
      recoverableRecordingIds: [],
      pendingFinalizationRecordingIds: ["rec-finalize"],
    });
  });

  it("fences attempt mutation and restart finalization to the local worker generation", async () => {
    const manager = {
      create: vi.fn().mockResolvedValue({ state: "starting" }),
      stop: vi.fn().mockResolvedValue({ state: "stopped" }),
      recoverFinalization: vi.fn().mockResolvedValue({ path: "final/rec.mp4" }),
      occupiedSlots: vi.fn().mockReturnValue(0),
      snapshot: vi.fn().mockReturnValue([]),
      pendingFinalizations: vi.fn().mockResolvedValue([]),
    } as unknown as AttemptManager;
    const secret = "a-secret-that-is-at-least-thirty-two-bytes";
    const config = readConfig({
      RECORDING_BOT_CONTROL_JWT_HS256_SECRET: secret,
      RECORDING_BOT_WORKER_ID: "worker-a",
    });
    const server = createRecordingBotServer(manager, config, {
      generationId: "generation-1",
    });
    servers.push(server);
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const origin = `http://127.0.0.1:${address.port}`;
    const headers = {
      authorization: `Bearer ${token(secret)}`,
      "content-type": "application/json",
      "x-recording-worker-id": "worker-a",
      "x-recording-worker-generation-id": "generation-1",
    };
    const body = {
      attemptId: "attempt",
      roomId: "room",
      recordingId: "rec",
      integrationId: "integration",
      requestedAt: new Date().toISOString(),
      protocolVersion: "recording-bot.v1",
      signalingUrl: "ws://signaling:4000/ws",
      admission: "admission",
      workerId: "worker-a",
      workerGenerationId: "generation-1",
    };

    expect(
      (
        await fetch(`${origin}/internal/v1/attempts/rec`, {
          method: "PUT",
          headers,
          body: JSON.stringify(body),
        })
      ).status,
    ).toBe(202);
    expect(
      (
        await fetch(`${origin}/internal/v1/attempts/rec`, {
          method: "DELETE",
          headers: { authorization: headers.authorization },
        })
      ).status,
    ).toBe(409);
    expect(
      (
        await fetch(`${origin}/internal/v1/finalizations/rec`, {
          method: "PUT",
          headers,
          body: JSON.stringify({
            attemptId: "attempt",
            roomId: "room",
            recordingId: "rec",
            integrationId: "integration",
            requestedAt: new Date().toISOString(),
            workerId: "worker-a",
            workerGenerationId: "generation-1",
          }),
        })
      ).status,
    ).toBe(200);
    expect(manager.recoverFinalization).toHaveBeenCalledWith("rec");
    expect(
      (
        await fetch(`${origin}/internal/v1/finalizations/rec`, {
          method: "PUT",
          headers,
          body: JSON.stringify({
            attemptId: "attempt",
            roomId: "room",
            recordingId: "different-recording",
            integrationId: "integration",
            requestedAt: new Date().toISOString(),
            workerId: "worker-a",
            workerGenerationId: "generation-1",
          }),
        })
      ).status,
    ).toBe(400);
  });
});

function token(secret: string): string {
  const now = Math.floor(Date.now() / 1000);
  const header = Buffer.from(
    JSON.stringify({ alg: "HS256", typ: "recording-bot-control+jwt" }),
  ).toString("base64url");
  const payload = Buffer.from(
    JSON.stringify({
      iss: "conference-signaling",
      aud: "conference-recording-bot-control",
      sub: "conference-signaling",
      iat: now,
      exp: now + 60,
    }),
  ).toString("base64url");
  const signature = createHmac("sha256", secret)
    .update(`${header}.${payload}`)
    .digest("base64url");
  return `${header}.${payload}.${signature}`;
}
