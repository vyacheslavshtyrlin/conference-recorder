import { createReadStream } from "node:fs";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { fileURLToPath } from "node:url";
import { ZodError } from "zod";

import {
  AttemptConflictError,
  AttemptManager,
  CapacityError,
} from "./attemptManager.js";
import { verifyControlToken } from "./auth.js";
import type { RecordingBotConfig } from "./config.js";
import {
  attemptRequestSchema,
  finalizationRequestSchema,
  runtimeEventSchema,
} from "./contracts.js";

const browserBundlePath = fileURLToPath(
  new URL("./public/browser.js", import.meta.url),
);
const publisherBundlePath = fileURLToPath(
  new URL("./public/e2ePublisher.js", import.meta.url),
);

export function createRecordingBotServer(
  manager: AttemptManager,
  config: RecordingBotConfig,
  identity: { generationId: string },
) {
  return createServer(async (request, response) => {
    try {
      await route(request, response, manager, config, identity);
    } catch (error) {
      const status =
        error instanceof AttemptConflictError
          ? 409
          : error instanceof CapacityError
            ? 503
            : error instanceof ZodError || error instanceof SyntaxError
              ? 400
              : 500;
      console.error(
        JSON.stringify({
          timestamp: new Date().toISOString(),
          severity: "error",
          service: "recording-bot",
          operation: `${request.method ?? "UNKNOWN"} ${request.url ?? "/"}`,
          error: error instanceof Error ? error.message : String(error),
        }),
      );
      sendJson(response, status, {
        error:
          status === 500
            ? "INTERNAL_ERROR"
            : status === 400
              ? "INVALID_PAYLOAD"
              : errorCode(error as Error),
      });
    }
  });
}

async function route(
  request: IncomingMessage,
  response: ServerResponse,
  manager: AttemptManager,
  config: RecordingBotConfig,
  identity: { generationId: string },
): Promise<void> {
  const url = new URL(request.url ?? "/", "http://recording-bot.local");
  if (request.method === "GET" && url.pathname === "/health/live")
    return sendJson(response, 200, { status: "ok" });
  if (request.method === "GET" && url.pathname === "/health/ready")
    return sendJson(response, 200, {
      status: "ready",
      workerId: config.workerId,
      generationId: identity.generationId,
      capacity: config.maxSlots,
      occupiedSlots: manager.occupiedSlots(),
      attempts: manager.snapshot(),
      recoverableRecordingIds: manager
        .snapshot()
        .filter((attempt) => attempt.recoverable)
        .map((attempt) => attempt.recordingId),
      pendingFinalizationRecordingIds: await manager.pendingFinalizations(),
    });
  if (request.method === "GET" && url.pathname === "/assets/browser.js") {
    response.writeHead(200, {
      "content-type": "text/javascript; charset=utf-8",
      "cache-control": "no-store",
    });
    createReadStream(browserBundlePath).pipe(response);
    return;
  }
  if (
    config.enableVerification &&
    request.method === "GET" &&
    url.pathname === "/assets/e2ePublisher.js"
  ) {
    response.writeHead(200, {
      "content-type": "text/javascript; charset=utf-8",
      "cache-control": "no-store",
    });
    createReadStream(publisherBundlePath).pipe(response);
    return;
  }
  if (
    config.enableVerification &&
    request.method === "GET" &&
    url.pathname === "/verification/publisher"
  ) {
    response.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      "content-security-policy":
        "default-src 'self'; connect-src 'self' ws: wss:; script-src 'self'",
    });
    response.end(
      '<!doctype html><html><body><script type="module" src="/assets/e2ePublisher.js"></script></body></html>',
    );
    return;
  }
  const runtimeMatch = /^\/runtime\/([^/]+)$/.exec(url.pathname);
  if (request.method === "GET" && runtimeMatch) {
    const attemptId = decodeURIComponent(runtimeMatch[1]!);
    const token = url.searchParams.get("token") ?? "";
    if (!manager.getRuntimeConfig(attemptId, token))
      return sendJson(response, 404, { error: "NOT_FOUND" });
    response.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      "content-security-policy":
        "default-src 'self'; connect-src 'self' ws: wss:; script-src 'self'",
    });
    response.end(
      '<!doctype html><html><body><canvas id="stage" width="1280" height="720"></canvas><script type="module" src="/assets/browser.js"></script></body></html>',
    );
    return;
  }
  if (request.method === "GET" && url.pathname === "/internal/runtime-config") {
    const attemptId = url.searchParams.get("attemptId") ?? "";
    const token = bearer(request) ?? "";
    const runtime = manager.getRuntimeConfig(attemptId, token);
    return runtime
      ? sendJson(response, 200, runtime)
      : sendJson(response, 404, { error: "NOT_FOUND" });
  }
  if (
    request.method === "POST" &&
    url.pathname === "/internal/runtime-events"
  ) {
    const body = await readJson(request);
    const event = runtimeEventSchema.parse(body.event);
    const accepted = manager.receiveRuntimeEvent(
      String(body.attemptId ?? ""),
      bearer(request) ?? "",
      event,
    );
    return sendJson(response, accepted ? 202 : 404, { accepted });
  }
  if (
    request.method === "POST" &&
    url.pathname === "/internal/runtime-chunks"
  ) {
    const attemptId = url.searchParams.get("attemptId") ?? "";
    const accepted = await manager.appendRuntimeChunk(
      attemptId,
      bearer(request) ?? "",
      await readBuffer(request, 8 * 1024 * 1024),
    );
    return sendJson(response, accepted ? 202 : 404, { accepted });
  }
  const controlMatch = /^\/internal\/v1\/attempts\/([^/]+)$/.exec(url.pathname);
  if (
    controlMatch &&
    (request.method === "PUT" || request.method === "DELETE")
  ) {
    try {
      verifyControlToken(request.headers.authorization, config.controlJwt);
    } catch {
      return sendJson(response, 401, { error: "UNAUTHORIZED" });
    }
    if (!matchesFence(request, config, identity))
      return sendJson(response, 409, { error: "WORKER_FENCE_MISMATCH" });
    const recordingId = decodeURIComponent(controlMatch[1]!);
    if (request.method === "DELETE")
      return sendJson(response, 200, await manager.stop(recordingId));
    const parsed = attemptRequestSchema.parse(await readJson(request));
    if (
      parsed.workerId !== config.workerId ||
      parsed.workerGenerationId !== identity.generationId
    )
      return sendJson(response, 409, { error: "WORKER_FENCE_MISMATCH" });
    if (parsed.recordingId !== recordingId)
      return sendJson(response, 400, { error: "INVALID_PAYLOAD" });
    return sendJson(response, 202, await manager.create(parsed));
  }
  const finalizationMatch =
    /^\/internal\/v1\/finalizations\/([^/]+)$/.exec(url.pathname);
  if (finalizationMatch && request.method === "PUT") {
    try {
      verifyControlToken(request.headers.authorization, config.controlJwt);
    } catch {
      return sendJson(response, 401, { error: "UNAUTHORIZED" });
    }
    if (!matchesFence(request, config, identity))
      return sendJson(response, 409, { error: "WORKER_FENCE_MISMATCH" });
    const finalization = finalizationRequestSchema.parse(
      await readJson(request),
    );
    if (
      finalization.workerId !== config.workerId ||
      finalization.workerGenerationId !== identity.generationId
    )
      return sendJson(response, 409, { error: "WORKER_FENCE_MISMATCH" });
    const recordingId = decodeURIComponent(finalizationMatch[1]!);
    if (finalization.recordingId !== recordingId)
      return sendJson(response, 400, { error: "INVALID_PAYLOAD" });
    return sendJson(
      response,
      200,
      await manager.recoverFinalization(recordingId),
    );
  }
  sendJson(response, 404, { error: "NOT_FOUND" });
}

function matchesFence(
  request: IncomingMessage,
  config: RecordingBotConfig,
  identity: { generationId: string },
): boolean {
  return (
    request.headers["x-recording-worker-id"] === config.workerId &&
    request.headers["x-recording-worker-generation-id"] ===
      identity.generationId
  );
}

function bearer(request: IncomingMessage): string | undefined {
  return request.headers.authorization?.startsWith("Bearer ")
    ? request.headers.authorization.slice(7)
    : undefined;
}

async function readJson(
  request: IncomingMessage,
): Promise<Record<string, unknown>> {
  return JSON.parse(
    (await readBuffer(request, 64 * 1024)).toString("utf8"),
  ) as Record<string, unknown>;
}

async function readBuffer(
  request: IncomingMessage,
  limit: number,
): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const buffer = Buffer.from(chunk);
    bytes += buffer.length;
    if (bytes > limit) throw new Error("request too large");
    chunks.push(buffer);
  }
  return Buffer.concat(chunks);
}

function sendJson(
  response: ServerResponse,
  status: number,
  body: unknown,
): void {
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  response.end(JSON.stringify(body));
}

function errorCode(error: Error): string {
  return error instanceof CapacityError
    ? "BOT_CAPACITY_EXHAUSTED"
    : "ATTEMPT_CONFLICT";
}
