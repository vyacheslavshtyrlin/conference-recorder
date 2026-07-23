import { z } from "zod";

export const protocolVersion = "recording-bot.v1" as const;

export const attemptRequestSchema = z.object({
  attemptId: z.string().min(1).max(128),
  roomId: z.string().min(1).max(128),
  recordingId: z.string().min(1).max(128),
  integrationId: z.string().min(1).max(128),
  requestedAt: z.string().datetime(),
  protocolVersion: z.literal(protocolVersion),
  signalingUrl: z.string().url().refine((value) => value.startsWith("ws://") || value.startsWith("wss://")),
  admission: z.string().min(1).max(8192),
  workerId: z.string().min(1).max(128),
  workerGenerationId: z.string().min(1).max(128),
}).strict();

export type AttemptRequest = z.infer<typeof attemptRequestSchema>;
export const workerFenceSchema = z
  .object({
    workerId: z.string().min(1).max(128),
    workerGenerationId: z.string().min(1).max(128),
  })
  .strict();
export const finalizationRequestSchema = z
  .object({
    attemptId: z.string().min(1).max(128),
    roomId: z.string().min(1).max(128),
    recordingId: z.string().min(1).max(128),
    integrationId: z.string().min(1).max(128),
    requestedAt: z.string().datetime(),
    workerId: z.string().min(1).max(128),
    workerGenerationId: z.string().min(1).max(128),
  })
  .strict();
export type AttemptState =
  | "starting"
  | "capturing"
  | "stopping"
  | "finalizing"
  | "ready"
  | "failed";

export const runtimeEventSchema = z.discriminatedUnion("type", [
  z.object({ type: z.literal("runtime:ready"), producerCount: z.number().int().min(0) }).strict(),
  z.object({ type: z.literal("runtime:failed"), code: z.string().min(1).max(64), message: z.string().min(1).max(1024) }).strict(),
  z.object({ type: z.literal("runtime:stopped") }).strict(),
]);

export type RuntimeEvent = z.infer<typeof runtimeEventSchema>;
