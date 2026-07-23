import ffmpeg from "@ffmpeg-installer/ffmpeg";
import ffprobe from "@ffprobe-installer/ffprobe";
import { randomUUID } from "node:crypto";

import { AttemptManager } from "./attemptManager.js";
import { PlaywrightChromiumLauncher } from "./chromiumLauncher.js";
import { readConfig } from "./config.js";
import { createRecordingBotServer } from "./httpServer.js";
import { LocalRecordingSink } from "./recordingSink.js";
import { RemoteRecordingSink } from "./remoteRecordingSink.js";

const config = readConfig();
const identity = { generationId: randomUUID() };
const launcher = new PlaywrightChromiumLauncher(config.chromiumExecutablePath, config.stopTimeoutMs);
const sink = config.recordingApiUrl && config.captureServiceSecret ? new RemoteRecordingSink(config.storageRoot, config.recordingApiUrl, config.captureServiceSecret) : new LocalRecordingSink(
  config.storageRoot,
  config.ffmpegPath ?? ffmpeg.path,
  config.ffprobePath ?? ffprobe.path,
  config.ffmpegTimeoutMs,
  undefined,
  { maxSourceBytes: config.maxSourceBytes, maxDurationMs: config.maxDurationMs },
  { workerId: config.workerId, generationId: identity.generationId },
);
const manager = new AttemptManager(launcher, config, sink);
const server = createRecordingBotServer(manager, config, identity);

server.listen(config.port, config.host, () => {
  console.info(JSON.stringify({ level: "info", operation: "recordingBot.listen", host: config.host, port: config.port }));
});

let closing = false;
async function shutdown(signal: string): Promise<void> {
  if (closing) return;
  closing = true;
  console.info(JSON.stringify({ level: "info", operation: "recordingBot.shutdown", signal }));
  server.close();
  await manager.close();
}
process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));
