import { createHmac } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import ffmpeg from "@ffmpeg-installer/ffmpeg";
import ffprobe from "@ffprobe-installer/ffprobe";
import { chromium } from "playwright-core";

import { ChildProcessRunner } from "./recordingSink.js";

if (process.env.RECORDING_BOT_ENABLE_VERIFICATION !== "true") throw new Error("verification endpoint is disabled");
const api = process.env.LOCAL_E2E_API_URL ?? "http://api:3000";
const origin = process.env.RECORDING_BOT_PUBLIC_ORIGIN ?? "http://127.0.0.1:8090";
const storageRoot = process.env.RECORDING_STORAGE_ROOT ?? "/var/lib/conference-recording";
const externalRoomId = `recording-e2e-${Date.now()}`;
const integrationAuthorization = `Bearer ${serviceToken()}`;

const room = await json(`${api}/api/integrations/v1/rooms`, {
  method: "POST",
  headers: { authorization: integrationAuthorization, "content-type": "application/json" },
  body: JSON.stringify({ externalRoomId, title: "Recording E2E", recordingRequested: true, initialUser: { id: "e2e-user", name: "E2E Speaker" } }),
});
const joined = await joinFromEntry(String(room.entryUrl), room);
const screenAdmission = await json(`${api}/api/integrations/v1/rooms/${room.roomId}/admissions`, {
  method: "POST",
  headers: { authorization: integrationAuthorization, "content-type": "application/json" },
  body: JSON.stringify({
    identity: { kind: "authenticated", externalSubject: "recording-e2e-screen", displayName: "E2E Screen" },
    roomControls: "none",
    expiresAt: new Date(Date.now() + 5 * 60_000).toISOString(),
  }),
});
const screenJoined = await joinFromEntry(String(screenAdmission.entryUrl), room);

const browser = await chromium.launch({ headless: true, args: ["--no-sandbox", "--use-fake-ui-for-media-stream", "--use-fake-device-for-media-stream", "--autoplay-policy=no-user-gesture-required"] });
try {
  const cameraPage = await openPublisher(joined);
  const screenPage = await openPublisher(screenJoined);
  await waitForCapturing(String(room.recordingId));
  await cameraPage.waitForTimeout(2_000);
  await screenPage.evaluate(() => globalThis.__publisherStartScreen?.("#d32f2f"));
  await screenPage.waitForTimeout(3_000);
  await screenPage.evaluate(() => globalThis.__publisherReplaceScreen?.("#1976d2"));
  await screenPage.waitForTimeout(3_000);
  await screenPage.evaluate(() => globalThis.__publisherStopScreen?.());
  await screenPage.waitForTimeout(2_000);
  await screenPage.evaluate(() => globalThis.__publisherStop?.());
  await cameraPage.evaluate(() => globalThis.__publisherStop?.());
} finally {
  await browser.close();
}

const recordingId = String(room.recordingId);
const manifestPath = join(storageRoot, "recordings", recordingId, "manifest.json");
const deadline = Date.now() + 150_000;
let manifest: any;
while (Date.now() < deadline) {
  manifest = await readFile(manifestPath, "utf8").then(JSON.parse).catch(() => undefined);
  if (manifest?.status === "ready") break;
  if (manifest?.status === "failed") throw new Error(`recording failed: ${JSON.stringify(manifest.failure)}`);
  await new Promise((resolve) => setTimeout(resolve, 500));
}
if (manifest?.status !== "ready") throw new Error("timed out waiting for final MP4");
const artifactPath = join(storageRoot, manifest.artifact.path);
const probe = await new ChildProcessRunner().run(ffprobe.path, ["-v", "error", "-show_streams", "-show_format", "-of", "json", artifactPath], 30_000);
const probeJson = JSON.parse(probe.stdout);
const duration = Number(probeJson.format?.duration);
if (!Number.isFinite(duration) || duration < 9) throw new Error(`recording timeline is too short: ${String(duration)}`);
const redDominant = await readSignalStats(artifactPath, 3.5, "crop=900:500:50:50");
const redSideRail = await readSignalStats(artifactPath, 3.5, "crop=180:500:1070:50");
const blueDominant = await readSignalStats(artifactPath, 6.5, "crop=900:500:50:50");
const finalCamera = await readSignalStats(artifactPath, 9.2, "crop=500:500:50:50");
const finalAvatar = await readSignalStats(artifactPath, 9.2, "crop=500:500:730:50");
const screenOwnerLabel = await readSignalStats(artifactPath, 3.5, "crop=300:32:12:676");
const audioAnalysis = await new ChildProcessRunner().run(ffmpeg.path, ["-hide_banner", "-nostats", "-i", artifactPath, "-map", "0:a:0", "-af", "volumedetect", "-f", "null", "-"], 30_000);
const meanVolume = Number(/mean_volume:\s*(-?[\d.]+) dB/.exec(audioAnalysis.stderr ?? "")?.[1]);
if (!(redDominant.u < 130 && redDominant.v > 160)) throw new Error(`red screen interval was not dominant: ${JSON.stringify(redDominant)}`);
if (!(blueDominant.u > 150 && blueDominant.v < 110)) throw new Error(`replacement screen interval was not dominant: ${JSON.stringify(blueDominant)}`);
if (Math.abs(redDominant.u - redSideRail.u) + Math.abs(redDominant.v - redSideRail.v) < 50) throw new Error("screen-share side rail was replaced by dominant screen content");
if (Math.abs(finalCamera.u - finalAvatar.u) + Math.abs(finalCamera.v - finalAvatar.v) < 20) throw new Error("camera grid and avatar fallback were not restored after screen stop");
if (finalCamera.u < 130 && finalCamera.v > 160) throw new Error("red screen remained after screen stop");
if (finalCamera.u > 150 && finalCamera.v < 110) throw new Error("replacement screen remained after screen stop");
if (!(screenOwnerLabel.y < redDominant.y - 15)) throw new Error("screen owner label overlay was not detected");
if (!Number.isFinite(meanVolume) || meanVolume < -60) throw new Error(`recorded audio is silent: mean_volume=${String(meanVolume)}`);
console.log(JSON.stringify({ ok: true, roomId: room.roomId, recordingId, artifactPath, duration, meanVolume, layout: { redDominant, redSideRail, blueDominant, finalCamera, finalAvatar, screenOwnerLabel }, streams: probeJson.streams?.map((stream: any) => ({ type: stream.codec_type, codec: stream.codec_name, width: stream.width, height: stream.height, frameRate: stream.avg_frame_rate })) }));

async function json(url: string, init: RequestInit): Promise<any> {
  const response = await fetch(url, init);
  const value = await response.json().catch(() => undefined);
  if (!response.ok) throw new Error(`${init.method} ${url} failed: ${response.status} ${JSON.stringify(value)}`);
  return value;
}

async function joinFromEntry(entryUrl: string, room: any): Promise<any> {
  const entry = new URL(entryUrl);
  const code = new URLSearchParams(entry.hash.slice(1)).get("code");
  if (!code) throw new Error("integration admission did not return an entry code");
  const exchange = await json(`${api}/api/v1/admissions/exchange`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ code, roomId: room.roomId }) });
  return json(`${api}/api/v1/rooms/${room.slug}/join`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ displayName: exchange.displayName, roomSessionToken: exchange.roomSessionToken }) });
}

async function openPublisher(joined: any) {
  const page = await browser.newPage();
  const publisher = new URL("/verification/publisher", origin);
  publisher.searchParams.set("signalingUrl", String(joined.signalingUrl).replace("localhost", "127.0.0.1"));
  publisher.searchParams.set("token", String(joined.token));
  await page.goto(publisher.toString());
  await page.waitForFunction(() => globalThis.__publisherReady === true || Boolean(document.body.dataset.error), undefined, { timeout: 30_000 });
  const error = await page.evaluate(() => document.body.dataset.error);
  if (error) throw new Error(error);
  return page;
}

function serviceToken(): string {
  const now = Math.floor(Date.now() / 1000);
  const secret = process.env.LOCAL_INTEGRATION_SECRET ?? "postman-local-crm-alpha-test-secret-2026";
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const payload = Buffer.from(JSON.stringify({
    iss: "postman-local",
    aud: "conference-integrations",
    sub: "postman-local",
    scope: "rooms:create admissions:create",
    iat: now,
    exp: now + 300,
  })).toString("base64url");
  const signature = createHmac("sha256", secret).update(`${header}.${payload}`).digest("base64url");
  return `${header}.${payload}.${signature}`;
}

declare global {
  var __publisherReady: boolean | undefined;
  var __publisherStop: (() => Promise<void>) | undefined;
  var __publisherStartScreen: ((color: string) => Promise<void>) | undefined;
  var __publisherReplaceScreen: ((color: string) => Promise<void>) | undefined;
  var __publisherStopScreen: (() => Promise<void>) | undefined;
}

async function waitForCapturing(recordingId: string): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const response = await fetch(`${origin}/health/ready`);
    const status = await response.json() as { attempts?: Array<{ recordingId: string; state: string; failureCode?: string }> };
    const attempt = status.attempts?.find((candidate) => candidate.recordingId === recordingId);
    if (attempt?.state === "capturing") return;
    if (attempt?.state === "failed") throw new Error(`recording bot failed before capture: ${attempt.failureCode ?? "unknown"}`);
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error("timed out waiting for recording bot capture readiness");
}

async function readSignalStats(path: string, seconds: number, crop: string): Promise<{ y: number; u: number; v: number }> {
  const result = await new ChildProcessRunner().run(process.env.RECORDING_FFMPEG_PATH ?? ffmpeg.path, [
    "-v", "error", "-ss", String(seconds), "-i", path,
    "-vf", `${crop},signalstats,metadata=print:file=-`, "-frames:v", "1", "-f", "null", "-",
  ], 30_000);
  const read = (name: string) => Number(new RegExp(`lavfi\\.signalstats\\.${name}=([0-9.]+)`).exec(result.stdout)?.[1]);
  const stats = { y: read("YAVG"), u: read("UAVG"), v: read("VAVG") };
  if (Object.values(stats).some((value) => !Number.isFinite(value))) throw new Error(`unable to read layout signal stats: ${result.stdout.slice(0, 500)}`);
  return stats;
}
