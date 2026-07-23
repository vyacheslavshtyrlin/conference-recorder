import { Device } from "mediasoup-client";
import type { Producer, Transport } from "mediasoup-client/types";

declare global {
  var __publisherReady: boolean | undefined;
  var __publisherStop: (() => Promise<void>) | undefined;
  var __publisherStartScreen: ((color: string) => Promise<void>) | undefined;
  var __publisherReplaceScreen: ((color: string) => Promise<void>) | undefined;
  var __publisherStopScreen: (() => Promise<void>) | undefined;
}

const parameters = new URL(location.href).searchParams;
const socketUrl = new URL(parameters.get("signalingUrl") ?? "");
socketUrl.searchParams.set("token", parameters.get("token") ?? "");
const socket = new WebSocket(socketUrl);
const pending = new Map<string, { resolve: (value: any) => void; reject: (error: Error) => void }>();
let resolveJoined: (() => void) | undefined;
let rejectJoined: ((error: Error) => void) | undefined;
const joined = new Promise<void>((resolve, reject) => {
  resolveJoined = resolve;
  rejectJoined = reject;
});
let sequence = 0;
let transport: Transport | undefined;
let stream: MediaStream | undefined;
const producers: Producer[] = [];
let cameraProducer: Producer | undefined;
let screenProducer: Producer | undefined;
let cameraTrack: MediaStreamTrack | undefined;
let screenTrack: MediaStreamTrack | undefined;
let screenTimer: number | undefined;
let audioContext: AudioContext | undefined;
let audioOscillator: OscillatorNode | undefined;

socket.addEventListener("message", (event) => {
  const message = JSON.parse(String(event.data));
  if (message.type === "room:snapshot") {
    resolveJoined?.();
    return;
  }
  if (message.type === "response" && message.requestId === "connection" && !message.ok) {
    rejectJoined?.(new Error(message.error?.code ?? "publisher connection rejected"));
    return;
  }
  if (message.type !== "response") return;
  const waiter = pending.get(message.requestId);
  if (!waiter) return;
  pending.delete(message.requestId);
  if (message.ok) waiter.resolve(message.data);
  else waiter.reject(new Error(message.error?.code ?? "signaling request failed"));
});

function request(type: string, body: Record<string, unknown> = {}): Promise<any> {
  const requestId = `publisher:${++sequence}`;
  return new Promise((resolve, reject) => {
    pending.set(requestId, { resolve, reject });
    socket.send(JSON.stringify({ type, requestId, ...body }));
  });
}

async function start(): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    socket.addEventListener("open", () => resolve(), { once: true });
    socket.addEventListener("error", () => reject(new Error("publisher socket failed")), { once: true });
  });
  await joined;
  await request("room:join");
  const routerRtpCapabilities = await request("mediasoup:getRouterRtpCapabilities");
  const device = new Device();
  await device.load({ routerRtpCapabilities });
  const data = await request("mediasoup:createWebRtcTransport", { direction: "send" });
  transport = device.createSendTransport(data);
  transport.on("connect", ({ dtlsParameters }, callback, errback) => {
    request("mediasoup:connectTransport", { transportId: transport!.id, dtlsParameters }).then(() => callback()).catch(errback);
  });
  transport.on("produce", ({ kind, rtpParameters, appData }, callback, errback) => {
    request("mediasoup:produce", { transportId: transport!.id, kind, rtpParameters, appData }).then(({ id }) => callback({ id })).catch(errback);
  });
  stream = await navigator.mediaDevices.getUserMedia({
    audio: false,
    video: { width: 640, height: 360, frameRate: 24 },
  });
  audioContext = new AudioContext();
  const audioDestination = audioContext.createMediaStreamDestination();
  const gain = audioContext.createGain();
  gain.gain.value = 0.08;
  audioOscillator = audioContext.createOscillator();
  audioOscillator.frequency.value = 440;
  audioOscillator.connect(gain).connect(audioDestination);
  audioOscillator.start();
  await audioContext.resume();
  const audio = audioDestination.stream.getAudioTracks()[0];
  cameraTrack = stream.getVideoTracks()[0];
  if (!audio || !cameraTrack) throw new Error("verification media tracks are unavailable");
  producers.push(await transport.produce({ track: audio, appData: { source: "mic" } }));
  cameraProducer = await transport.produce({ track: cameraTrack, stopTracks: false, appData: { source: "camera" } });
  producers.push(cameraProducer);
  globalThis.__publisherReady = true;
}

globalThis.__publisherStartScreen = async (color) => {
  await closeCamera();
  await delay(250);
  await produceScreen(color);
};

globalThis.__publisherReplaceScreen = async (color) => {
  await closeScreen();
  await delay(250);
  await produceScreen(color);
};

globalThis.__publisherStopScreen = async () => {
  await closeScreen();
};

globalThis.__publisherStop = async () => {
  for (const producer of producers) producer.close();
  await closeScreen();
  for (const track of stream?.getTracks() ?? []) track.stop();
  audioOscillator?.stop();
  await audioContext?.close();
  await request("room:leave").catch(() => undefined);
  transport?.close();
  socket.close();
};

async function closeCamera(): Promise<void> {
  if (cameraProducer) await request("mediasoup:closeProducer", { producerId: cameraProducer.id });
  cameraProducer?.close();
  cameraProducer = undefined;
}

async function closeScreen(): Promise<void> {
  if (screenProducer) await request("mediasoup:closeProducer", { producerId: screenProducer.id });
  screenProducer?.close();
  screenProducer = undefined;
  screenTrack?.stop();
  screenTrack = undefined;
  if (screenTimer !== undefined) window.clearInterval(screenTimer);
  screenTimer = undefined;
}

async function produceScreen(color: string): Promise<void> {
  if (!transport) throw new Error("publisher transport is unavailable");
  const canvas = document.createElement("canvas");
  canvas.width = 1280;
  canvas.height = 720;
  const context = canvas.getContext("2d")!;
  const draw = () => {
    context.fillStyle = color;
    context.fillRect(0, 0, canvas.width, canvas.height);
    context.fillStyle = "white";
    context.font = "72px sans-serif";
    context.fillText(`SCREEN ${color}`, 96, 160);
    context.font = "36px monospace";
    context.fillText(new Date().toISOString(), 96, 230);
  };
  draw();
  screenTimer = window.setInterval(draw, 100);
  screenTrack = canvas.captureStream(24).getVideoTracks()[0];
  if (!screenTrack) throw new Error("screen track is unavailable");
  screenProducer = await transport.produce({ track: screenTrack, stopTracks: false, appData: { source: "screen" } });
  producers.push(screenProducer);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
}

void start().catch((error) => { document.body.dataset.error = String(error); });
