import { Device } from "mediasoup-client";
import type { Consumer } from "mediasoup-client/types";

import { ChunkRecorder } from "./browserRuntime/chunkRecorder.js";
import { AudioMixer } from "./browserRuntime/audioMixer.js";
import { CanvasComposer } from "./browserRuntime/canvasComposer.js";
import type {
  BrowserBootstrap as Bootstrap,
  Participant,
  Producer,
} from "./browserRuntime/contracts.js";
import { RevisionConflictError, RevisionGapError, RevisionTracker } from "./browserRuntime/revisionTracker.js";
import { RuntimeReporter } from "./browserRuntime/runtimeReporter.js";
import { SignalingClient } from "./browserRuntime/signalingClient.js";
import { ConsumerManager } from "./browserRuntime/consumerManager.js";
import { OrderedProducerRegistry } from "./browserRuntime/orderedProducerRegistry.js";
class RecordingBotBrowser {
  private readonly producers: ReadonlyMap<string, Producer>;
  private readonly participants: ReadonlyMap<string, Participant>;
  private readonly videos = new Map<string, HTMLVideoElement>();
  private readonly canvas =
    document.querySelector<HTMLCanvasElement>("#stage")!;
  private readonly audioMixer = new AudioMixer();
  private readonly composer: CanvasComposer;
  private readonly signaling: SignalingClient;
  private readonly consumerManager: ConsumerManager;
  private recorder?: ChunkRecorder;
  private drawTimer?: number;
  private lifecycleTail = Promise.resolve();
  private stopping?: Promise<void>;
  private readonly revisions = new RevisionTracker();
  private readonly reporter: RuntimeReporter;
  private ready = false;
  private failureReported = false;
  private runtimeFailure?: Error;

  constructor(
    private readonly bootstrap: Bootstrap,
    private readonly registry: OrderedProducerRegistry,
  ) {
    this.producers = registry.producers;
    this.participants = registry.participants;
    this.composer = new CanvasComposer(
      this.canvas,
      this.participants,
      this.producers,
      this.videos,
    );
    this.reporter = new RuntimeReporter(bootstrap);
    this.signaling = new SignalingClient(
      bootstrap.attemptId,
      (message) => this.queueMessage(message),
      (event) => {
        console.error(
          `recording-bot: signaling closed code=${event.code} clean=${event.wasClean} reason=${event.reason || "none"}`,
        );
        if (this.ready && !this.stopping)
          void this.failRuntime("BOT_CAPTURE_INCOMPLETE", "signaling disconnected");
      },
      () => {
        if (!this.ready)
          void this.failRuntime("BOT_CONNECT_TIMEOUT", "signaling connection failed");
      },
    );
    this.consumerManager = new ConsumerManager(
      (type, body) => this.request(type, body),
      (producer, consumer) => this.attachTrack(producer, consumer),
    );
  }

  async start(): Promise<void> {
    const config = await this.reporter.loadConfig();
    const url = new URL(config.signalingUrl);
    url.pathname = "/internal/recording-bot/v1";
    url.search = "";
    url.searchParams.set("admission", config.admission);
    this.signaling.connect(url);
  }

  private queueMessage(message: Record<string, any>): void {
    if (message.type === "bot:invalidMessage") {
      void this.failRuntime("BOT_RUNTIME_EXITED", "invalid signaling message");
      return;
    }
    this.lifecycleTail = this.lifecycleTail
      .then(() => this.onLifecycleMessage(message))
      .catch(async (error: unknown) => {
        await this.failRuntime("BOT_RUNTIME_EXITED", String(error));
        this.signaling.close();
      })
      .catch((error: unknown) =>
        console.error(
          `recording-bot: failure delivery failed: ${String(error)}`,
        ),
      );
  }

  private async onLifecycleMessage(
    message: Record<string, any>,
  ): Promise<void> {
    if (message.type === "bot:admitted") {
      console.info("recording-bot: admitted");
      await this.initializeTransport();
      return;
    }
    if (message.type === "bot:producerSnapshot") {
      console.info(
        `recording-bot: snapshot participants=${message.participants.length} producers=${message.producers.length}`,
      );
      this.revisions.reset(Number(message.revision));
      this.registry.reset(
        message.participants as Participant[],
        message.producers as Producer[],
      );
      for (const producer of this.producers.values())
        await this.reconcileProducer(producer);
      await this.startCapture();
      this.ready = true;
      await this.reporter.report({
        type: "runtime:ready",
        producerCount: this.consumerManager.size,
      });
      return;
    }
    if (message.type === "bot:stop") {
      await this.stopCapture();
      return;
    }
    if (typeof message.revision === "number")
      await this.applyRevision(message);
  }

  private async initializeTransport(): Promise<void> {
    const routerRtpCapabilities = await this.request(
      "bot:getRouterRtpCapabilities",
      {},
    );
    const device = new Device();
    await device.load({ routerRtpCapabilities });
    const options = await this.request("bot:createRecvTransport", {});
    const transport = device.createRecvTransport(options);
    transport.on("connect", ({ dtlsParameters }, callback, errback) => {
      this.request("bot:connectRecvTransport", {
        transportId: transport.id,
        dtlsParameters,
      })
        .then(() => callback())
        .catch(errback);
    });
    transport.on("connectionstatechange", (state) => {
      if (state === "failed" || state === "closed")
        void this.failRuntime(
          "BOT_RUNTIME_EXITED",
          `receive transport ${state}`,
        );
    });
    this.consumerManager.configure(transport, device.rtpCapabilities);
    await this.request("bot:syncSnapshot", {});
  }

  private async applyRevision(
    event: Record<string, any>,
  ): Promise<void> {
    try {
      if (this.revisions.accept(event) === "duplicate") return;
    } catch (error) {
      const message =
        error instanceof RevisionConflictError
          ? "conflicting producer revision"
          : error instanceof RevisionGapError
            ? "producer revision gap"
            : String(error);
      await this.failRuntime("BOT_SIGNALING_GAP", message);
      this.signaling.close();
      return;
    }
    if (
      event.type === "bot:participantAdded" ||
      event.type === "bot:participantUpdated"
    ) {
      this.registry.upsertParticipant(event.participant);
    }
    if (event.type === "bot:participantLeft")
      this.registry.removeParticipant(event.participantId);
    if (
      event.type === "bot:producerAdded" ||
      event.type === "bot:producerUpdated"
    ) {
      await this.reconcileProducer(event.producer);
    }
    const producerId = String(event.producerId ?? "");
    if (event.type === "bot:producerPaused") {
      this.registry.setProducerPaused(producerId, true);
      await this.consumerManager.pause(producerId);
    }
    if (event.type === "bot:producerResumed") {
      const producer = this.registry.setProducerPaused(producerId, false);
      if (producer) {
        await this.reconcileProducer(producer);
      }
    }
    if (event.type === "bot:producerClosed") {
      this.registry.removeProducer(producerId);
      this.consumerManager.close(producerId);
      this.videos.get(producerId)?.remove();
      this.videos.delete(producerId);
      this.audioMixer.remove(producerId);
    }
  }

  private async reconcileProducer(producer: Producer): Promise<void> {
    this.registry.upsertProducer(producer);
    await this.consumerManager.reconcile(producer);
  }

  stopCapture(): Promise<void> {
    if (this.stopping) return this.stopping;
    this.stopping = this.doStopCapture();
    return this.stopping;
  }

  private async startCapture(): Promise<void> {
    if (this.recorder) return;
    await this.audioMixer.resume();
    console.info(
      `recording-bot: audio context state=${this.audioMixer.state} inputs=${this.audioMixer.inputCount}`,
    );
    const stream = this.composer.captureStream(24);
    const audioTrack = this.audioMixer.outputTrack();
    if (audioTrack) stream.addTrack(audioTrack);
    this.recorder = new ChunkRecorder({
      stream,
      upload: (chunk) => this.uploadChunk(chunk),
      onFailure: async (error) => {
        await this.failRuntime("BOT_CAPTURE_INCOMPLETE", error.message);
      },
    });
    this.recorder.start();
    this.composer.drawFrame();
    this.drawTimer = window.setInterval(() => this.composer.drawFrame(), 1000 / 24);
  }

  private async doStopCapture(): Promise<void> {
    this.ready = false;
    if (this.drawTimer !== undefined) clearInterval(this.drawTimer);
    const uploadError = await this.recorder?.stop().then(
      () => undefined,
      (error: unknown) => error,
    );
    if (this.signaling.isOpen)
      await this.request("bot:teardown", {}).catch(() => undefined);
    const failure = uploadError ?? this.runtimeFailure;
    if (failure) {
      await this.failRuntime("BOT_CAPTURE_INCOMPLETE", String(failure)).catch(
        () => undefined,
      );
      throw failure;
    }
    await this.reporter.report({ type: "runtime:stopped" });
  }

  private async uploadChunk(blob: Blob): Promise<void> {
    const url = new URL("/internal/runtime-chunks", location.origin);
    url.searchParams.set("attemptId", this.bootstrap.attemptId);
    const response = await fetch(url, {
      method: "POST",
      headers: {
        authorization: `Bearer ${this.bootstrap.token}`,
        "content-type": "video/webm",
      },
      body: blob,
    });
    if (!response.ok) throw new Error("recording chunk rejected");
  }

  private async attachTrack(
    producer: Producer,
    consumer: Consumer,
  ): Promise<void> {
    const stream = new MediaStream([consumer.track]);
    if (producer.kind === "audio") {
      this.audioMixer.attach(producer.producerId, consumer.track);
      consumer.track.addEventListener("unmute", () =>
        console.info(
          `recording-bot: audio track unmuted producer=${producer.producerId}`,
        ),
      );
      return;
    }
    const video = document.createElement("video");
    video.autoplay = true;
    video.muted = true;
    video.playsInline = true;
    video.srcObject = stream;
    this.videos.set(producer.producerId, video);
    void video.play().catch((error: unknown) => {
      console.warn(
        `recording-bot: video playback pending producer=${producer.producerId} error=${String(error)}`,
      );
    });
  }

  private request(type: string, body: Record<string, unknown>): Promise<any> {
    return this.signaling.request(type, body);
  }

  private async failRuntime(code: string, message: string): Promise<void> {
    if (this.failureReported) return;
    this.failureReported = true;
    console.error(`recording-bot: runtime failure code=${code} message=${message}`);
    this.runtimeFailure = await this.reporter.fail(code, message);
    this.ready = false;
  }
}

const bootstrap: Bootstrap = {
  attemptId: decodeURIComponent(location.pathname.split("/").at(-1) ?? ""),
  token: new URL(location.href).searchParams.get("token") ?? "",
};
const runtime = new RecordingBotBrowser(
  bootstrap,
  new OrderedProducerRegistry(),
);
globalThis.__stopRecordingBot = () => runtime.stopCapture();
void runtime.start().catch((error: unknown) => {
  void fetch("/internal/runtime-events", {
    method: "POST",
    headers: {
      authorization: `Bearer ${bootstrap.token}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      attemptId: bootstrap.attemptId,
      event: {
        type: "runtime:failed",
        code: "BOT_RUNTIME_EXITED",
        message: String(error),
      },
    }),
  });
});

declare global {
  var __stopRecordingBot: (() => Promise<void>) | undefined;
}
