import type { Consumer, Transport } from "mediasoup-client/types";

import type { Producer } from "./contracts.js";

export class ConsumerManager {
  private readonly consumers = new Map<string, Consumer>();
  private transport?: Transport;
  private rtpCapabilities?: unknown;

  constructor(
    private readonly request: (
      type: string,
      body: Record<string, unknown>,
    ) => Promise<any>,
    private readonly attachTrack: (
      producer: Producer,
      consumer: Consumer,
    ) => Promise<void>,
  ) {}

  get size(): number {
    return this.consumers.size;
  }

  configure(transport: Transport, rtpCapabilities: unknown): void {
    this.transport = transport;
    this.rtpCapabilities = rtpCapabilities;
  }

  async reconcile(producer: Producer): Promise<void> {
    const consumer = this.consumers.get(producer.producerId);
    if (producer.paused) {
      if (consumer) {
        await this.request("bot:pauseConsumer", {
          producerId: producer.producerId,
        });
        consumer.pause();
      }
      return;
    }
    const active = await this.ensure(producer);
    if (active.paused) {
      await this.request("bot:resumeConsumer", {
        producerId: producer.producerId,
      });
      active.resume();
    }
  }

  async pause(producerId: string): Promise<void> {
    const consumer = this.consumers.get(producerId);
    if (!consumer) return;
    await this.request("bot:pauseConsumer", { producerId });
    consumer.pause();
  }

  close(producerId: string): void {
    this.consumers.get(producerId)?.close();
    this.consumers.delete(producerId);
  }

  private async ensure(producer: Producer): Promise<Consumer> {
    const existing = this.consumers.get(producer.producerId);
    if (existing) return existing;
    if (!this.transport) throw new Error("receive transport is not ready");
    const data = await this.request("bot:consume", {
      transportId: this.transport.id,
      producerId: producer.producerId,
      rtpCapabilities: this.rtpCapabilities,
    });
    const consumer = await this.transport.consume({
      id: data.id,
      producerId: data.producerId,
      kind: data.kind,
      rtpParameters: data.rtpParameters,
      appData: {
        source: producer.source,
        participantId: producer.participantId,
      },
    });
    this.consumers.set(producer.producerId, consumer);
    await this.attachTrack(producer, consumer);
    consumer.on("transportclose", () =>
      this.consumers.delete(producer.producerId),
    );
    consumer.on("trackended", () => this.consumers.delete(producer.producerId));
    await this.request("bot:resumeConsumer", {
      producerId: producer.producerId,
    });
    consumer.resume();
    return consumer;
  }
}
