import type { Participant, Producer } from "./contracts.js";

export class OrderedProducerRegistry {
  private readonly participantMap = new Map<string, Participant>();
  private readonly producerMap = new Map<string, Producer>();

  get participants(): ReadonlyMap<string, Participant> {
    return this.participantMap;
  }

  get producers(): ReadonlyMap<string, Producer> {
    return this.producerMap;
  }

  reset(participants: Participant[], producers: Producer[]): void {
    this.participantMap.clear();
    this.producerMap.clear();
    for (const participant of participants)
      this.participantMap.set(participant.participantId, { ...participant });
    for (const producer of producers)
      this.producerMap.set(producer.producerId, { ...producer });
  }

  upsertParticipant(participant: Participant): void {
    this.participantMap.set(participant.participantId, { ...participant });
    for (const producer of this.producerMap.values()) {
      if (producer.participantId === participant.participantId)
        producer.displayName = participant.displayName;
    }
  }

  removeParticipant(participantId: string): void {
    this.participantMap.delete(participantId);
  }

  upsertProducer(producer: Producer): void {
    this.producerMap.set(producer.producerId, { ...producer });
  }

  getProducer(producerId: string): Producer | undefined {
    return this.producerMap.get(producerId);
  }

  removeProducer(producerId: string): void {
    this.producerMap.delete(producerId);
  }

  setProducerPaused(producerId: string, paused: boolean): Producer | undefined {
    const current = this.producerMap.get(producerId);
    if (!current) return undefined;
    const updated = { ...current, paused };
    this.producerMap.set(producerId, updated);
    return updated;
  }
}
