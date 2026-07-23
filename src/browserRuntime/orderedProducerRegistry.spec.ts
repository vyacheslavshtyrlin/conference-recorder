import { describe, expect, it } from "vitest";

import { OrderedProducerRegistry } from "./orderedProducerRegistry.js";

describe("OrderedProducerRegistry", () => {
  it("atomically replaces a snapshot and applies ordered identity updates", () => {
    const registry = new OrderedProducerRegistry();
    registry.reset(
      [{ participantId: "alice", displayName: "Alice" }],
      [{
        producerId: "camera",
        participantId: "alice",
        displayName: "Alice",
        kind: "video",
        source: "camera",
        paused: false,
      }],
    );
    registry.upsertParticipant({
      participantId: "alice",
      displayName: "Alice Updated",
    });
    expect(registry.getProducer("camera")?.displayName).toBe("Alice Updated");
    expect([...registry.participants]).toHaveLength(1);
  });

  it("keys producers by producerId and removes exact entries idempotently", () => {
    const registry = new OrderedProducerRegistry();
    registry.reset([], []);
    registry.removeProducer("missing");
    expect(registry.getProducer("missing")).toBeUndefined();
  });

  it("applies pause then resume as ordered immutable producer replacements", () => {
    const registry = new OrderedProducerRegistry();
    registry.reset([], [{
      producerId: "screen",
      participantId: "alice",
      displayName: "Alice",
      kind: "video",
      source: "screen",
      paused: false,
    }]);
    const original = registry.getProducer("screen");
    const paused = registry.setProducerPaused("screen", true);
    const resumed = registry.setProducerPaused("screen", false);
    expect(original?.paused).toBe(false);
    expect(paused?.paused).toBe(true);
    expect(resumed?.paused).toBe(false);
    expect(paused).not.toBe(original);
    expect(resumed).not.toBe(paused);
  });
});
