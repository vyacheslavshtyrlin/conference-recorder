import { describe, expect, it } from "vitest";

import { RevisionConflictError, RevisionGapError, RevisionTracker } from "./revisionTracker.js";

describe("RevisionTracker", () => {
  it("accepts ordered changes and exact duplicate delivery", () => {
    const tracker = new RevisionTracker();
    tracker.reset(4);
    const event = { type: "bot:producerAdded", revision: 5, producerId: "p1" };
    expect(tracker.accept(event)).toBe("apply");
    expect(tracker.accept(event)).toBe("duplicate");
  });

  it("fails closed on gaps and conflicting duplicates", () => {
    const tracker = new RevisionTracker();
    tracker.reset(4);
    expect(() => tracker.accept({ revision: 6 })).toThrow(RevisionGapError);
    tracker.accept({ revision: 5, value: "a" });
    expect(() => tracker.accept({ revision: 5, value: "b" })).toThrow(RevisionConflictError);
  });
});
