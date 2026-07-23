import { describe, expect, it, vi } from "vitest";

import { RuntimeReporter } from "./runtimeReporter.js";

describe("RuntimeReporter", () => {
  it("calls browser fetch without rebinding its receiver", async () => {
    vi.stubGlobal("location", { origin: "http://127.0.0.1:8090" });
    const fetchFn = vi.fn(function (this: unknown) {
      expect(this).toBeUndefined();
      return Promise.resolve(
        new Response(JSON.stringify({ roomId: "room-one" }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      );
    }) as unknown as typeof fetch;
    const reporter = new RuntimeReporter(
      { attemptId: "attempt-one", token: "runtime-token" },
      fetchFn,
    );

    await expect(reporter.loadConfig()).resolves.toMatchObject({
      roomId: "room-one",
    });
    await expect(
      reporter.report({ type: "runtime:ready", producerCount: 1 }),
    ).resolves.toBeUndefined();
    expect(fetchFn).toHaveBeenCalledTimes(2);
    vi.unstubAllGlobals();
  });
});
