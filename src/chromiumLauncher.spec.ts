import type { ChildProcess } from "node:child_process";

import type { Browser, BrowserContext, BrowserServer } from "playwright-core";
import { describe, expect, it, vi } from "vitest";

import { closeBounded, PlaywrightChromiumLauncher, type ChromiumDriver } from "./chromiumLauncher.js";

describe("closeBounded", () => {
  it("force-kills Chromium when graceful browser and context close hang", async () => {
    const processKill = vi.fn().mockReturnValue(true);
    const child = { exitCode: null, signalCode: null, kill: processKill } as unknown as ChildProcess;
    const server = { process: () => child, kill: vi.fn().mockResolvedValue(undefined) } as unknown as BrowserServer;
    const never = () => new Promise<void>(() => undefined);
    const browser = { close: vi.fn(never) } as unknown as Browser;
    const context = { close: vi.fn(never) } as unknown as BrowserContext;

    await closeBounded(server, browser, context, 5);

    expect(server.kill).toHaveBeenCalledOnce();
    expect(processKill).toHaveBeenCalledWith("SIGKILL");
  });

  it("kills a launched BrowserServer when connecting to it fails", async () => {
    const processKill = vi.fn().mockReturnValue(true);
    const child = { exitCode: null, signalCode: null, kill: processKill } as unknown as ChildProcess;
    const server = {
      wsEndpoint: () => "ws://127.0.0.1/devtools/browser/test",
      process: () => child,
      kill: vi.fn().mockResolvedValue(undefined),
    } as unknown as BrowserServer;
    const driver: ChromiumDriver = {
      launchServer: vi.fn().mockResolvedValue(server),
      connect: vi.fn().mockRejectedValue(new Error("connect failed")),
    };
    const launcher = new PlaywrightChromiumLauncher(undefined, 5, driver);

    await expect(launcher.start({
      attemptId: "attempt-one",
      runtimeUrl: "http://127.0.0.1:8090/runtime/attempt-one",
      signal: new AbortController().signal,
      onExit: vi.fn(),
    })).rejects.toThrow("connect failed");

    expect(server.kill).toHaveBeenCalledOnce();
    expect(processKill).toHaveBeenCalledWith("SIGKILL");
  });
});
