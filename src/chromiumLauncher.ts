import {
  chromium,
  type Browser,
  type BrowserContext,
  type BrowserServer,
} from "playwright-core";

import type { BrowserHandle, BrowserLauncher } from "./attemptManager.js";

export type ChromiumDriver = {
  launchServer(options: {
    executablePath?: string;
    headless: boolean;
    args: string[];
  }): Promise<BrowserServer>;
  connect(wsEndpoint: string): Promise<Browser>;
};

export class PlaywrightChromiumLauncher implements BrowserLauncher {
  constructor(
    private readonly executablePath: string | undefined,
    private readonly stopTimeoutMs: number,
    private readonly driver: ChromiumDriver = {
      launchServer: (options) => chromium.launchServer(options),
      connect: (wsEndpoint) => chromium.connect(wsEndpoint),
    },
  ) {}

  async start(input: {
    attemptId: string;
    runtimeUrl: string;
    signal: AbortSignal;
    onExit: (error?: Error) => void;
  }): Promise<BrowserHandle> {
    if (input.signal.aborted) throw new Error("Chromium start aborted");
    const launch = this.driver.launchServer({
      ...(this.executablePath ? { executablePath: this.executablePath } : {}),
      headless: true,
      args: [
        "--autoplay-policy=no-user-gesture-required",
        "--disable-background-timer-throttling",
        "--disable-renderer-backgrounding",
        "--disable-dev-shm-usage",
        "--no-sandbox",
      ],
    });
    void launch
      .then((candidate) => {
        if (input.signal.aborted)
          void closeBounded(
            candidate,
            undefined,
            undefined,
            this.stopTimeoutMs,
          );
      })
      .catch(() => undefined);
    const server = await launch;
    if (input.signal.aborted) {
      await closeBounded(server, undefined, undefined, this.stopTimeoutMs);
      throw new Error("Chromium start aborted");
    }
    let browser: Browser | undefined;
    let context: BrowserContext | undefined;
    let abort: (() => void) | undefined;
    const abortDuringStart = () => {
      void closeBounded(server, browser, context, this.stopTimeoutMs);
    };
    input.signal.addEventListener("abort", abortDuringStart, { once: true });
    try {
      browser = await this.driver.connect(server.wsEndpoint());
      if (input.signal.aborted) throw new Error("Chromium start aborted");
      context = await browser.newContext({
        viewport: { width: 1280, height: 720 },
      });
      if (input.signal.aborted) throw new Error("Chromium start aborted");
      const page = await context.newPage();
      if (input.signal.aborted) throw new Error("Chromium start aborted");
      input.signal.removeEventListener("abort", abortDuringStart);
      abort = () => {
        void closeBounded(server, browser, context, this.stopTimeoutMs);
      };
      input.signal.addEventListener("abort", abort, { once: true });
      page.on("pageerror", (error) =>
        logRuntimeError(input.attemptId, "pageerror", error.message),
      );
      page.on("console", (message) =>
        logRuntimeMessage(input.attemptId, message.type(), message.text()),
      );
      page.on("requestfailed", (request) =>
        logRuntimeError(
          input.attemptId,
          "requestfailed",
          `${request.method()} ${request.url()} ${request.failure()?.errorText ?? "failed"}`,
        ),
      );
      let stopping = false;
      browser.once("disconnected", () => {
        if (!stopping) input.onExit(new Error("Chromium disconnected"));
      });
      await page.goto(input.runtimeUrl, { waitUntil: "domcontentloaded" });
      return {
        stop: async () => {
          if (stopping) return;
          stopping = true;
          input.signal.removeEventListener("abort", abort!);
          let stopError: unknown;
          try {
            await Promise.race([
              page.evaluate(async () => {
                const stop = (
                  globalThis as typeof globalThis & {
                    __stopRecordingBot?: () => Promise<void>;
                  }
                ).__stopRecordingBot;
                if (stop) await stop();
              }),
              rejectAfter(
                this.stopTimeoutMs,
                "Chromium runtime stop timed out",
              ),
            ]);
          } catch (error) {
            stopError = error;
          }
          await closeBounded(server, browser, context, this.stopTimeoutMs);
          if (stopError) throw stopError;
        },
      };
    } catch (error) {
      input.signal.removeEventListener("abort", abortDuringStart);
      if (abort) input.signal.removeEventListener("abort", abort);
      await closeBounded(server, browser, context, this.stopTimeoutMs);
      throw error;
    }
  }
}

function logRuntimeError(
  attemptId: string,
  source: string,
  error: string,
): void {
  console.error(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      severity: "error",
      service: "recording-bot",
      operation: "chromium.runtime",
      attemptId,
      source,
      error,
    }),
  );
}

function logRuntimeMessage(
  attemptId: string,
  level: string,
  message: string,
): void {
  const body = {
    timestamp: new Date().toISOString(),
    severity: level === "error" ? "error" : "info",
    service: "recording-bot",
    operation: "chromium.console",
    attemptId,
    level,
    message,
  };
  if (level === "error") console.error(JSON.stringify(body));
  else console.info(JSON.stringify(body));
}

export async function closeBounded(
  server: BrowserServer,
  browser: Browser | undefined,
  context: BrowserContext | undefined,
  timeoutMs: number,
): Promise<void> {
  const process = server.process();
  await Promise.race([
    Promise.allSettled([
      context?.close(),
      browser?.close(),
      server.kill(),
    ]).then(() => undefined),
    new Promise<void>((resolve) => setTimeout(resolve, timeoutMs)),
  ]);
  if (process.exitCode === null && process.signalCode === null)
    process.kill("SIGKILL");
}

function rejectAfter(timeoutMs: number, message: string): Promise<never> {
  return new Promise((_, reject) =>
    setTimeout(() => reject(new Error(message)), timeoutMs),
  );
}
