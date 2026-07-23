import type { BrowserBootstrap, RuntimeConfig } from "./contracts.js";

export class RuntimeReporter {
  private failureReported = false;

  constructor(
    private readonly bootstrap: BrowserBootstrap,
    private readonly fetchFn: typeof fetch = fetch,
  ) {}

  async loadConfig(): Promise<RuntimeConfig> {
    const url = new URL("/internal/runtime-config", location.origin);
    url.searchParams.set("attemptId", this.bootstrap.attemptId);
    const fetchFn = this.fetchFn;
    const response = await fetchFn(url, {
      headers: { authorization: `Bearer ${this.bootstrap.token}` },
    });
    if (!response.ok) throw new Error("runtime config unavailable");
    return response.json() as Promise<RuntimeConfig>;
  }

  async report(event: Record<string, unknown>): Promise<void> {
    const fetchFn = this.fetchFn;
    const response = await fetchFn("/internal/runtime-events", {
      method: "POST",
      headers: {
        authorization: `Bearer ${this.bootstrap.token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ attemptId: this.bootstrap.attemptId, event }),
    });
    if (!response.ok)
      throw new Error(`runtime event rejected with status ${response.status}`);
  }

  async fail(code: string, message: string): Promise<Error> {
    const failure = new Error(`${code}: ${message}`);
    if (this.failureReported) return failure;
    this.failureReported = true;
    await this.report({
      type: "runtime:failed",
      code,
      message: message.slice(0, 1024),
    });
    return failure;
  }
}
