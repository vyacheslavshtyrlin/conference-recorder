import { spawn } from "node:child_process";

const maximumStdoutBytes = 4 * 1024 * 1024;
const maximumStderrBytes = 16 * 1024;

export interface ProcessRunner {
  run(command: string, args: string[], timeoutMs: number, signal?: AbortSignal): Promise<{ stdout: string; stderr?: string }>;
}

export class ChildProcessRunner implements ProcessRunner {
  run(command: string, args: string[], timeoutMs: number, signal?: AbortSignal): Promise<{ stdout: string; stderr: string }> {
    return new Promise((resolve, reject) => {
      const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"], windowsHide: true });
      const stdout: Buffer[] = [];
      const stderr: Buffer[] = [];
      let stdoutBytes = 0;
      let stderrBytes = 0;
      let settled = false;
      let terminationReason: "timeout" | "aborted" | "output" | undefined;
      const terminate = (reason: typeof terminationReason) => {
        if (terminationReason) return;
        terminationReason = reason;
        child.kill("SIGKILL");
      };
      const append = (chunks: Buffer[], chunk: Buffer, current: number, maximum: number) => {
        const remaining = maximum - current;
        if (remaining > 0) chunks.push(chunk.subarray(0, remaining));
        if (chunk.length > remaining) terminate("output");
        return current + chunk.length;
      };
      child.stdout.on("data", (chunk: Buffer) => {
        stdoutBytes = append(stdout, Buffer.from(chunk), stdoutBytes, maximumStdoutBytes);
      });
      child.stderr.on("data", (chunk: Buffer) => {
        stderrBytes = append(stderr, Buffer.from(chunk), stderrBytes, maximumStderrBytes);
      });
      const onAbort = () => terminate("aborted");
      signal?.addEventListener("abort", onAbort, { once: true });
      const finish = (callback: () => void) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        callback();
      };
      const timer = setTimeout(() => terminate("timeout"), timeoutMs);
      child.once("error", (error) => finish(() => reject(error)));
      child.once("exit", (code, exitSignal) => finish(() => {
        if (terminationReason === "timeout") return reject(new Error(`${command} timed out`));
        if (terminationReason === "aborted") return reject(new Error(`${command} aborted after lease loss`));
        if (terminationReason === "output") return reject(new Error(`${command} output exceeded its bound`));
        const stderrText = Buffer.concat(stderr).toString("utf8");
        if (code !== 0) return reject(new Error(`${command} exited with ${String(code ?? exitSignal)}: ${stderrText.slice(0, 2048)}`));
        resolve({ stdout: Buffer.concat(stdout).toString("utf8"), stderr: stderrText });
      }));
      if (signal?.aborted) onAbort();
    });
  }
}
