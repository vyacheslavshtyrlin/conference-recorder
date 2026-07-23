import { createHash } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { AttemptRequest } from "./contracts.js";
import type { RecordingSink, SegmentInput } from "./recordingSink.js";

type Session = { request: AttemptRequest; generation: number };

export class RemoteRecordingSink implements RecordingSink {
  private readonly sessions = new Map<string, Session>();
  constructor(private readonly root: string, private readonly apiUrl: string, private readonly secret: string) {}
  async begin(request: AttemptRequest): Promise<{ generation: number }> { this.sessions.set(request.recordingId, { request, generation: 1 }); return { generation: 1 }; }
  async appendSegment(recordingId: string, segment: SegmentInput): Promise<void> {
    if (!segment.bytes.length) return;
    const session = this.require(recordingId);
    if (segment.generation !== session.generation) throw new Error("remote recording generation mismatch");
    const sha256 = createHash("sha256").update(segment.bytes).digest("hex");
    const folder = join(this.root, "spool", recordingId, String(segment.generation));
    const file = join(folder, `${segment.sequence}.webm`);
    await mkdir(folder, { recursive: true, mode: 0o750 });
    await writeFile(file, segment.bytes, { flag: "wx", mode: 0o640 }).catch(async (error: NodeJS.ErrnoException) => { if (error.code !== "EEXIST") throw error; });
    const body = { generation: segment.generation, sequence: segment.sequence, timelineStartMs: segment.timelineStartMs, timelineEndMs: segment.timelineEndMs, bytes: segment.bytes.length, sha256 };
    const [target] = await this.json<Array<{ uploadId: string; uploadUrl: string }>>(`/internal/v1/recordings/${encodeURIComponent(recordingId)}/segments/upload-batch`, { segments: [body] });
    if (!target) throw new Error("recording upload allocation missing");
    const put = await fetch(target.uploadUrl, { method: "PUT", headers: { "content-type": "video/webm", "content-length": String(segment.bytes.length) }, body: segment.bytes });
    if (!put.ok) {
      const detail = (await put.text()).replace(/\s+/g, " ").slice(0, 512);
      throw new Error(`recording segment upload failed: ${put.status}${detail ? ` ${detail}` : ""}`);
    }
    await this.json(`/internal/v1/recordings/${encodeURIComponent(recordingId)}/segments/commit`, { ...body, uploadId: target.uploadId });
    await rm(file, { force: true });
  }
  async finalize(recordingId: string): Promise<{ path: string }> { await this.json(`/internal/v1/recordings/${encodeURIComponent(recordingId)}/seal`, {}); this.sessions.delete(recordingId); return { path: "remote" }; }
  async fail(recordingId: string, code: string, message: string): Promise<void> {
    try {
      await this.json(`/internal/v1/recordings/${encodeURIComponent(recordingId)}/fail`, { code, message: message.replace(/[\r\n\t]+/g, " ").slice(0, 512) });
    } finally {
      this.sessions.delete(recordingId);
    }
  }
  private async json<T = unknown>(path: string, body: unknown): Promise<T> { const response = await fetch(`${this.apiUrl}${path}`, { method: "POST", headers: { authorization: `Bearer ${this.secret}`, "content-type": "application/json" }, body: JSON.stringify(body) }); if (!response.ok) throw new Error(`recording API request failed: ${response.status}`); return response.json() as Promise<T>; }
  private require(recordingId: string): Session { const session = this.sessions.get(recordingId); if (!session) throw new Error("remote recording session not found"); return session; }
}
