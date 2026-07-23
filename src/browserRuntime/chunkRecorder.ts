export type ChunkRecorderOptions = {
  stream: MediaStream;
  upload: (segment: Blob) => Promise<void>;
  onFailure: (error: Error) => Promise<void>;
  segmentDurationMs?: number;
};

/**
 * Produces independently closed WebM blobs. Each segment owns a fresh
 * MediaRecorder/container header, so an acknowledged segment survives a later
 * browser or recorder crash.
 */
export class ChunkRecorder {
  private recorder?: MediaRecorder;
  private uploadTail = Promise.resolve();
  private uploadFailure?: Error;
  private segmentTimer?: number;
  private stopping = false;
  private currentStopped = Promise.resolve();

  constructor(private readonly options: ChunkRecorderOptions) {}

  start(): void {
    if (this.recorder) return;
    this.startSegment();
  }

  async stop(): Promise<void> {
    this.stopping = true;
    if (this.segmentTimer !== undefined) clearTimeout(this.segmentTimer);
    if (this.recorder && this.recorder.state !== "inactive")
      this.recorder.stop();
    await this.currentStopped;
    await this.uploadTail;
  }

  private startSegment(): void {
    const mimeType = [
      "video/webm;codecs=vp9,opus",
      "video/webm;codecs=vp8,opus",
      "video/webm",
    ].find((candidate) => MediaRecorder.isTypeSupported(candidate));
    if (!mimeType) throw new Error("WebM MediaRecorder is unavailable");
    const chunks: Blob[] = [];
    const recorder = new MediaRecorder(this.options.stream, {
      mimeType,
      videoBitsPerSecond: 8_000_000,
      audioBitsPerSecond: 192_000,
    });
    this.recorder = recorder;
    this.currentStopped = new Promise<void>((resolve) => {
      recorder.addEventListener("dataavailable", (event) => {
        if (event.data.size > 0) chunks.push(event.data);
      });
      recorder.addEventListener(
        "stop",
        () => {
          const blob = new Blob(chunks, { type: mimeType });
          if (blob.size > 0) this.queueUpload(blob);
          resolve();
          if (!this.stopping && !this.uploadFailure) this.startSegment();
        },
        { once: true },
      );
    });
    recorder.start();
    this.segmentTimer = window.setTimeout(
      () => {
        if (recorder.state !== "inactive") recorder.stop();
      },
      this.options.segmentDurationMs ?? 5_000,
    );
    console.info(`recording-bot: MediaRecorder segment started mime=${mimeType}`);
  }

  private queueUpload(blob: Blob): void {
    this.uploadTail = this.uploadTail.then(async () => {
      if (this.uploadFailure) throw this.uploadFailure;
      try {
        await this.options.upload(blob);
      } catch (error) {
        this.uploadFailure =
          error instanceof Error ? error : new Error(String(error));
        await this.options.onFailure(this.uploadFailure);
        throw this.uploadFailure;
      }
    });
  }
}
