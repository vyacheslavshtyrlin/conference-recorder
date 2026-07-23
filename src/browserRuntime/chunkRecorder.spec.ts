import { afterEach, describe, expect, it, vi } from "vitest";

import { ChunkRecorder } from "./chunkRecorder.js";

class FakeMediaRecorder extends EventTarget {
  static instances: FakeMediaRecorder[] = [];
  static isTypeSupported(): boolean {
    return true;
  }

  state: RecordingState = "inactive";

  constructor(
    readonly stream: MediaStream,
    readonly options?: MediaRecorderOptions,
  ) {
    super();
    FakeMediaRecorder.instances.push(this);
  }

  start(): void {
    this.state = "recording";
  }

  stop(): void {
    this.state = "inactive";
    const dataEvent = Object.assign(new Event("dataavailable"), {
      data: new Blob([`segment-${FakeMediaRecorder.instances.indexOf(this)}`]),
    });
    this.dispatchEvent(dataEvent);
    this.dispatchEvent(new Event("stop"));
  }
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  FakeMediaRecorder.instances = [];
});

describe("ChunkRecorder", () => {
  it("stops and recreates MediaRecorder so every upload is a closed container", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("window", globalThis);
    vi.stubGlobal("MediaRecorder", FakeMediaRecorder);
    const upload = vi.fn().mockResolvedValue(undefined);
    const recorder = new ChunkRecorder({
      stream: {} as MediaStream,
      upload,
      onFailure: vi.fn().mockResolvedValue(undefined),
      segmentDurationMs: 1000,
    });

    recorder.start();
    expect(FakeMediaRecorder.instances).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(FakeMediaRecorder.instances).toHaveLength(2);
    await vi.waitFor(() => expect(upload).toHaveBeenCalledTimes(1));

    await recorder.stop();
    expect(upload).toHaveBeenCalledTimes(2);
    expect(FakeMediaRecorder.instances[0]).not.toBe(
      FakeMediaRecorder.instances[1],
    );
  });
});
