export class AudioMixer {
  private readonly context = new AudioContext();
  private readonly destination = this.context.createMediaStreamDestination();
  private readonly nodes = new Map<string, MediaStreamAudioSourceNode>();
  private readonly elements = new Map<string, HTMLAudioElement>();

  get inputCount(): number {
    return this.nodes.size;
  }

  get state(): AudioContextState {
    return this.context.state;
  }

  async resume(): Promise<void> {
    await this.context.resume();
  }

  outputTrack(): MediaStreamTrack | undefined {
    return this.destination.stream.getAudioTracks()[0];
  }

  attach(producerId: string, track: MediaStreamTrack): void {
    const stream = new MediaStream([track]);
    const source = this.context.createMediaStreamSource(stream);
    source.connect(this.destination);
    this.nodes.set(producerId, source);
    const audio = document.createElement("audio");
    audio.autoplay = true;
    audio.muted = true;
    audio.srcObject = stream;
    this.elements.set(producerId, audio);
    void audio.play().catch((error: unknown) =>
      console.warn(
        `recording-bot: audio playback pending producer=${producerId} error=${String(error)}`,
      ),
    );
  }

  remove(producerId: string): void {
    this.nodes.get(producerId)?.disconnect();
    this.nodes.delete(producerId);
    this.elements.get(producerId)?.remove();
    this.elements.delete(producerId);
  }
}
