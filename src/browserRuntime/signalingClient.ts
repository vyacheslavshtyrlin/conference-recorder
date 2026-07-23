type ResponseMessage = {
  type: "response";
  requestId: string;
  ok: boolean;
  data?: unknown;
  error?: { code: string; message: string };
};

export class SignalingClient {
  private readonly pending = new Map<
    string,
    {
      resolve: (value: any) => void;
      reject: (error: Error) => void;
      timer: number;
    }
  >();
  private socket?: WebSocket;
  private sequence = 0;

  constructor(
    private readonly attemptId: string,
    private readonly onLifecycleMessage: (message: Record<string, any>) => void,
    private readonly onClose: (event: CloseEvent) => void,
    private readonly onError: () => void,
  ) {}

  connect(url: URL): void {
    const socket = new WebSocket(url);
    this.socket = socket;
    socket.addEventListener("message", (event) => {
      let message: Record<string, any>;
      try {
        message = JSON.parse(String(event.data)) as Record<string, any>;
      } catch {
        this.onLifecycleMessage({ type: "bot:invalidMessage" });
        return;
      }
      if (message.type === "response") this.onResponse(message as ResponseMessage);
      else this.onLifecycleMessage(message);
    });
    socket.addEventListener("close", this.onClose);
    socket.addEventListener("error", this.onError);
  }

  get isOpen(): boolean {
    return this.socket?.readyState === WebSocket.OPEN;
  }

  request(type: string, body: Record<string, unknown>): Promise<any> {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN)
      return Promise.reject(new Error("signaling socket is not open"));
    const requestId = `${this.attemptId}:${++this.sequence}`;
    return new Promise((resolve, reject) => {
      const timer = window.setTimeout(() => {
        this.pending.delete(requestId);
        reject(new Error(`${type} timed out`));
      }, 10000);
      this.pending.set(requestId, { resolve, reject, timer });
      this.socket!.send(JSON.stringify({ type, requestId, ...body }));
    });
  }

  close(): void {
    this.socket?.close();
  }

  private onResponse(message: ResponseMessage): void {
    const pending = this.pending.get(message.requestId);
    if (!pending) return;
    this.pending.delete(message.requestId);
    clearTimeout(pending.timer);
    if (message.ok) pending.resolve(message.data);
    else pending.reject(new Error(message.error?.code ?? "bot request failed"));
  }
}
