export type BrowserBootstrap = { attemptId: string; token: string };

export type RuntimeConfig = {
  attemptId: string;
  recordingId: string;
  signalingUrl: string;
  admission: string;
};

export type Participant = { participantId: string; displayName: string };

export type Producer = {
  producerId: string;
  participantId: string;
  displayName: string;
  kind: "audio" | "video";
  source: "mic" | "camera" | "screen";
  paused: boolean;
};
