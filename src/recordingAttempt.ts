import type { AttemptState } from "./contracts.js";

export type RecordingAttemptEvent =
  | { type: "runtime-ready" }
  | { type: "stop-requested" }
  | { type: "runtime-stopped" }
  | { type: "finalized" }
  | { type: "failed"; code: string };

/**
 * The single authority for recording-attempt lifecycle transitions.
 * Orchestration owns effects; this object owns which state changes are legal.
 */
export class RecordingAttempt {
  private current: AttemptState = "starting";
  private terminalFailureCode?: string;

  get state(): AttemptState {
    return this.current;
  }

  get failureCode(): string | undefined {
    return this.terminalFailureCode;
  }

  get acceptsRuntimeAccess(): boolean {
    return this.current !== "failed";
  }

  get occupiesSlot(): boolean {
    return this.current !== "failed";
  }

  transition(event: RecordingAttemptEvent): boolean {
    if (this.current === "failed" || this.current === "ready") return false;

    if (event.type === "failed") {
      this.current = "failed";
      this.terminalFailureCode = event.code;
      return true;
    }
    if (event.type === "stop-requested") {
      if (this.current !== "starting" && this.current !== "capturing")
        return false;
      this.current = "stopping";
      return true;
    }
    if (event.type === "runtime-stopped") {
      if (this.current !== "stopping") return false;
      this.current = "finalizing";
      return true;
    }
    if (event.type === "finalized") {
      if (this.current !== "finalizing") return false;
      this.current = "ready";
      return true;
    }
    if (event.type === "runtime-ready") {
      if (this.current !== "starting") return false;
      this.current = "capturing";
      return true;
    }
    return false;
  }
}
