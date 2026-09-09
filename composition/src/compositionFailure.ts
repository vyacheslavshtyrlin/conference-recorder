export type CompositionFailureStage =
  | "lease"
  | "control-plane"
  | "source"
  | "render"
  | "artifact-verify"
  | "artifact-upload";

export class CompositionFailure extends Error {
  constructor(
    readonly code: string,
    readonly stage: CompositionFailureStage,
    readonly retryable: boolean,
    readonly safeMessage: string,
    options?: ErrorOptions,
  ) {
    super(safeMessage, options);
  }
}

export function retryableFailure(
  code: string,
  stage: CompositionFailureStage,
  safeMessage: string,
  cause?: unknown,
): CompositionFailure {
  return new CompositionFailure(code, stage, true, safeMessage, { cause });
}

export function terminalFailure(
  code: string,
  stage: CompositionFailureStage,
  safeMessage: string,
  cause?: unknown,
): CompositionFailure {
  return new CompositionFailure(code, stage, false, safeMessage, { cause });
}

export function failureReport(error: unknown): {
  code: string;
  message: string;
  retryable: boolean;
  diagnostic?: string;
} {
  if (error instanceof CompositionFailure) {
    const diagnostic = diagnosticMessage(error.cause);
    return {
      code: error.code,
      message: `${error.stage}: ${error.safeMessage}`.slice(0, 512),
      retryable: error.retryable,
      ...(diagnostic ? { diagnostic } : {}),
    };
  }

  return {
    code: "COMPOSITION_FAILED",
    message: "composition: Composition failed",
    retryable: false,
  };
}

function diagnosticMessage(error: unknown): string | undefined {
  if (!(error instanceof Error) || !error.message) return undefined;
  return error.message.replace(/[\r\n\t]+/g, " ").slice(0, 2_048);
}
