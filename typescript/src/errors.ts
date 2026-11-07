/** A failure as recorded in a history: a type name, a message, whether a retry may help. */
export interface Failure {
  readonly type: string;
  readonly message: string;
  readonly retryable: boolean;
}

/** The base of every error Substantial raises, control flow excepted. */
export class SubstantialError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Control flow of Substantial: workflow code rethrows it, `isControl(e)`. */
export class Control extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Whether `e` is Substantial's control flow, which workflow code must rethrow. */
export function isControl(e: unknown): e is Control {
  return e instanceof Control;
}

/** The run is cancelled: thrown at every call it is blocked on. */
export class Cancelled extends Control {
  readonly reason: string;
  constructor(reason: string) {
    super(reason);
    this.reason = reason;
  }
}

/** A rollback ran its undos: the run ends failed with this reason. */
export class RolledBack extends Control {
  readonly reason: string;
  constructor(reason: string) {
    super(reason);
    this.reason = reason;
  }
}

/** The run ends and the next of its chain starts: `continueAsNew`, or `iterate`. */
export class ContinuedAsNew extends Control {
  readonly workflow: string | undefined;
  readonly version: number | undefined;
  /** JSON bytes. */
  readonly input: Uint8Array;
  readonly state: Uint8Array | undefined;
  constructor(
    workflow: string | undefined,
    version: number | undefined,
    input: Uint8Array,
    state?: Uint8Array,
  ) {
    super("continued as new");
    this.workflow = workflow;
    this.version = version;
    this.input = input;
    this.state = state;
  }
}

/** A step had no attempt left, or failed with a permanent error; `cause` is its last attempt's
 * failure, such as `StepTimeout`, `ValueTooLarge` or the type of the error it threw. */
export class StepFailed extends SubstantialError {
  readonly failure: Failure;
  override readonly cause: Failure;
  constructor(failure: Failure, cause: Failure) {
    super(failure.message);
    this.failure = failure;
    this.cause = cause;
  }
}

/** A call resolved with an error other than a step's. */
export class Failed extends SubstantialError {
  readonly failure: Failure;
  constructor(failure: Failure) {
    super(`${failure.type}: ${failure.message}`);
    this.failure = failure;
  }
}

/** An undo had no attempt left; `step` is the undo's step, `<step>.undo`. */
export class UndoFailed extends SubstantialError {
  readonly step: string;
  readonly failure: Failure;
  constructor(step: string, failure: Failure) {
    super(`undo ${step} failed: ${failure.type}: ${failure.message}`);
    this.step = step;
    this.failure = failure;
  }
}

/** The state of `iterate` does not serialize to JSON at a compression. */
export class NotCompressible extends SubstantialError {}

/** A value whose JSON is over the limit a value may take. */
export class ValueTooLarge extends SubstantialError {}

export type EndedStatus = "failed" | "cancelled";

/** The run ended failed or cancelled. */
/** The hint a divergence from the history carries; the core records the failure without it. */
export const NEW_VERSION = "an incompatible change needs a new version (version=)";

export class RunFailed extends SubstantialError {
  readonly run: string;
  readonly status: EndedStatus;
  readonly error: Failure;
  constructor(run: string, status: EndedStatus, error: Failure) {
    const hint = error.type === "NonDeterminism" ? `; ${NEW_VERSION}` : "";
    super(`run ${run} ${status}: ${error.type}: ${error.message}${hint}`);
    this.run = run;
    this.status = status;
    this.error = error;
  }
}

export class NonDeterminism extends SubstantialError {}
export class NotInWorkflow extends SubstantialError {}
export class RunExists extends SubstantialError {}
export class InvalidArgument extends SubstantialError {}
export class RunEnded extends SubstantialError {}
export class RunNotFound extends SubstantialError {}
export class WaitTimeout extends SubstantialError {}
export class StoreError extends SubstantialError {}
export class LayoutError extends SubstantialError {}
export class ConfigError extends SubstantialError {}

/** A core error other than non-determinism and layout: `INVALID`, `ENDED`, `CORRUPT`. */
export class CoreError extends SubstantialError {
  readonly kind: string;
  readonly detail: string;
  constructor(kind: string, detail: string) {
    super(`core error ${kind}: ${detail}`);
    this.kind = kind;
    this.detail = detail;
  }
}

/** The core trapped, a bug of the core: its instance is dropped with every machine in it. */
export class Trap extends SubstantialError {}

/** The module could not be loaded: checksum, compilation, missing export. */
export class ModuleError extends SubstantialError {}

/** The core answered with a response this host does not expect at that point. */
export class ProtocolError extends SubstantialError {}

/** Refuses options that are not an object, or that hold a name not in `names`. */
export function onlyOptions(
  options: unknown,
  names: string[],
  what: string,
  error: new (message: string) => SubstantialError = InvalidArgument,
): void {
  if (typeof options !== "object" || options === null) {
    throw new error(`${what} options are an object, not ${options}`);
  }
  const unknown = Object.keys(options).filter((name) => !names.includes(name));
  if (unknown.length > 0) throw new error(`unknown ${what} options ${unknown.join(", ")}`);
}

/** The type name of a thrown value, as a failure records it. */
export function typeName(e: unknown): string {
  if (e instanceof Error) {
    return e.constructor.name || e.name;
  }
  return typeof e;
}

export function messageOf(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** The failure an error of the code ends its run with. */
export function failureOf(e: unknown): Failure {
  if (e instanceof StepFailed || e instanceof Failed) {
    return e.failure;
  }
  if (e instanceof RolledBack) return { type: "RolledBack", message: e.reason, retryable: false };
  return { type: typeName(e), message: messageOf(e), retryable: false };
}
