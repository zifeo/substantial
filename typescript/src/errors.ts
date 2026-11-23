/** A failure as recorded in a history: a type name, a message, whether a retry may help, and the
 * JSON object of details the core records with the failures it makes, else `undefined`. */
export interface Failure {
  // § r3sd261h4@knsap
  readonly type: string;
  readonly message: string;
  readonly retryable: boolean;
  readonly details?: Record<string, unknown>;
}

/** A failure with `details` as JSON gives them: an object, or null or `undefined` for none. */
export function makeFailure(
  type: string,
  message: string,
  retryable: boolean,
  details: unknown,
): Failure {
  if (details === undefined || details === null) return { type, message, retryable };
  if (typeof details !== "object" || Array.isArray(details)) {
    throw new ProtocolError(`${type} with details ${JSON.stringify(details)}, not an object`);
  }
  return { type, message, retryable, details: details as Record<string, unknown> };
}

// § rabbgjpb2@z2tc8
/** The base of every error Substantial raises, control flow excepted. */
export class SubstantialError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

// § b7wsejcvb.b@ae69b
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
  /** Into the highest version of its workflow the worker serves: a loop with `upgrade`. */
  readonly highest: boolean;
  constructor(
    workflow: string | undefined,
    version: number | undefined,
    input: Uint8Array,
    state?: Uint8Array,
    highest = false,
  ) {
    super("continued as new");
    this.workflow = workflow;
    this.version = version;
    this.input = input;
    this.state = state;
    this.highest = highest;
  }
}
// § end b7wsejcvb.b

/** A step had no attempt left, or failed with a permanent error; `cause` is its last attempt's
 * failure, such as `StepTimeout`, `ValueTooLarge`, `Json` for a value that does not encode, or
 * the type of the error it threw. */
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

/** A failure the workflow's code raises on purpose: the run ends failed with exactly this type
 * and message. */
export class Fail extends SubstantialError {
  readonly failure: Failure;
  constructor(type: string, message: string) {
    if (typeof type !== "string" || typeof message !== "string") {
      throw new InvalidArgument(`Fail takes a type and a message, strings: ${type}, ${message}`);
    }
    super(`${type}: ${message}`);
    this.failure = { type, message, retryable: false };
  }
}

/** The run passed a limit of its history: the core ends it failed with this type. */
export class HistoryTooLarge extends SubstantialError {}

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

/** The hint a divergence from the history carries; the core records the failure without it. */
export const NEW_VERSION = "an incompatible change needs a new version (version=)";

/** The run ended failed or cancelled. */
export class RunFailed extends SubstantialError {
  readonly run: string;
  readonly status: EndedStatus;
  readonly error: Failure;
  /** The `StepFailed.cause` of the step that failed the run, else `undefined`. */
  override readonly cause: Failure | undefined;
  constructor(run: string, status: EndedStatus, error: Failure, cause: Failure | undefined) {
    const hint = error.type === "NonDeterminism" ? `; ${NEW_VERSION}` : "";
    super(`run ${run} ${status}: ${error.type}: ${error.message}${hint}`);
    this.run = run;
    this.status = status;
    this.error = error;
    this.cause = cause;
  }
}

/** The run passed the `limit` of its budget on `meter`, having `used` that much: thrown at every
 * call it was blocked on then and at every later step, start and send. The run ends failed with
 * it whatever the code does. */
export class BudgetExceeded extends SubstantialError {
  readonly meter: string;
  readonly limit: bigint;
  readonly used: bigint;
  constructor(meter: string, limit: bigint, used: bigint) {
    super(`budget exceeded: ${meter} used ${used}, over the limit of ${limit}`);
    this.meter = meter;
    this.limit = limit;
    this.used = used;
  }
}

/** The failure a `StepFailed`'s details wrap, `{ error: {...} }`; `undefined` if they hold none. */
export function wrappedFailure(stepDetails: unknown): Failure | undefined {
  const error = (stepDetails as Record<string, unknown> | undefined)?.error as
    | Record<string, unknown>
    | undefined;
  const { type, message, retryable, details } = error ?? {};
  if (typeof type !== "string" || typeof message !== "string" || typeof retryable !== "boolean") {
    return undefined;
  }
  return makeFailure(type, message, retryable, details);
}

/** The cause of a run failed with `type`: a `StepFailed`'s wraps it in its details. */
export function runFailedCause(type: string, details: unknown): Failure | undefined {
  // § r3sd261h4.a@sx2cf
  if (type !== "StepFailed") return undefined;
  const cause = wrappedFailure(details);
  if (cause === undefined) {
    throw new ProtocolError(`a run failed with StepFailed with details ${JSON.stringify(details)}`);
  }
  return cause;
}

export class NonDeterminism extends SubstantialError {}
export class NotInWorkflow extends SubstantialError {}
export class RunExists extends SubstantialError {
  constructor(id: string) {
    super(`id ${id} is taken by another workflow, version or input`);
  }
}
export class InvalidArgument extends SubstantialError {}
export class RunEnded extends SubstantialError {}
/** No such chain: it never existed, or it ended and its retention passed. */
export class RunNotFound extends SubstantialError {
  constructor(chain: string) {
    super(`no chain ${chain}`);
  }
}
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
// § end rabbgjpb2

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
  // § r3sd261h4.c@e31hh
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
  if (e instanceof StepFailed || e instanceof Failed || e instanceof Fail) {
    return e.failure;
  }
  if (e instanceof RolledBack) return { type: "RolledBack", message: e.reason, retryable: false };
  // § one r3sd261h4.c@4349e
  return { type: typeName(e), message: messageOf(e), retryable: false };
}

/** Whether an error the code lets out ends its run failed: a recorded outcome of a verb, the same
 * at every replay. Any other error blocks the run. */
export function endsRun(e: unknown): boolean {
  // § dxgjekysz.d@mpdas
  return (
    e instanceof Fail ||
    e instanceof StepFailed ||
    e instanceof BudgetExceeded ||
    e instanceof RolledBack ||
    e instanceof UndoFailed ||
    e instanceof HistoryTooLarge ||
    e instanceof RunFailed ||
    e instanceof WaitTimeout ||
    e instanceof RunNotFound ||
    e instanceof RunEnded ||
    e instanceof Failed
  );
}
