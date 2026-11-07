// The TypeScript scheduler of I-24: what the code and the replay driver share for one replay. The
// code's calls go to the run machine in the order the code makes them; the driver resolves their
// promises one `Next` at a time and lets the microtask queue drain before it asks for the next.
import { AsyncLocalStorage } from "node:async_hooks";
import { create, type MessageInitShape } from "@bufbuild/protobuf";
import type { Duration } from "./durations.ts";
import {
  Cancelled,
  CoreError,
  Failed,
  type Failure,
  InvalidArgument,
  messageOf,
  NEW_VERSION,
  NonDeterminism,
  ProtocolError,
  RunEnded,
  RunFailed,
  RunNotFound,
  StepFailed,
  typeName,
  ValueTooLarge,
} from "./errors.ts";
import type { RunMachine } from "./machines.ts";
import { decodeValue, type layout, run } from "./proto.ts";

export type Clock = () => layout.Time;
export type Decode = (value: layout.Value | undefined) => unknown;
export type StepBody = () => unknown;
/** An undo, given the value of the step it undoes, as the body of its step. */
export type UndoBody = (value: unknown) => StepBody;
/** A bound handler of messages; it throws `Failed` when it cannot take one. */
export type Handler = (message: run.Message) => void;
type Predicate = () => unknown;
type Answer = Exclude<run.Called["called"], { case: undefined }>;
type Answers = { [A in Answer as A["case"]]: A["value"] };

export interface StepSpec {
  readonly body: StepBody;
  readonly undo?: {
    readonly body: UndoBody;
    readonly retry: run.RetryPolicy | undefined;
    readonly timeoutMs: bigint;
  };
}

/** The replay whose code is running, for declared steps; unset in step attempts. */
export const current: AsyncLocalStorage<Shared> = new AsyncLocalStorage<Shared>();

// Calls carrying the code's own names or values: `INVALID` for them is the code's argument.
const NAMED = new Set([
  "step",
  "receive",
  "bind",
  "start",
  "childResult",
  "send",
  "publish",
  "iterate",
]);

interface Slot {
  readonly resolve: (value: unknown) => void;
  readonly reject: (error: unknown) => void;
  readonly decode: Decode;
}

export function makeCall(call: MessageInitShape<typeof run.CallSchema>["call"]): run.Call {
  return create(run.CallSchema, { call });
}

function handled<T>(promise: Promise<T>): Promise<T> {
  // A call the code never awaits must not crash the process when it is rejected, as on a cancel.
  promise.catch(() => {});
  return promise;
}

function settle(decode: Decode, value: layout.Value | undefined): Promise<unknown> {
  try {
    return Promise.resolve(decode(value));
  } catch (e) {
    return Promise.reject(e);
  }
}

function runFailed(f: layout.Failure): Error {
  const d = f.details?.kind.case === "json" ? decodeValue(f.details) : undefined;
  const { run, status, error, reason } = (d ?? {}) as Record<string, unknown>;
  if (typeof run !== "string") return new ProtocolError(`RunFailed of ${JSON.stringify(d)}`);
  if (status === "failed" && typeof error === "object" && error !== null) {
    const { type, message, retryable } = error as Failure;
    return new RunFailed(run, status, { type, message, retryable });
  }
  if (status === "cancelled" && typeof reason === "string") {
    return new RunFailed(run, status, { type: "Cancelled", message: reason, retryable: false });
  }
  return new ProtocolError(`RunFailed with details ${JSON.stringify(d)}`);
}

function stepFailed(f: layout.Failure, failure: Failure): Error {
  const d = f.details?.kind.case === "json" ? decodeValue(f.details) : undefined;
  const error = (d as Record<string, unknown> | undefined)?.error as
    | Record<string, unknown>
    | undefined;
  const { type, message, retryable } = error ?? {};
  if (typeof type !== "string" || typeof message !== "string" || typeof retryable !== "boolean") {
    return new ProtocolError(`StepFailed with details ${JSON.stringify(d)}`);
  }
  return new StepFailed(failure, { type, message, retryable });
}

/** The error a call resolved with a failure throws in the code. */
export function errorFromFailure(f: layout.Failure): Error {
  const failure: Failure = { type: f.type, message: f.message, retryable: f.retryable };
  switch (f.type) {
    case "RunFailed":
      return runFailed(f);
    case "StepFailed":
      return stepFailed(f, failure);
    case "RunNotFound":
      return new RunNotFound(f.message);
    case "RunEnded":
      return new RunEnded(f.message);
    default:
      return new Failed(failure);
  }
}

/** What the code and the replay driver share for one replay. */
export class Shared {
  readonly machine: RunMachine;
  readonly clock: Clock;
  readonly run: string;
  readonly start: layout.Start;
  /** The timeout of a step without its own: its workflow's, else its worker's. */
  readonly stepTimeout: Duration;
  /** Whether the code went past the recorded history, so that its logs are written (I-14). */
  live: boolean;
  readonly slots: Map<bigint, Slot> = new Map();
  /** The steps pending, with their step key. */
  readonly steps: Map<bigint, { body: StepBody; key: string }> = new Map();
  /** The undos of every step call of the replay, by call, for a rollback. */
  readonly undos: Map<bigint, NonNullable<StepSpec["undo"]>> = new Map();
  /** The children whose chain ended. */
  readonly ended: Set<string> = new Set();
  readonly handlers: Map<string, Handler[]> = new Map();
  waits: [bigint, Predicate][] = [];
  /** A core error met by a call of the code: the replay cannot go on. */
  fatal: string | undefined;
  /** A call the history does not answer: the core recorded the run's failure, so the code is
   * abandoned and every later call fails the same way. */
  diverged: string | undefined;
  /** A failure outside the code's reach, a handler's or a predicate's: the run fails with it. */
  codeFailure: Failure | undefined;
  /** The run's end is recorded: nothing of the code runs again, as it cannot be stopped. */
  abandoned = false;
  closed = false;

  constructor(
    machine: RunMachine,
    clock: Clock,
    data: run.RunInput,
    start: layout.Start,
    stepTimeout: Duration,
  ) {
    this.machine = machine;
    this.clock = clock;
    this.run = data.run;
    this.start = start;
    this.stepTimeout = stepTimeout;
    this.live = data.history.length === 0;
  }

  protocol(message: string): ProtocolError {
    this.fatal ??= message;
    return new ProtocolError(message);
  }

  /** Makes a call of the code. `INVALID` or `VALUE_TOO_LARGE` for a call carrying the code's own
   * names or values is the code's argument and goes back to it; any other core error is fatal to
   * the replay. */
  call(call: run.Call): run.Called {
    if (this.closed) throw new ProtocolError("a call of the code after its replay ended");
    if (this.diverged !== undefined) throw new NonDeterminism(this.diverged);
    let called: run.Called;
    try {
      called = this.machine.call(call, this.clock());
    } catch (e) {
      if (e instanceof NonDeterminism) {
        this.diverged = `${e.message}; ${NEW_VERSION}`;
        this.abandoned = true;
        this.waits = [];
        console.warn(`substantial: run ${this.run}: ${this.diverged}`);
        throw new NonDeterminism(this.diverged);
      }
      const named = NAMED.has(call.call.case ?? "");
      if (e instanceof CoreError && e.kind === "INVALID" && named) {
        throw new InvalidArgument(e.detail);
      }
      if (e instanceof ValueTooLarge && named) throw e;
      this.fatal ??= e instanceof Error ? e.message : String(e);
      throw e;
    }
    this.live = called.live;
    return called;
  }

  /** Makes a call answered at once with `expected`, and returns its answer. */
  expect<K extends keyof Answers>(call: run.Call, expected: K): Answers[K] {
    const answer = this.call(call).called;
    if (answer.case !== expected) {
      throw this.protocol(`${call.call.case} answered ${answer.case}, not ${expected}`);
    }
    return answer.value as Answers[K];
  }

  /** Makes a call and returns the promise of its value: settled at once, or by a later
   * `resolve`. A pending wait is satisfied once `predicate` holds. */
  future<T>(call: run.Call, decode: Decode, spec?: StepSpec, predicate?: Predicate): Promise<T> {
    const called = this.call(call);
    const answer = called.called;
    if (spec !== undefined) {
      if (called.occurrence === undefined) {
        throw this.protocol("step call answered without its occurrence");
      }
      if (answer.case === "pending" && call.call.case === "step") {
        const key = `${this.run}/${call.call.value.name}/${called.occurrence}`;
        this.steps.set(answer.value, { body: spec.body, key });
        if (spec.undo !== undefined) this.undos.set(answer.value, spec.undo);
      }
    }
    switch (answer.case) {
      case "pending": {
        const id = answer.value;
        if (predicate !== undefined) this.waits.push([id, predicate]);
        return handled(
          new Promise<T>((resolve, reject) => {
            this.slots.set(id, { resolve: resolve as (v: unknown) => void, reject, decode });
          }),
        );
      }
      case "value":
        return handled(settle(decode, answer.value) as Promise<T>);
      case "done":
        return Promise.resolve(undefined as T);
      default:
        throw this.protocol(`unexpected answer ${answer.case} to ${call.call.case}`);
    }
  }

  failCode(failure: Failure): void {
    this.codeFailure ??= failure;
  }

  /** Binds `handler` to `names`: the messages consumed so far first, then each new one. */
  bind(names: string[], handler: Handler): void {
    const past = this.expect(makeCall({ case: "bind", value: { names } }), "messages");
    try {
      for (const m of past.messages) handler(m);
    } catch (e) {
      if (e instanceof Failed) this.failCode(e.failure);
      throw e;
    }
    for (const name of names) {
      const bound = this.handlers.get(name);
      if (bound === undefined) this.handlers.set(name, [handler]);
      else bound.push(handler);
    }
  }

  /** Hands a message to the handlers of its name, in the order they were bound. */
  deliver(message: run.Message): void {
    if (this.abandoned) return;
    for (const handler of [...(this.handlers.get(message.name) ?? [])]) {
      try {
        handler(message);
      } catch (e) {
        if (!(e instanceof Failed)) throw e;
        this.failCode(e.failure);
        return;
      }
    }
  }

  /** Tells the run machine of every wait whose predicate now holds. */
  evaluateWaits(): void {
    if (this.abandoned) return;
    const pending: [bigint, Predicate][] = [];
    for (const [call, predicate] of this.waits) {
      let held: boolean;
      try {
        held = Boolean(predicate());
      } catch (e) {
        const message = `a waitUntil predicate: ${messageOf(e)}`;
        this.failCode({ type: typeName(e), message, retryable: false });
        return;
      }
      if (held) this.machine.satisfied(call);
      else pending.push([call, predicate]);
    }
    this.waits = pending;
  }

  resolve(r: run.Next_Resolve): void {
    const slot = this.slots.get(r.call);
    if (slot === undefined) throw new ProtocolError(`resolve of unknown call ${r.call}`);
    this.slots.delete(r.call);
    this.steps.delete(r.call);
    this.waits = this.waits.filter(([call]) => call !== r.call);
    if (this.abandoned) return;
    const resolution = r.resolution?.resolution;
    switch (resolution?.case) {
      case "value":
        settle(slot.decode, resolution.value).then(slot.resolve, slot.reject);
        return;
      case "error":
        slot.reject(errorFromFailure(resolution.value));
        return;
      case "cancelled":
        slot.reject(new Cancelled(resolution.value));
        return;
      default:
        throw new ProtocolError(`empty resolution of call ${r.call}`);
    }
  }

  stepEnded(call: bigint, attempt: number, outcome: run.Outcome): void {
    this.machine.stepEnded(call, attempt, outcome, this.clock());
  }

  arrived(messages: run.InboxMessage[]): void {
    this.machine.arrived(messages);
  }

  next(): run.Next {
    const next = this.machine.next(this.clock());
    this.live = next.live;
    return next;
  }
}
