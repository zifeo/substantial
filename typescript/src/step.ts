// Steps (I-6): the declared step, the step call, its attempts, and the step key.
import { AsyncLocalStorage } from "node:async_hooks";
import { create } from "@bufbuild/protobuf";
import type { Clock } from "./clock.ts";
import { MAX } from "./cost.ts";
import { Duration } from "./durations.ts";
import { InvalidArgument, messageOf, NotInWorkflow, typeName } from "./errors.ts";
import { checkStepName } from "./names.ts";
import { capped, decodeValue, jsonText, jsonValue, layout, run } from "./proto.ts";
import { current, makeCall, type Shared, type StepBody } from "./scheduler.ts";
import { Permanent, type Retry, retryPolicy } from "./workflow.ts";

/** The attempt running: its key, its signal, and what it charged so far, in call order. */
const attempts = new AsyncLocalStorage<{
  key: string;
  signal: AbortSignal;
  charges: layout.Amount[];
  beat: () => void;
}>();
/** The saga the steps called here belong to, set by `c.saga`. */
export const sagaScope: AsyncLocalStorage<bigint> = new AsyncLocalStorage<bigint>();

/** `<run>/<step>/<occurrence>`, the same for every attempt and replay of the step running. */
export function stepKey(): string {
  const running = attempts.getStore();
  if (running === undefined) throw new NotInWorkflow("stepKey is called inside a step");
  return running.key;
}

/** The signal of the attempt running, aborted once it is abandoned: its timeout passed, its
 * worker died, or its worker shut down past its grace. JavaScript cannot stop a body: pass it to
 * `fetch`, timers and SDK calls, else the body runs to its end, its outcome dropped. */
export function stepSignal(): AbortSignal {
  const running = attempts.getStore();
  if (running === undefined) throw new NotInWorkflow("stepSignal is called inside a step");
  return running.signal;
}

/** Tells the worker that the step attempt running is alive: a step with a `heartbeatTimeout`
 * fails its attempt with a retryable `StepTimeout` once that long passed without one. */
export function heartbeat(): void {
  // § vvp37rp40.a@v0tvp
  const running = attempts.getStore();
  if (running === undefined) throw new NotInWorkflow("heartbeat is called inside a step");
  running.beat();
}

/** Adds `nano` nano-units of `currency` to the charges of the step attempt running, recorded with
 * its outcome and counted in the run's cost; a currency outside ISO 4217 fails the step at once
 * with `InvalidArgument`, once the attempt ended. `nano` is a bigint from 0n to 2n ** 63n - 1n. */
export function charge(currency: string, nano: bigint): void {
  const running = attempts.getStore();
  if (running === undefined) throw new NotInWorkflow("charge is called inside a step");
  if (typeof currency !== "string") {
    throw new InvalidArgument(`a currency is a string, not ${String(currency)}`);
  }
  // § one cvnf47vy8.a@8sb9f
  if (typeof nano !== "bigint" || nano < 0n || nano > MAX) {
    const shown = typeof nano === "bigint" ? `${nano}n` : String(nano);
    throw new InvalidArgument(`a charge is a bigint from 0n to ${MAX}n, not ${shown}`);
  }
  // § one cvnf47vy8@3ccsy
  running.charges.push(create(layout.AmountSchema, { currency, nano }));
}

interface CommonOptions<T> {
  /** None means one attempt. */
  readonly retry?: Retry;
  /** Per attempt, the step's undo's too; the attempt fails with a retryable `StepTimeout`. It
   * wins over the workflow's `stepTimeout` and the worker's, larger or smaller. */
  readonly timeout?: Duration;
  /** The longest an attempt may go without `s.heartbeat()`, the step's undo's too, over the
   * workflow's `heartbeatTimeout`: an attempt silent for longer fails with a retryable
   * `StepTimeout`. None unless set. */
  readonly heartbeatTimeout?: Duration;
  /** Turns the recorded JSON into the value the code sees, the first time too. */
  readonly decode?: (value: unknown) => T;
}

export interface StepOptions<T> extends CommonOptions<T> {
  /** Takes the step's value; a rollback runs it. */
  readonly undo?: (value: T) => unknown;
}

export interface DeclaredStepOptions<A extends unknown[], T> extends CommonOptions<T> {
  /** Takes the step's value then its arguments; a rollback runs it. */
  readonly undo?: (value: T, ...args: A) => unknown;
}

const KNOWN = new Set(["retry", "timeout", "heartbeatTimeout", "decode", "undo"]);

/** Checks the options of a step, declared or inline. */
export function checkOptions(options: DeclaredStepOptions<unknown[], unknown>): void {
  if (typeof options !== "object" || options === null) {
    throw new InvalidArgument("step options are an object");
  }
  for (const key of Object.keys(options)) {
    if (!KNOWN.has(key)) {
      throw new InvalidArgument(`unknown step option ${key}`);
    }
  }
  const { retry, timeout, heartbeatTimeout, decode, undo } = options;
  if (retry !== undefined) retryPolicy(retry);
  if (timeout !== undefined && !(timeout instanceof Duration)) {
    throw new InvalidArgument("timeout is a Duration");
  }
  if (
    heartbeatTimeout !== undefined &&
    !(heartbeatTimeout instanceof Duration && heartbeatTimeout.ms > 0)
  ) {
    throw new InvalidArgument("heartbeatTimeout is a positive Duration");
  }
  if (decode !== undefined && typeof decode !== "function") {
    throw new InvalidArgument("decode is a function");
  }
  if (undo !== undefined && typeof undo !== "function")
    throw new InvalidArgument("undo is a function");
}

/** Calls the step `name`: its occurrence is taken now, and its value comes from its attempts or
 * from the history. */
export function callStep<T>(
  shared: Shared,
  name: string,
  fn: (...args: unknown[]) => unknown,
  args: unknown[],
  options: DeclaredStepOptions<unknown[], unknown>,
): Promise<T> {
  // § wv67rmrjm@h7dg9
  if (typeof name !== "string") throw new InvalidArgument(`a step name is a string, not ${name}`);
  if (typeof fn !== "function") throw new InvalidArgument(`a step runs a function, not ${fn}`);
  const { retry, timeout, heartbeatTimeout, decode, undo } = options;
  const policy = retry === undefined ? undefined : retryPolicy(retry);
  const saga = sagaScope.getStore();
  // § one aym52h8ch@m9cxb
  const timeoutMs = BigInt((timeout ?? shared.stepTimeout).ms);
  const stepOptions = create(run.StepOptionsSchema, {
    undo: undo !== undefined,
    timeoutMs,
    ...(saga === undefined ? {} : { saga }),
    ...(policy === undefined ? {} : { retry: policy }),
  });
  const call = makeCall({ case: "step", value: { name, options: stepOptions } });
  // § vjy5p3exq@h7efq
  const decoded = (value: Parameters<typeof decodeValue>[0]) => {
    const json = decodeValue(value);
    return decode === undefined ? json : decode(json);
  };
  // § end vjy5p3exq
  const body: StepBody = () => fn(...args);
  // § vvp37rp40@t2zza
  const heartbeatMs = (heartbeatTimeout ?? shared.heartbeatTimeout)?.ms;
  // § end vvp37rp40
  // § a5c1mfs8d@pwf7k
  const undoSpec =
    undo === undefined
      ? undefined
      : {
          body: (value: unknown) => () => undo(value, ...args),
          retry: policy,
          timeoutMs,
          // § one vvp37rp40.b@8pgw7
          heartbeatMs,
        };
  // § end a5c1mfs8d
  return shared.future<T>(call, decoded, {
    body,
    heartbeatMs,
    ...(undoSpec ? { undo: undoSpec } : {}),
  });
}

/** A declared step: calling it inside a workflow returns the promise of its value. */
export function step<A extends unknown[], R, T = Awaited<R>>(
  name: string,
  fn: (...args: A) => R,
  options: DeclaredStepOptions<A, T> = {},
): (...args: A) => Promise<T> {
  checkStepName(name);
  if (typeof fn !== "function") throw new InvalidArgument(`a step runs a function, not ${fn}`);
  checkOptions(options as DeclaredStepOptions<unknown[], unknown>);
  const opts = options as DeclaredStepOptions<unknown[], unknown>;
  const declared = (...args: A): Promise<T> => {
    const shared = current.getStore();
    if (shared === undefined) throw new NotInWorkflow(`step ${name} is called outside a workflow`);
    return callStep<T>(shared, name, fn as (...args: unknown[]) => unknown, args, opts);
  };
  Object.defineProperty(declared, "name", { value: name });
  return declared;
}

function failure(type: string, message: string, retryable: boolean): run.Outcome {
  return create(run.OutcomeSchema, {
    outcome: { case: "error", value: { type, message, retryable } },
  });
}

/** How an attempt ended: its outcome, what it charged, and whether it was abandoned as its
 * replay ended, its outcome then never recorded. */
export interface Attempted {
  readonly outcome: run.Outcome;
  readonly charges: layout.Amount[];
  readonly abandoned: boolean;
}

/** Runs one attempt of a step with its step key, to its outcome and its charges; a `Permanent`
 * error is not retryable. */
export async function attempt(
  clock: Clock,
  name: string,
  body: StepBody,
  key: string,
  timeoutMs: number | undefined,
  abandoned: AbortSignal,
  heartbeatMs?: number,
): Promise<Attempted> {
  const charges: layout.Amount[] = [];
  const heartbeat =
    heartbeatMs === undefined ? undefined : { timeoutMs: heartbeatMs, last: clock.elapsed() };
  const beat = () => {
    if (heartbeat !== undefined) heartbeat.last = clock.elapsed();
  };
  const outcome = await clock.attempt(
    name,
    key,
    (signal) => outcomeOf(body, { key, signal, charges, beat }),
    timeoutMs,
    abandoned,
    heartbeat,
  );
  // A copy: an attempt left running charges on, past its end.
  // § cvnf47vy8@h7sz5
  const charged = [...charges];
  if (outcome === "silent") {
    // § vvp37rp40@zejv2
    const silent = failure("StepTimeout", `no heartbeat within ${heartbeatMs} ms`, true);
    return { outcome: silent, charges: charged, abandoned: false };
  }
  // § v4n6ja42e@g43mf
  if (outcome !== undefined) return { outcome, charges: charged, abandoned: false };
  // § one aym52h8ch@q82j5, epxcjcz4s@nv4p2
  const timedOut = failure("StepTimeout", `attempt past ${timeoutMs} ms`, true);
  return { outcome: timedOut, charges: charged, abandoned: abandoned.aborted };
  // § end v4n6ja42e
}

async function outcomeOf(
  body: StepBody,
  running: { key: string; signal: AbortSignal; charges: layout.Amount[]; beat: () => void },
): Promise<run.Outcome> {
  // § epxcjcz4s@16116
  let value: unknown;
  try {
    value = await current.exit(() => attempts.run(running, async () => await body()));
  } catch (e) {
    // § epxcjcz4s.a@2vdk6
    return failure(typeName(e), messageOf(e), !(e instanceof Permanent));
  }
  let text: string;
  try {
    text = jsonText(value);
  } catch (e) {
    return failure("Json", messageOf(e), false);
  }
  let json: Uint8Array;
  try {
    json = capped(new TextEncoder().encode(text), "the step's value");
  } catch (e) {
    // § h8wg39cct.b@fad42
    return failure(typeName(e), messageOf(e), false);
  }
  return create(run.OutcomeSchema, { outcome: { case: "value", value: jsonValue(json) } });
}
