// Steps (I-6): the declared step, the step call, its attempts, and the step key.
import { AsyncLocalStorage } from "node:async_hooks";
import { create } from "@bufbuild/protobuf";
import { Duration } from "./durations.ts";
import { InvalidArgument, messageOf, NotInWorkflow, typeName } from "./errors.ts";
import { checkName } from "./names.ts";
import { capped, decodeValue, jsonText, jsonValue, run } from "./proto.ts";
import { current, makeCall, type Shared, type StepBody } from "./scheduler.ts";
import { Permanent, type Retry, retryPolicy } from "./workflow.ts";

const stepKeys = new AsyncLocalStorage<string>();
/** The saga the steps called here belong to, set by `c.saga`. */
export const sagaScope: AsyncLocalStorage<bigint> = new AsyncLocalStorage<bigint>();

/** `<run>/<step>/<occurrence>`, the same for every attempt and replay of the step running. */
export function stepKey(): string {
  const key = stepKeys.getStore();
  if (key === undefined) throw new NotInWorkflow("stepKey is called inside a step");
  return key;
}

interface CommonOptions<T> {
  /** None means one attempt. */
  readonly retry?: Retry;
  /** Per attempt, the step's undo's too; the attempt fails with a retryable `StepTimeout`. It
   * wins over the workflow's `stepTimeout` and the worker's, larger or smaller. */
  readonly timeout?: Duration;
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

const KNOWN = new Set(["retry", "timeout", "decode", "undo"]);

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
  const { retry, timeout, decode, undo } = options;
  if (retry !== undefined) retryPolicy(retry);
  if (timeout !== undefined && !(timeout instanceof Duration)) {
    throw new InvalidArgument("timeout is a Duration");
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
  if (typeof name !== "string") throw new InvalidArgument(`a step name is a string, not ${name}`);
  if (typeof fn !== "function") throw new InvalidArgument(`a step runs a function, not ${fn}`);
  const { retry, timeout, decode, undo } = options;
  const policy = retry === undefined ? undefined : retryPolicy(retry);
  const saga = sagaScope.getStore();
  const timeoutMs = BigInt((timeout ?? shared.stepTimeout).ms);
  const stepOptions = create(run.StepOptionsSchema, {
    undo: undo !== undefined,
    timeoutMs,
    ...(saga === undefined ? {} : { saga }),
    ...(policy === undefined ? {} : { retry: policy }),
  });
  const call = makeCall({ case: "step", value: { name, options: stepOptions } });
  const decoded = (value: Parameters<typeof decodeValue>[0]) => {
    const json = decodeValue(value);
    return decode === undefined ? json : decode(json);
  };
  const body: StepBody = () => fn(...args);
  const undoSpec =
    undo === undefined
      ? undefined
      : { body: (value: unknown) => () => undo(value, ...args), retry: policy, timeoutMs };
  return shared.future<T>(call, decoded, { body, ...(undoSpec ? { undo: undoSpec } : {}) });
}

/** A declared step: calling it inside a workflow returns the promise of its value. */
export function step<A extends unknown[], R, T = Awaited<R>>(
  name: string,
  fn: (...args: A) => R,
  options: DeclaredStepOptions<A, T> = {},
): (...args: A) => Promise<T> {
  checkName("step name", name);
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

const TIMEOUT = Symbol("timeout");

/** Runs one attempt of a step with its step key; a `Permanent` error is not retryable. */
export async function attempt(
  body: StepBody,
  key: string,
  timeoutMs?: number,
): Promise<run.Outcome> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let value: unknown;
  try {
    const running = current.exit(() => stepKeys.run(key, async () => await body()));
    if (timeoutMs === undefined) {
      value = await running;
    } else {
      // The attempt cannot be stopped: its late outcome is discarded.
      running.catch(() => {});
      const late = new Promise((_, reject) => {
        timer = setTimeout(() => reject(TIMEOUT), timeoutMs);
      });
      value = await Promise.race([running, late]);
    }
  } catch (e) {
    if (e === TIMEOUT) return failure("StepTimeout", `attempt past ${timeoutMs} ms`, true);
    return failure(typeName(e), messageOf(e), !(e instanceof Permanent));
  } finally {
    clearTimeout(timer);
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
    return failure(typeName(e), messageOf(e), false);
  }
  return create(run.OutcomeSchema, { outcome: { case: "value", value: jsonValue(json) } });
}
