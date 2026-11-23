// Children (I-11) and messages sent from a workflow (I-9).
import { type Budget, effectiveBudget } from "./cost.ts";
import { Duration } from "./durations.ts";
import { InvalidArgument, onlyOptions, RunFailed } from "./errors.ts";
import { checkKey, checkSentName } from "./names.ts";
import { decodeValue, encodeCanonical, encodeJson, jsonValue, type layout } from "./proto.ts";
import { makeCall, type Shared } from "./scheduler.ts";
import { type AnyWorkflow, Workflow } from "./workflow.ts";

export interface MessageOptions {
  /** Delivered once per id; the core derives one from this call when none is given. */
  readonly id?: string;
}

export interface ChildOptions {
  /** The child's id; the core derives one from this run's when none is given. */
  readonly id?: string;
  /** The child's budget, else its workflow's (I-28). */
  readonly budget?: Budget;
  /** The child goes on when this run ends; by default the run's end, completed, failed or
   * cancelled, cancels it. */
  readonly abandon?: boolean;
}

/** The canonical JSON bytes (RFC 8785) of an input, as the core compares a start's input byte
 * for byte: an object, `{}` when none is given. */
export function inputObject(input: unknown): Uint8Array {
  // § e1dc4dar4.a@6sk3g
  const object = input === undefined ? {} : input;
  if (typeof object !== "object" || object === null || Array.isArray(object)) {
    throw new InvalidArgument(`an input is a JSON object, not ${JSON.stringify(object)}`);
  }
  // § one aaev3rs1q@evgsa
  return encodeCanonical(object, "the input");
}

export function send(
  shared: Shared,
  runId: string,
  name: string,
  value: unknown,
  options: MessageOptions,
): Promise<void> {
  onlyOptions(options, ["id"], "send");
  const call = makeCall({
    case: "send",
    value: {
      run: checkKey("run id", runId),
      name: checkSentName(name),
      value: jsonValue(encodeJson(value, "the message")),
      ...(options.id === undefined ? {} : { id: checkKey("message id", options.id) }),
    },
  });
  return shared.future(call, () => undefined);
}

/** Tells the clock when `result`, a result of `chain`, gave the code its failure. */
function failureRead<T>(shared: Shared, chain: string, result: Promise<T>): Promise<T> {
  result.catch((e: unknown) => {
    if (e instanceof RunFailed && e.status === "failed") shared.clock.failureRead(chain);
  });
  return result;
}

export interface ChainResultOptions<T> {
  /** `WaitTimeout` once it passed with the chain not ended. */
  readonly timeout?: Duration;
  /** Turns the chain's value, JSON, into the value the code sees. */
  readonly decode?: (value: unknown) => T;
}

export function chainResult<T>(
  shared: Shared,
  chain: string,
  options: ChainResultOptions<T>,
): Promise<T> {
  onlyOptions(options, ["timeout", "decode"], "result");
  const { timeout, decode } = options;
  if (timeout !== undefined && !(timeout instanceof Duration)) {
    throw new InvalidArgument(`timeout is a Duration, not ${timeout}`);
  }
  if (decode !== undefined && typeof decode !== "function") {
    throw new InvalidArgument("decode is a function");
  }
  const call = makeCall({
    case: "chainResult",
    value: {
      chain: checkKey("run id", chain),
      ...(timeout === undefined ? {} : { timeoutMs: BigInt(timeout.ms) }),
    },
  });
  const decoded = (value: layout.Value | undefined) => {
    const json = decodeValue(value);
    return decode === undefined ? json : decode(json);
  };
  return failureRead(shared, chain, shared.future<T>(call, decoded));
}

/** A run started by this one: its `result`, whether it is `done`, and messages to it. */
export class Child<O = unknown> {
  /** The child's chain, the id a client gives `sub.run`. */
  readonly chain: string;
  readonly #shared: Shared;

  constructor(shared: Shared, chain: string) {
    this.#shared = shared;
    this.chain = chain;
  }

  toString(): string {
    return `child ${this.chain}`;
  }

  /** Whether the child's chain ended, for a `waitUntil` over several things. */
  get done(): boolean {
    return this.#shared.ended.has(this.chain);
  }

  /** The value the child's chain ended with; `RunFailed` if it failed or was cancelled. */
  result(): Promise<O> {
    const call = makeCall({ case: "childResult", value: this.chain });
    return failureRead(this.#shared, this.chain, this.#shared.future<O>(call, decodeValue));
  }

  /** Sends the child a message, delivered once per `id`. */
  send(name: string, value?: unknown, options: MessageOptions = {}): Promise<void> {
    return send(this.#shared, this.chain, name, value, options);
  }

  /** Cancels the child as a client's `run.cancel` does; resolves once the outcome is recorded,
   * before the child ends: `result()` then rejects with `RunFailed`, cancelled. */
  cancel(reason: string): Promise<void> {
    if (typeof reason !== "string") {
      throw new InvalidArgument(`a reason is a string, not ${reason}`);
    }
    const call = makeCall({ case: "cancel", value: { run: this.chain, reason } });
    return this.#shared.future(call, () => undefined);
  }
}

export function start<I, O>(
  shared: Shared,
  workflow: Workflow<I, O>,
  input: I | undefined,
  options: ChildOptions,
): Promise<Child<O>> {
  // § bxb0ygj7m@z3drd
  if (!(workflow instanceof Workflow)) {
    throw new InvalidArgument(`start takes a workflow, not ${workflow}`);
  }
  onlyOptions(options, ["id", "budget", "abandon"], "start");
  const { abandon = false } = options;
  if (typeof abandon !== "boolean")
    throw new InvalidArgument(`abandon is a boolean, not ${abandon}`);
  const w = workflow as AnyWorkflow;
  const budget = effectiveBudget(options.budget, w.budget);
  const call = makeCall({
    case: "start",
    value: {
      workflow: w.name,
      ...(w.version === undefined ? {} : { version: w.version }),
      // § one aaev3rs1q.b@3phfj
      input: jsonValue(inputObject(input)),
      ...(options.id === undefined ? {} : { id: checkKey("run id", options.id) }),
      ...(budget === undefined ? {} : { budget }),
      abandon,
    },
  });
  return Promise.resolve(new Child<O>(shared, shared.expect(call, "run")));
}
