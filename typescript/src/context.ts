// What workflow code calls (I-6 to I-14); one context per replay.
import {
  type Child,
  type ChildOptions,
  inputObject,
  type MessageOptions,
  send,
  start,
} from "./child.ts";
import { Duration } from "./durations.ts";
import { Cancelled, ContinuedAsNew, InvalidArgument, ProtocolError } from "./errors.ts";
import { Iteration } from "./iterate.ts";
import { type Log, runLog } from "./log.ts";
import { type HandleOptions, handle, handleAll } from "./messages.ts";
import { decodeValue, encodeJson, jsonValue, type layout, time } from "./proto.ts";
import { rollback, saga } from "./saga.ts";
import { makeCall, type Shared } from "./scheduler.ts";
import { callStep, checkOptions, type DeclaredStepOptions, type StepOptions } from "./step.ts";
import { Workflow } from "./workflow.ts";

export interface ReceiveOptions<T> {
  /** Turns the message's JSON into the value the code sees. */
  readonly decode?: (value: unknown) => T;
}

/** The context a workflow's code calls, its first argument. */
export interface Context {
  /** The run's chain, the id a client gives `sub.run` and every run of the chain shares. */
  readonly chain: string;
  /** The run's logger, muted while a replay walks the recorded history. */
  readonly log: Log;
  /** Runs `fn` as the step `name`. */
  step<R, T = Awaited<R>>(name: string, fn: () => R, options?: StepOptions<T>): Promise<T>;
  /** The time, recorded. */
  now(): Promise<Date>;
  /** A number in [0, 1), recorded. */
  random(): Promise<number>;
  /** A UUID, recorded. */
  uuid(): Promise<string>;
  /** Sleeps for `duration`, fixed when it first runs; a replay never sleeps again. */
  sleep(duration: Duration): Promise<void>;
  /** Sleeps until `at`; a time that has passed fires at once. */
  sleepUntil(at: Date): Promise<void>;
  /** The next message of `name` that no `receive` took yet. */
  receive<T = unknown>(name: string, options?: ReceiveOptions<T>): Promise<T>;
  /** Binds `fn` to the messages of `name`: every one consumed so far, in order, then each new
   * one. */
  handle<T = unknown>(name: string, fn: (value: T) => unknown, options?: HandleOptions<T>): void;
  /** Binds a handler to each name of `handlers` at once: the messages of all those names consumed
   * so far, in the order they arrived, then each new one. */
  handle(handlers: Readonly<Record<string, (value: never) => unknown>>): void;
  /** `true` once `predicate` holds, evaluated after each message, child end and timer; `false`
   * when `timeout` passes first. */
  waitUntil(predicate: () => unknown, options?: WaitOptions): Promise<boolean>;
  /** Sends a message to the last run of the chain `run`, delivered once per `id`. */
  send(run: string, name: string, value?: unknown, options?: MessageOptions): Promise<void>;
  /** Makes `value` the last one published under `key` by the chain (`run.read(key)`). */
  publish(key: string, value: unknown): void;
  /** Runs `fn`; on an error, the undos of the steps called inside, most recent first, then the
   * error again. */
  saga<T>(fn: () => T | PromiseLike<T>): Promise<T>;
  /** Runs the undos of every step so far, most recent first, then ends the run failed with
   * `RolledBack(reason)`; it does not return. */
  rollback(reason: string): Promise<never>;
  /** Starts a child run of `workflow`, under `id`, or an id the core derives from this run's. */
  start<I, O>(workflow: Workflow<I, O>, input?: I, options?: ChildOptions): Promise<Child<O>>;
  /** `for await (const s of c.iterate(init))`: compresses at each boundary past the threshold,
   * carrying the state only. */
  iterate<S extends object>(init: S): AsyncIterableIterator<S>;
  /** Ends the run and starts the next of its chain with `input`, of `workflow` if given; it does
   * not return. */
  continueAsNew<I>(input?: I, options?: ContinueOptions<I>): Promise<never>;
  /** Ends the run cancelled; it does not return. */
  cancel(reason: string): Promise<never>;
}

export interface WaitOptions {
  /** The wait returns `false` once it passes. */
  readonly timeout?: Duration;
}

export interface ContinueOptions<I> {
  /** The next run's workflow, with its version or none; this one, with this run's version, when
   * not given. */
  readonly workflow?: Workflow<I, unknown>;
}

function known(options: object, names: string[], what: string): Record<string, unknown> {
  if (typeof options !== "object" || options === null) {
    throw new InvalidArgument(`${what} options are an object`);
  }
  const unknown = Object.keys(options).filter((name) => !names.includes(name));
  if (unknown.length > 0) {
    throw new InvalidArgument(`unknown ${what} options ${unknown.join(", ")}`);
  }
  return options as Record<string, unknown>;
}

function text(value: unknown, what: string): string {
  if (typeof value !== "string") throw new InvalidArgument(`${what} is a string, not ${value}`);
  return value;
}

function decodeNow(value: layout.Value | undefined): Date {
  const text = decodeValue(value);
  const date = typeof text === "string" ? new Date(text) : undefined;
  if (date === undefined || Number.isNaN(date.getTime())) {
    throw new ProtocolError(`now ${JSON.stringify(text)}`);
  }
  return date;
}

function decodeRandom(value: layout.Value | undefined): number {
  const x = decodeValue(value);
  if (typeof x !== "number" || x < 0 || x >= 1) throw new ProtocolError(`random ${x}`);
  return x;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function decodeUuid(value: layout.Value | undefined): string {
  const text = decodeValue(value);
  if (typeof text !== "string" || !UUID.test(text)) throw new ProtocolError(`uuid ${text}`);
  return text;
}

function decodeNothing(): undefined {
  return undefined;
}

function decodeBool(value: layout.Value | undefined): boolean {
  const x = decodeValue(value);
  if (typeof x !== "boolean") throw new ProtocolError(`a wait answered ${x}`);
  return x;
}

export class RunContext implements Context {
  readonly chain: string;
  readonly log: Log;
  readonly #shared: Shared;

  constructor(shared: Shared) {
    this.#shared = shared;
    this.chain = shared.start.chain;
    this.log = runLog(shared);
  }

  step<R, T = Awaited<R>>(name: string, fn: () => R, options: StepOptions<T> = {}): Promise<T> {
    const opts = options as DeclaredStepOptions<unknown[], unknown>;
    checkOptions(opts);
    return callStep<T>(this.#shared, name, fn, [], opts);
  }

  now(): Promise<Date> {
    return this.#shared.future(makeCall({ case: "now", value: {} }), decodeNow);
  }

  random(): Promise<number> {
    return this.#shared.future(makeCall({ case: "random", value: {} }), decodeRandom);
  }

  uuid(): Promise<string> {
    return this.#shared.future(makeCall({ case: "uuid", value: {} }), decodeUuid);
  }

  sleep(duration: Duration): Promise<void> {
    if (!(duration instanceof Duration)) {
      throw new InvalidArgument(`sleep takes a Duration, not ${duration}`);
    }
    const call = makeCall({ case: "sleepMs", value: BigInt(duration.ms) });
    return this.#shared.future(call, decodeNothing);
  }

  sleepUntil(at: Date): Promise<void> {
    if (!(at instanceof Date) || Number.isNaN(at.getTime())) {
      throw new InvalidArgument(`sleepUntil takes a valid Date, not ${at}`);
    }
    const call = makeCall({ case: "sleepUntil", value: time(at.getTime()) });
    return this.#shared.future(call, decodeNothing);
  }

  receive<T = unknown>(name: string, options: ReceiveOptions<T> = {}): Promise<T> {
    text(name, "a message name");
    const { decode } = known(options, ["decode"], "receive");
    if (decode !== undefined && typeof decode !== "function") {
      throw new InvalidArgument("decode is a function");
    }
    const decoded = (value: layout.Value | undefined) => {
      const json = decodeValue(value);
      return decode === undefined ? json : (decode as (value: unknown) => T)(json);
    };
    return this.#shared.future<T>(makeCall({ case: "receive", value: name }), decoded);
  }

  handle<T = unknown>(
    name: string | Readonly<Record<string, (value: never) => unknown>>,
    fn?: (value: T) => unknown,
    options: HandleOptions<T> = {},
  ): void {
    if (typeof name === "object" && name !== null && !Array.isArray(name)) {
      if (fn !== undefined) {
        throw new InvalidArgument("handle takes an object of handlers alone, without fn");
      }
      handleAll(this.#shared, name);
      return;
    }
    handle(this.#shared, name as string, fn as (value: T) => unknown, options);
  }

  waitUntil(predicate: () => unknown, options: WaitOptions = {}): Promise<boolean> {
    if (typeof predicate !== "function") {
      throw new InvalidArgument(`waitUntil takes a predicate, not ${predicate}`);
    }
    const { timeout } = known(options, ["timeout"], "waitUntil");
    if (timeout !== undefined && !(timeout instanceof Duration)) {
      throw new InvalidArgument(`timeout is a Duration, not ${timeout}`);
    }
    const wait = timeout === undefined ? {} : { timeoutMs: BigInt(timeout.ms) };
    const call = makeCall({ case: "wait", value: wait });
    return this.#shared.future(call, decodeBool, undefined, predicate);
  }

  send(run: string, name: string, value?: unknown, options: MessageOptions = {}): Promise<void> {
    return send(this.#shared, run, name, value, options);
  }

  publish(key: string, value: unknown): void {
    const published = { key: text(key, "a key"), value: jsonValue(encodeJson(value, "the value")) };
    this.#shared.expect(makeCall({ case: "publish", value: published }), "done");
  }

  saga<T>(fn: () => T | PromiseLike<T>): Promise<T> {
    if (typeof fn !== "function") throw new InvalidArgument(`saga takes a function, not ${fn}`);
    return saga(this.#shared, fn);
  }

  rollback(reason: string): Promise<never> {
    return rollback(this.#shared, text(reason, "a reason"));
  }

  start<I, O>(workflow: Workflow<I, O>, input?: I, options: ChildOptions = {}): Promise<Child<O>> {
    return start(this.#shared, workflow, input, options);
  }

  iterate<S extends object>(init: S): AsyncIterableIterator<S> {
    return new Iteration(this.#shared, init);
  }

  async continueAsNew<I>(input?: I, options: ContinueOptions<I> = {}): Promise<never> {
    const { workflow } = known(options, ["workflow"], "continueAsNew");
    const data = inputObject(input);
    if (workflow === undefined) throw new ContinuedAsNew(undefined, undefined, data);
    if (!(workflow instanceof Workflow)) {
      throw new InvalidArgument(`continueAsNew takes a workflow, not ${workflow}`);
    }
    throw new ContinuedAsNew(workflow.name, workflow.version, data);
  }

  async cancel(reason: string): Promise<never> {
    throw new Cancelled(text(reason, "a reason"));
  }
}
