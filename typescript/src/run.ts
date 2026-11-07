// A handle on a chain (I-17): `send`, `result`, `status`, `read` and `cancel` act on its last run.
import { create } from "@bufbuild/protobuf";
import { Duration, delay } from "./durations.ts";
import {
  InvalidArgument,
  onlyOptions,
  ProtocolError,
  RunEnded,
  RunFailed,
  RunNotFound,
  runFailedCause,
  StoreError,
  WaitTimeout,
} from "./errors.ts";
import { checkKey, checkName } from "./names.ts";
import { outcomeOf, type runPlan } from "./plan.ts";
import { decodeValue, encodeJson, jsonValue, layout, plan } from "./proto.ts";
import { RETENTION_MIN } from "./settings.ts";
import { resultCreated } from "./stores/s3.ts";
import { type Store, StoreFailure } from "./stores/store.ts";
import type { AnyWorkflow } from "./workflow.ts";

export type RunStatus = "pending" | "running" | "suspended" | "completed" | "failed" | "cancelled";

const STATUSES: (RunStatus | undefined)[] = [
  undefined,
  "pending",
  "running",
  "suspended",
  "completed",
  "failed",
  "cancelled",
];

/** What a handle needs of its namespace. */
export interface Namespace {
  readonly partitions: number;
  readonly store: Store;
  plan(input: plan.PlanInput): ReturnType<typeof runPlan>;
  wake(): void;
  /** Rejected with the error of the next worker of the namespace that dies. */
  died(): Promise<never>;
  now(): layout.Time;
  ulid(ms: number): string;
}

/** A wait follows its chain from the first run again this often, as polling one run's `result`
 * never sees that run collected (I-17). */
const REFOLLOW_MS = RETENTION_MIN.ms / 2;

/** When a run was collected, from a `NoChain` or a `RunExists`. */
export function collectedUntil(t: layout.Time | undefined): Date | undefined {
  return t === undefined ? undefined : new Date(Number(t.ms));
}

/** `resultCreated` until `deadline` or `signal`, its failure the caller's `StoreError`. */
async function created(
  store: Store,
  key: string,
  deadline: number,
  signal: AbortSignal,
): Promise<void> {
  try {
    await resultCreated(store, key, deadline, signal);
  } catch (e) {
    throw e instanceof StoreFailure ? new StoreError(e.message) : e;
  }
}

export interface SendOptions {
  /** Delivered once per id; a ULID when none is given. */
  readonly id?: string;
}

export interface ResultOptions {
  /** `WaitTimeout` once it passes first. */
  readonly timeout?: Duration;
}

function text(value: unknown, what: string): string {
  if (typeof value !== "string") throw new InvalidArgument(`${what} is a string, not ${value}`);
  return value;
}

/** The chain's end, or its last run while it has none. */
export type Ended =
  | { done: false; run: string; resultKey: string }
  | { done: true; value: unknown };

let endedOf: (run: Run, last: string | undefined) => Promise<Ended>;
let refollowOf: (run: Run, ms: number) => void;

/** Has `run`'s `result` waits follow the chain again every `ms`: for tests. */
export function refollow(run: Run, ms: number): void {
  refollowOf(run, ms);
}

/** The chain's end, or its last run while it has none, read once. */
export function ended(run: Run, last?: string): Promise<Ended> {
  return endedOf(run, last);
}

/** A handle on a chain: `run.chain`; its actions reach the chain's last run. */
export class Run<O = unknown> {
  readonly workflow: AnyWorkflow;
  readonly chain: string;
  readonly #ns: Namespace;
  #refollow = REFOLLOW_MS;

  constructor(ns: Namespace, workflow: AnyWorkflow, chain: string) {
    this.#ns = ns;
    this.workflow = workflow;
    this.chain = chain;
  }

  static {
    endedOf = (run, last) => run.#ended(last);
    refollowOf = (run, ms) => {
      run.#refollow = ms;
    };
  }

  toString(): string {
    return `run ${this.chain} of ${this.workflow.name}`;
  }

  #notFound(noChain: plan.NoChain): RunNotFound {
    return new RunNotFound(this.chain, collectedUntil(noChain.collectedUntil));
  }

  /** Sends a message, delivered once per `id`. */
  async send(name: string, value?: unknown, options: SendOptions = {}): Promise<void> {
    onlyOptions(options, ["id"], "send");
    const at = this.#ns.now();
    const chain = checkKey("run id", this.chain);
    const id = options.id === undefined ? undefined : checkKey("message id", options.id);
    const ord = this.#ns.ulid(Number(at.ms));
    const send = {
      prefix: "",
      partitions: this.#ns.partitions,
      chain,
      name: checkName("message name", name),
      value: jsonValue(encodeJson(value, "the message")),
      id: id ?? ord,
      now: at,
      ord,
    };
    await this.#sent(create(plan.PlanInputSchema, { plan: { case: "send", value: send } }));
  }

  /** Cancels the run: it throws `Cancelled` at every call it is blocked on. */
  async cancel(reason: string): Promise<void> {
    const at = this.#ns.now();
    const chain = checkKey("run id", this.chain);
    const why = text(reason, "a reason");
    const ord = this.#ns.ulid(Number(at.ms));
    const cancel = {
      prefix: "",
      partitions: this.#ns.partitions,
      chain,
      reason: why,
      id: ord,
      now: at,
      ord,
    };
    await this.#sent(create(plan.PlanInputSchema, { plan: { case: "cancel", value: cancel } }));
  }

  async #sent(input: plan.PlanInput): Promise<void> {
    const sent = outcomeOf("send", await this.#ns.plan(input), "sent").sent;
    switch (sent.case) {
      case "accepted":
        this.#ns.wake();
        return;
      case "refused":
        throw new RunEnded(`run ${sent.value.run} has ended`);
      case "noChain":
        throw this.#notFound(sent.value);
      case "failed":
        throw new StoreError(sent.value.message);
      default:
        throw new ProtocolError("empty send outcome");
    }
  }

  async status(): Promise<RunStatus> {
    const value = {
      prefix: "",
      partitions: this.#ns.partitions,
      chain: checkKey("run id", this.chain),
      now: this.#ns.now(),
    };
    const input = create(plan.PlanInputSchema, { plan: { case: "status", value } });
    const state = outcomeOf("status", await this.#ns.plan(input), "status").state;
    if (state.case === "noChain") throw this.#notFound(state.value);
    if (state.case === "failed") throw new StoreError(state.value.message);
    const status = state.case === "status" ? STATUSES[state.value.status] : undefined;
    if (status === undefined) throw new ProtocolError("unspecified run status");
    return status;
  }

  /** The last value published under `key` by any run of the chain, `undefined` if none. */
  async read<T = unknown>(key: string): Promise<T | undefined> {
    const value = {
      prefix: "",
      partitions: this.#ns.partitions,
      chain: checkKey("run id", this.chain),
      key: checkKey("published key", key),
    };
    const input = create(plan.PlanInputSchema, { plan: { case: "read", value } });
    const read = outcomeOf("read", await this.#ns.plan(input), "read").read;
    if (read.case === "none") return undefined;
    if (read.case === "failed") throw new StoreError(read.value.message);
    if (read.case !== "value") throw new ProtocolError("empty read outcome");
    return decodeValue(read.value) as T;
  }

  /** The value the chain ended with, waited for through its continuations; throws `RunFailed`
   * if it failed or was cancelled, and the error of a worker of this namespace that dies
   * meanwhile. */
  async result(options: ResultOptions = {}): Promise<O> {
    onlyOptions(options, ["timeout"], "result");
    const { timeout } = options;
    if (timeout !== undefined && !(timeout instanceof Duration)) {
      throw new InvalidArgument(`timeout is a Duration, not ${timeout}`);
    }
    const deadline = timeout === undefined ? undefined : performance.now() + timeout.ms;
    const died = this.#ns.died();
    let wait = 10;
    let last: string | undefined;
    let followed = performance.now();
    for (;;) {
      if (performance.now() - followed >= this.#refollow) {
        last = undefined;
        followed = performance.now();
      }
      const ended = await this.#ended(last);
      if (ended.done) return ended.value as O;
      last = ended.run;
      const left = deadline === undefined ? Number.POSITIVE_INFINITY : deadline - performance.now();
      if (left <= 0) throw new WaitTimeout(`run ${this.chain} did not end within ${timeout}`);
      const { store } = this.#ns;
      const held = Math.min(left, followed + this.#refollow - performance.now());
      const pause = delay(store.capabilities.wait ? held : Math.min(wait, left));
      // Ends the waiting listings once the race is settled, as Python cancels them.
      const settled = new AbortController();
      const ends = [pause.promise, died];
      if (store.capabilities.wait)
        ends.push(created(store, ended.resultKey, performance.now() + held, settled.signal));
      else wait = Math.min(wait * 2, 30_000);
      try {
        await Promise.race(ends);
      } finally {
        pause.cancel();
        settled.abort();
      }
    }
  }

  /** The chain's end, or its last run while it has none: `last` is the run the poll before
   * answered, whose `result` alone the core then gets. */
  async #ended(last: string | undefined): Promise<Ended> {
    const value = {
      prefix: "",
      partitions: this.#ns.partitions,
      chain: checkKey("run id", this.chain),
      run: last,
    };
    const input = create(plan.PlanInputSchema, { plan: { case: "result", value } });
    const result = outcomeOf("result", await this.#ns.plan(input), "result").result;
    switch (result.case) {
      case "notEnded":
        return { done: false, run: result.value.run, resultKey: result.value.resultKey };
      case "noChain":
        throw this.#notFound(result.value);
      case "failed":
        throw new StoreError(result.value.message);
      case "ended":
        break;
      default:
        throw new ProtocolError("empty result outcome");
    }
    const { run, result: r } = result.value;
    if (r === undefined) throw new ProtocolError(`ended ${run} without its result`);
    if (r.status === layout.Status.COMPLETED) return { done: true, value: decodeValue(r.value) };
    if (r.status === layout.Status.FAILED && r.error !== undefined) {
      const { type, message, retryable, details } = r.error;
      const cause = runFailedCause(type, details === undefined ? undefined : decodeValue(details));
      throw new RunFailed(run, "failed", { type, message, retryable }, cause);
    }
    if (r.status === layout.Status.CANCELLED) {
      const failure = { type: "Cancelled", message: r.reason, retryable: false };
      throw new RunFailed(run, "cancelled", failure, undefined);
    }
    throw new ProtocolError(`result of ${run} with status ${r.status}`);
  }
}
