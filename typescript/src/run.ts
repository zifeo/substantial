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
  StoreError,
  WaitTimeout,
} from "./errors.ts";
import { checkKey, checkName } from "./names.ts";
import { outcomeOf, type runPlan, ulid } from "./plan.ts";
import { decodeValue, encodeJson, jsonValue, layout, now, plan } from "./proto.ts";
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
}

/** `resultCreated`, its failure the caller's `StoreError`. */
async function created(store: Store, key: string): Promise<void> {
  try {
    await resultCreated(store, key);
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

/** A handle on a chain: `run.chain`; its actions reach the chain's last run. */
export class Run<O = unknown> {
  readonly workflow: AnyWorkflow;
  readonly chain: string;
  readonly #ns: Namespace;

  constructor(ns: Namespace, workflow: AnyWorkflow, chain: string) {
    this.#ns = ns;
    this.workflow = workflow;
    this.chain = chain;
  }

  toString(): string {
    return `run ${this.chain} of ${this.workflow.name}`;
  }

  #notFound(): RunNotFound {
    return new RunNotFound(`no chain ${this.chain}`);
  }

  /** Sends a message, delivered once per `id`. */
  async send(name: string, value?: unknown, options: SendOptions = {}): Promise<void> {
    onlyOptions(options, ["id"], "send");
    const at = now();
    const send = {
      prefix: "",
      partitions: this.#ns.partitions,
      chain: checkKey("run id", this.chain),
      name: checkName("message name", name),
      value: jsonValue(encodeJson(value, "the message")),
      id: options.id === undefined ? ulid(Number(at.ms)) : checkKey("message id", options.id),
      now: at,
    };
    await this.#sent(create(plan.PlanInputSchema, { plan: { case: "send", value: send } }));
  }

  /** Cancels the run: it throws `Cancelled` at every call it is blocked on. */
  async cancel(reason: string): Promise<void> {
    const at = now();
    const cancel = {
      prefix: "",
      partitions: this.#ns.partitions,
      chain: checkKey("run id", this.chain),
      reason: text(reason, "a reason"),
      id: ulid(Number(at.ms)),
      now: at,
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
        throw this.#notFound();
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
      now: now(),
    };
    const input = create(plan.PlanInputSchema, { plan: { case: "status", value } });
    const state = outcomeOf("status", await this.#ns.plan(input), "status").state;
    if (state.case === "noChain") throw this.#notFound();
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
    for (;;) {
      const ended = await this.#ended(last);
      if (ended.done) return ended.value as O;
      last = ended.run;
      const left = deadline === undefined ? Number.POSITIVE_INFINITY : deadline - performance.now();
      if (left <= 0) throw new WaitTimeout(`run ${this.chain} did not end within ${timeout}`);
      const { store } = this.#ns;
      const pause = delay(store.capabilities.wait ? left : Math.min(wait, left));
      const ends = [pause.promise, died];
      if (store.capabilities.wait) ends.push(created(store, ended.resultKey));
      else wait = Math.min(wait * 2, 30_000);
      try {
        await Promise.race(ends);
      } finally {
        pause.cancel();
      }
    }
  }

  /** The chain's end, or its last run while it has none: `last` is the run the poll before
   * answered, whose `result` alone the core then gets. */
  async #ended(
    last: string | undefined,
  ): Promise<{ done: false; run: string; resultKey: string } | { done: true; value: unknown }> {
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
        throw this.#notFound();
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
      const { type, message, retryable } = r.error;
      throw new RunFailed(run, "failed", { type, message, retryable });
    }
    if (r.status === layout.Status.CANCELLED) {
      throw new RunFailed(run, "cancelled", {
        type: "Cancelled",
        message: r.reason,
        retryable: false,
      });
    }
    throw new ProtocolError(`result of ${run} with status ${r.status}`);
  }
}
