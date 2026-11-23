// A handle on a chain (I-17): `send`, `result`, `status`, `read`, `history`, `retry` and `cancel`
// act on its last run.
import { create } from "@bufbuild/protobuf";
import { type Budget, budgetProto } from "./cost.ts";
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
import { checkKey, checkSentName } from "./names.ts";
import { ObserverRaised } from "./observe.ts";
import { outcomeOf, type runPlan } from "./plan.ts";
import {
  decodeValue,
  encodeJson,
  failureFrom,
  jsonValue,
  layout,
  parseJson,
  plan,
} from "./proto.ts";
import { listingChanged } from "./stores/s3.ts";
import { type Store, StoreFailure } from "./stores/store.ts";
import type { AnyWorkflow } from "./workflow.ts";

export type RunStatus =
  | "pending"
  | "running"
  | "suspended"
  | "blocked"
  | "completed"
  | "failed"
  | "cancelled";

const ENDS = new Map<layout.Status, RunStatus>([
  [layout.Status.COMPLETED, "completed"],
  [layout.Status.FAILED, "failed"],
  [layout.Status.CANCELLED, "cancelled"],
]);

const STATUSES: (RunStatus | undefined)[] = [
  undefined,
  "pending",
  "running",
  "suspended",
  "completed",
  "failed",
  "cancelled",
  undefined,
  "blocked",
];

/** An event of a run's history, as its replay reads it. */
export interface HistoryEvent {
  /** The run whose segment recorded it: the chain's last run, or a failed run it resumes. */
  readonly run: string;
  readonly segment: number;
  /** When its segment was committed. */
  readonly at: Date;
  /** `step`, `step_retry`, `step_failed`, `timer`, `fired`, `message`, `child`, `sent`,
   * `published`, `budget_exceeded`, `patched`, `completed`, `failed`, `cancelled` or
   * `continued`. */
  readonly kind: string;
  /** Its fields: times in RFC 3339, failures as `{type, message, retryable, details}`, values as
   * the code gave them, raw bytes as `{$bytes: <base64>}`. */
  readonly data: Record<string, unknown>;
}

function historyEvent(event: plan.HistoryEvent): HistoryEvent {
  if (event.at === undefined) throw new ProtocolError(`event of ${event.run} without its time`);
  return {
    run: event.run,
    segment: Number(event.segment),
    at: new Date(Number(event.at.ms)),
    kind: event.kind,
    data: parseJson(event.data) as Record<string, unknown>,
  };
}

/** What a handle needs of its namespace. */
export interface Namespace {
  readonly store: Store;
  plan(input: plan.PlanInput): ReturnType<typeof runPlan>;
  /** Wakes the workers of this process serving `workflow`. */
  wake(workflow: string): void;
  /** Rejected with the error of the next worker of the namespace that dies. */
  died(): Promise<never>;
  now(): layout.Time;
  ulid(ms: number): string;
}

/** `listingChanged` until `signal`, its failure the caller's `StoreError`. */
async function changed(
  store: Store,
  key: string,
  etag: string | undefined,
  signal: AbortSignal,
): Promise<string> {
  try {
    return await listingChanged(store, key, etag, signal);
  } catch (e) {
    if (e instanceof ObserverRaised) throw e.error;
    throw e instanceof StoreFailure ? new StoreError(e.message) : e;
  }
}

export interface SendOptions {
  /** Delivered once per id; a ULID when none is given. */
  readonly id?: string;
}

export interface RetryOptions {
  /** The retry's budget; the ended run's when not given. */
  readonly budget?: Budget;
  /** A reset: how many events of the last run's `history()` the new run replays, from the
   * first, for a run ended completed, failed or cancelled. */
  readonly keep?: number;
}

export interface ResultOptions {
  /** `WaitTimeout` once it passes first. */
  readonly timeout?: Duration;
  /** The longest wait between two polls, whose interval doubles up to it, 30 s when not given;
   * a store whose listings wait does not poll. */
  readonly maxInterval?: Duration;
}

/** The longest wait between two polls of `result`, in ms. */
export function maxIntervalMs(maxInterval: unknown): number {
  // § one y025f8qen@v0rgp
  if (maxInterval === undefined) return 30_000;
  if (!(maxInterval instanceof Duration) || maxInterval.ms <= 0) {
    throw new InvalidArgument(`maxInterval is a positive Duration, not ${maxInterval}`);
  }
  return maxInterval.ms;
}

function text(value: unknown, what: string): string {
  if (typeof value !== "string") throw new InvalidArgument(`${what} is a string, not ${value}`);
  return value;
}

/** A send's outcome to `chain`: the workers serving its run woken when it was accepted, else its
 * error thrown. */
export function sentOutcome(
  outcome: plan.Sent,
  chain: string,
  wake: (workflow: string) => void,
): void {
  const sent = outcome.sent;
  switch (sent.case) {
    case "accepted":
      wake(sent.value.workflow);
      return;
    case "refused": {
      const how = ENDS.get(sent.value.status);
      if (how === undefined) throw new ProtocolError(`send refused by a run ${sent.value.status}`);
      throw new RunEnded(`run ${sent.value.run} has ended, ${how}`);
    }
    case "noChain":
      throw new RunNotFound(chain);
    case "failed":
      throw new StoreError(sent.value.message);
    default:
      throw new ProtocolError("empty send outcome");
  }
}

/** The chain's end, or its last run while it has none. */
export type Ended =
  | { done: false; run: string; resultKey: string }
  | { done: true; value: unknown };

let endedOf: (run: Run, last: string | undefined) => Promise<Ended>;

/** The chain's end, or its last run while it has none, read once. */
export function ended(run: Run, last?: string): Promise<Ended> {
  return endedOf(run, last);
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

  static {
    endedOf = (run, last) => run.#ended(last);
  }

  toString(): string {
    return `run ${this.chain} of ${this.workflow.name}`;
  }

  #notFound(): RunNotFound {
    return new RunNotFound(this.chain);
  }

  /** Sends a message, delivered once per `id`. */
  async send(name: string, value?: unknown, options: SendOptions = {}): Promise<void> {
    onlyOptions(options, ["id"], "send");
    const at = this.#ns.now();
    const chain = checkKey("run id", this.chain);
    const id = options.id === undefined ? undefined : checkKey("message id", options.id);
    // § wtwx1e769@1kfrb
    const ord = this.#ns.ulid(Number(at.ms));
    const send = {
      prefix: "",
      chain,
      name: checkSentName(name),
      // § one h8wg39cct.a@mjvy8
      value: jsonValue(encodeJson(value, "the message")),
      // § one wtwx1e769.a@wm9p6
      id: id ?? ord,
      now: at,
      ord,
    };
    // § end wtwx1e769
    await this.#sent(create(plan.PlanInputSchema, { plan: { case: "send", value: send } }));
  }

  /** Cancels the run: it throws `Cancelled` at every call it is blocked on. */
  async cancel(reason: string): Promise<void> {
    const at = this.#ns.now();
    const chain = checkKey("run id", this.chain);
    const why = text(reason, "a reason");
    // § wtwx1e769.c@mn8r5
    const ord = this.#ns.ulid(Number(at.ms));
    const cancel = {
      prefix: "",
      chain,
      reason: why,
      id: ord,
      now: at,
      ord,
    };
    // § end wtwx1e769.c
    await this.#sent(create(plan.PlanInputSchema, { plan: { case: "cancel", value: cancel } }));
  }

  async #sent(input: plan.PlanInput): Promise<void> {
    const sent = outcomeOf("send", await this.#ns.plan(input), "sent");
    sentOutcome(sent, this.chain, (workflow) => this.#ns.wake(workflow));
  }

  async status(): Promise<RunStatus> {
    const value = {
      prefix: "",
      chain: checkKey("run id", this.chain),
      now: this.#ns.now(),
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

  /** The events of the chain's last run as its replay reads them, a retry's including those of
   * the failed run it resumes. */
  async history(): Promise<HistoryEvent[]> {
    const value = { prefix: "", chain: checkKey("run id", this.chain) };
    const input = create(plan.PlanInputSchema, { plan: { case: "history", value } });
    const history = outcomeOf("history", await this.#ns.plan(input), "history").history;
    if (history.case === "noChain") throw this.#notFound();
    if (history.case === "failed") throw new StoreError(history.value.message);
    if (history.case !== "events") throw new ProtocolError("empty history outcome");
    return history.value.events.map(historyEvent);
  }

  /** Retries the chain's last run, which failed: a new run replays its history without the
   * failure, so its successful steps keep their values and the failed step runs again. It keeps
   * the failed run's budget and its cost so far, unless `budget` gives another; a second retry of
   * the same failed run with another budget is `InvalidArgument`. */
  async retry(options: RetryOptions = {}): Promise<void> {
    onlyOptions(options, ["budget", "keep"], "retry");
    const { keep } = options;
    if (keep !== undefined && !(Number.isSafeInteger(keep) && keep >= 0)) {
      throw new InvalidArgument(`keep is a non-negative integer, not ${keep}`);
    }
    const value = {
      prefix: "",
      chain: checkKey("run id", this.chain),
      now: this.#ns.now(),
      ...(options.budget === undefined ? {} : { budget: budgetProto(options.budget) }),
      ...(keep === undefined ? {} : { keep: BigInt(keep) }),
    };
    const input = create(plan.PlanInputSchema, { plan: { case: "retry", value } });
    const retried = outcomeOf("retry", await this.#ns.plan(input), "retried").retried;
    switch (retried.case) {
      case "created":
      case "existing":
        this.#ns.wake(retried.value.workflow);
        return;
      case "notRetryable":
        throw new InvalidArgument(retried.value.message);
      case "noChain":
        throw this.#notFound();
      case "failed":
        throw new StoreError(retried.value.message);
      default:
        throw new ProtocolError("empty retry outcome");
    }
  }

  /** The value the chain ended with, waited for through its continuations; throws `RunFailed`
   * if it failed or was cancelled, and the error of a worker of this namespace that dies
   * meanwhile. Polls double their interval up to `maxInterval`. */
  async result(options: ResultOptions = {}): Promise<O> {
    onlyOptions(options, ["timeout", "maxInterval"], "result");
    const { timeout } = options;
    if (timeout !== undefined && !(timeout instanceof Duration)) {
      throw new InvalidArgument(`timeout is a Duration, not ${timeout}`);
    }
    const cap = maxIntervalMs(options.maxInterval);
    const deadline = timeout === undefined ? undefined : performance.now() + timeout.ms;
    const died = this.#ns.died();
    // § y025f8qen@jr8ks
    let wait = Math.min(10, cap);
    // § end y025f8qen
    let last: string | undefined;
    let etag: string | undefined;
    for (;;) {
      // § y025f8qen@w5ah6
      // § y025f8qen.b@0gd9w
      const ended = await this.#ended(last);
      if (ended.done) return ended.value as O;
      // § end y025f8qen.b
      last = ended.run;
      const left = deadline === undefined ? Number.POSITIVE_INFINITY : deadline - performance.now();
      if (left <= 0) throw new WaitTimeout(`run ${this.chain} did not end within ${timeout}`);
      const { store } = this.#ns;
      // § y025f8qen.e@sf3xe
      const pause = delay(store.capabilities.wait ? left : Math.min(wait, left));
      // Ends the waiting listings once the race is settled, as Python cancels them.
      const settled = new AbortController();
      const ends: Promise<unknown>[] = [pause.promise, died];
      if (store.capabilities.wait) {
        // The first listing waits for nothing: it takes the etag the next one waits on, and the
        // read after it sees any commit that landed before it.
        // § one hnjtggsx7.a@t0qnf
        const listed = changed(store, ended.resultKey, etag, settled.signal);
        ends.push(
          listed.then((next) => {
            etag = next;
          }),
        );
      } else wait = Math.min(wait * 2, cap);
      // § end y025f8qen.e
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
      chain: checkKey("run id", this.chain),
      run: last,
    };
    const input = create(plan.PlanInputSchema, { plan: { case: "result", value } });
    const result = outcomeOf("result", await this.#ns.plan(input), "result").result;
    switch (result.case) {
      case "notEnded":
        return { done: false, run: result.value.run, resultKey: result.value.resultKey };
      // § one hnjtggsx7.a@035gr
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
      const error = failureFrom(r.error);
      throw new RunFailed(run, "failed", error, runFailedCause(error.type, error.details));
    }
    if (r.status === layout.Status.CANCELLED) {
      const failure = { type: "Cancelled", message: r.reason, retryable: false };
      throw new RunFailed(run, "cancelled", failure, undefined);
    }
    throw new ProtocolError(`result of ${run} with status ${r.status}`);
  }
}
