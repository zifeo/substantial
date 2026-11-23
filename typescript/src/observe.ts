// What a `Substantial` tells its observer (I-29): store operations, step attempts, replays, scans,
// blocked runs, history limits neared and runs left for a clock behind.
import { type Amount, Prices, priceOf } from "./cost.ts";
import { ConfigError, type Failure } from "./errors.ts";
import { measured, Sent } from "./stores/attempts.ts";
import type {
  Capabilities,
  Cas,
  Create,
  Listing,
  ListOptions,
  Store,
  StoreObject,
} from "./stores/store.ts";

/** A store operation of a client or a worker, once it answered. */
export class StoreOp {
  readonly op: "create" | "cas" | "get" | "list" | "delete";
  readonly seconds: number;
  /** The requests the adapter sent, its retries included. */
  readonly attempts: number;
  readonly outcome:
    | "created"
    | "exists"
    | "written"
    | "mismatch"
    | "found"
    | "missing"
    | "listed"
    | "deleted"
    | "failed";
  /** `attempts` requests at the price of the operation's kind; `undefined` without prices. */
  readonly cost: Amount | undefined;

  constructor(
    op: StoreOp["op"],
    seconds: number,
    attempts: number,
    outcome: StoreOp["outcome"],
    cost: Amount | undefined,
  ) {
    this.op = op;
    this.seconds = seconds;
    this.attempts = attempts;
    this.outcome = outcome;
    this.cost = cost;
  }
}

/** A step attempt a worker ran, once it ended: `retry` is a retryable error with attempts left,
 * `failed` no attempt left or a permanent error, `abandoned` an attempt interrupted, its outcome
 * not recorded. */
export class Attempt {
  readonly workflow: string;
  readonly step: string;
  readonly attempt: number;
  readonly seconds: number;
  readonly outcome: "ok" | "retry" | "failed" | "timeout" | "abandoned";

  constructor(
    workflow: string,
    step: string,
    attempt: number,
    seconds: number,
    outcome: Attempt["outcome"],
  ) {
    this.workflow = workflow;
    this.step = step;
    this.attempt = attempt;
    this.seconds = seconds;
    this.outcome = outcome;
  }
}

/** A replay a worker ran, once it ended: `idle` found nothing new, `dropped` lost a race, or was
 * stopped or shut down. */
export class Replay {
  readonly workflow: string;
  /** How late it started after the earliest due entry it serves; `undefined` when it serves
   * none. */
  readonly delay: number | undefined;
  readonly seconds: number;
  readonly end: "suspended" | "ended" | "idle" | "dropped";

  constructor(workflow: string, delay: number | undefined, seconds: number, end: Replay["end"]) {
    this.workflow = workflow;
    this.delay = delay;
    this.seconds = seconds;
    this.end = end;
  }
}

/** A scan listed the due entries of a workflow, `due` of them due. */
export class Scan {
  readonly workflow: string;
  readonly due: number;

  constructor(workflow: string, due: number) {
    this.workflow = workflow;
    this.due = due;
  }
}

/** A replay a worker dropped as its code raised an error that is no end, or diverged from the
 * history: the run is tried again after a backoff. */
export class Blocked {
  readonly workflow: string;
  readonly run: string;
  readonly error: Failure;

  constructor(workflow: string, run: string, error: Failure) {
    this.workflow = workflow;
    this.run = run;
    this.error = error;
  }
}

/** A commit took a run past the warning share of one of its history limits, `history_bytes`,
 * `history_events`, `replay_inbox` or `seen`: `used` of `max`. */
export class NearLimit {
  readonly workflow: string;
  readonly run: string;
  readonly limit: string;
  readonly used: number;
  readonly max: number;

  constructor(workflow: string, run: string, limit: string, used: number, max: number) {
    this.workflow = workflow;
    this.run = run;
    this.limit = limit;
    this.used = used;
    this.max = max;
  }
}

/** A worker left the chain `run` alone, `current` its current run, as its clock, `now`, is behind
 * the run's last segment, made at `segmentAt`, by more than `claim_skew`. */
export class Skewed {
  readonly run: string;
  readonly current: string;
  readonly segmentAt: Date;
  readonly now: Date;

  constructor(run: string, current: string, segmentAt: Date, now: Date) {
    this.run = run;
    this.current = current;
    this.segmentAt = segmentAt;
    this.now = now;
  }
}

/** An event of a `Substantial`, given to its `observe`. */
export type Observed = StoreOp | Attempt | Replay | Scan | Blocked | NearLimit | Skewed;

/** The error of an observer, carried past the catch-alls of the store's path to where it stops
 * the worker or fails the client call. */
export class ObserverRaised extends Error {
  readonly error: unknown;

  constructor(error: unknown) {
    super("an observer raised");
    this.error = error;
  }
}

/** The prices and the observer of a `Substantial`, which its workers take. */
export class Metering {
  readonly prices: Prices | undefined;
  readonly observe: ((event: Observed) => unknown) | undefined;

  constructor(prices: unknown, observe: unknown) {
    if (prices !== undefined && !(prices instanceof Prices)) {
      throw new ConfigError(`prices is a Prices, not ${String(prices)}`);
    }
    if (observe !== undefined && typeof observe !== "function") {
      throw new ConfigError(`observe is a function, not ${String(observe)}`);
    }
    this.prices = prices;
    this.observe = observe as Metering["observe"];
  }

  tell(event: Observed): void {
    this.observe?.(event);
  }

  /** `store`, its operations observed when there is an observer. */
  wrap(store: Store): Store {
    return this.observe === undefined
      ? store
      : new ObservingStore(store, this.observe, this.prices);
  }
}

/** A store whose every operation is told to an observer once it answered. */
class ObservingStore implements Store {
  readonly #inner: Store;
  readonly #observe: (event: Observed) => unknown;
  readonly #prices: Prices | undefined;

  constructor(inner: Store, observe: (event: Observed) => unknown, prices: Prices | undefined) {
    this.#inner = inner;
    this.#observe = observe;
    this.#prices = prices;
  }

  get capabilities(): Capabilities {
    return this.#inner.capabilities;
  }

  async #observed<T>(
    op: StoreOp["op"],
    call: () => Promise<T>,
    outcome: (result: T) => StoreOp["outcome"],
  ): Promise<T> {
    // § yp7fv5sa7.a@8jr8k
    const started = performance.now();
    const sent = new Sent();
    let result: T;
    try {
      result = await measured(sent, call);
    } catch (e) {
      this.#tell(op, started, sent.n, "failed");
      throw e;
    }
    this.#tell(op, started, sent.n, outcome(result));
    return result;
  }

  #tell(op: StoreOp["op"], started: number, attempts: number, outcome: StoreOp["outcome"]): void {
    const seconds = (performance.now() - started) / 1000;
    // § vg9yp9xsf@nmxcc
    const cost = this.#prices === undefined ? undefined : priceOf(this.#prices, op, attempts);
    // § one yp7fv5sa7.a@93r1d
    try {
      this.#observe(new StoreOp(op, seconds, attempts, outcome, cost));
    } catch (e) {
      // § yp7fv5sa7.b@xszt0
      throw new ObserverRaised(e);
    }
  }

  create(key: string, data: Uint8Array, expiry: number | undefined): Promise<Create> {
    return this.#observed(
      "create",
      () => this.#inner.create(key, data, expiry),
      (c) => (c === "exists" ? "exists" : "created"),
    );
  }

  cas(key: string, data: Uint8Array, etag: string, expiry: number | undefined): Promise<Cas> {
    return this.#observed(
      "cas",
      () => this.#inner.cas(key, data, etag, expiry),
      (c) => (c === "mismatch" ? "mismatch" : "written"),
    );
  }

  checkExpiry(days: readonly number[]): Promise<void> {
    return this.#inner.checkExpiry(days);
  }

  get(key: string): Promise<StoreObject | undefined> {
    const found = (data: StoreObject | undefined) => (data === undefined ? "missing" : "found");
    return this.#observed("get", () => this.#inner.get(key), found);
  }

  list(prefix: string, options: ListOptions): Promise<Listing> {
    return this.#observed(
      "list",
      () => this.#inner.list(prefix, options),
      () => "listed",
    );
  }

  delete(key: string): Promise<void> {
    return this.#observed(
      "delete",
      () => this.#inner.delete(key),
      () => "deleted",
    );
  }

  open(): Promise<void> {
    return this.#inner.open();
  }

  close(): Promise<void> {
    return this.#inner.close();
  }
}
