// The host's clock: the time every machine is given, and every wait on it (step timeouts, timers,
// backoffs). A worker takes one; `s.testing` gives it a virtual one.
import type { run } from "./proto.ts";

/** A promise whose state can be read at once: done, then its value or its error. */
export class Future<T = void> {
  #state: { ok: true; value: T } | { ok: false; error: unknown } | undefined;
  #resolve: () => void = () => {};
  /** Resolves once it is done, never rejects. */
  readonly settled: Promise<void> = new Promise((resolve) => {
    this.#resolve = resolve;
  });

  get done(): boolean {
    return this.#state !== undefined;
  }

  resolve(value: T): void {
    if (this.#state !== undefined) return;
    this.#state = { ok: true, value };
    this.#resolve();
  }

  reject(error: unknown): void {
    if (this.#state !== undefined) return;
    this.#state = { ok: false, error };
    this.#resolve();
  }

  /** Its value, or its error thrown; it must be done. */
  result(): T {
    const state = this.#state;
    if (state === undefined) throw new Error("the result of a future not done");
    if (!state.ok) throw state.error;
    return state.value;
  }

  /** A future of what `promise` settles with. */
  static of<T>(promise: Promise<T>): Future<T> {
    const future = new Future<T>();
    promise.then(
      (value) => future.resolve(value),
      (error) => future.reject(error),
    );
    return future;
  }
}

/** What an attempt runs: its body to its outcome, the body's errors included, given the signal
 * aborted once the attempt is abandoned. */
export type Attempt = (signal: AbortSignal) => Promise<run.Outcome>;

export interface Clock {
  /** The time given to the machines, in ms since 1970. */
  now(): number;
  /** Monotonic ms, for durations a step of the wall clock must not change: the shutdown grace. */
  elapsed(): number;
  /** Returns once one of `futures` is done, or once `now()` reaches `at`. */
  wait(futures: Iterable<Future<unknown>>, at: number | undefined): Promise<void>;
  /** Runs `fn` as a task of the worker. */
  spawn<T>(fn: () => Promise<T>, name: string): Future<T>;
  /** Runs one attempt of the step `name`; `undefined` when its timeout passed first or
   * `abandoned` was aborted, its body's signal aborted then. */
  attempt(
    name: string,
    key: string,
    body: Attempt,
    timeoutMs: number | undefined,
    abandoned: AbortSignal,
  ): Promise<run.Outcome | undefined>;
  /** The code was given the failure of `chain`, a child it started; the testing environment
   * counts it read. */
  failureRead(chain: string): void;
}

/** `setTimeout` fires at once past this delay, so longer waits are cut and taken again. */
const MAX_TIMER_MS = 2 ** 31 - 1;
const ABANDONED = Symbol("abandoned");

/** An attempt's signal, aborted by its own `abort` or by the replay's `abandoned`;
 * `aborted` resolves then. */
export class Interrupt {
  readonly #controller = new AbortController();
  readonly #abandoned: AbortSignal;
  readonly #onAbandoned: () => void;
  readonly aborted: Promise<typeof ABANDONED>;

  constructor(abandoned: AbortSignal) {
    this.#abandoned = abandoned;
    this.#onAbandoned = () => this.abort(abandoned.reason);
    const signal = this.#controller.signal;
    this.aborted = new Promise((resolve) => {
      signal.addEventListener("abort", () => resolve(ABANDONED), { once: true });
    });
    if (abandoned.aborted) this.abort(abandoned.reason);
    else abandoned.addEventListener("abort", this.#onAbandoned, { once: true });
  }

  get signal(): AbortSignal {
    return this.#controller.signal;
  }

  abort(reason: unknown): void {
    this.#controller.abort(reason);
  }

  close(): void {
    this.#abandoned.removeEventListener("abort", this.#onAbandoned);
  }
}

/** The reason of an attempt's signal aborted by its timeout. */
export function pastTimeout(ms: number): DOMException {
  return new DOMException(`attempt past ${ms} ms`, "TimeoutError");
}

/** The reason of an attempt's signal aborted as its replay ends without it. */
export function abandonedReplay(): DOMException {
  return new DOMException("the attempt was abandoned", "AbortError");
}

/** The real clock. */
export class RealClock implements Clock {
  now(): number {
    return Date.now();
  }

  elapsed(): number {
    return performance.now();
  }

  async wait(futures: Iterable<Future<unknown>>, at: number | undefined): Promise<void> {
    const pending = [...futures];
    const ms = at === undefined ? undefined : at - this.now();
    if (pending.some((f) => f.done) || (ms !== undefined && ms <= 0)) return;
    if (ms === undefined && pending.length === 0) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const waits = pending.map((f) => f.settled);
    if (ms !== undefined) {
      waits.push(
        new Promise((resolve) => (timer = setTimeout(resolve, Math.min(ms, MAX_TIMER_MS)))),
      );
    }
    try {
      await Promise.race(waits);
    } finally {
      clearTimeout(timer);
    }
  }

  spawn<T>(fn: () => Promise<T>, _name: string): Future<T> {
    return Future.of(fn());
  }

  async attempt(
    _name: string,
    _key: string,
    body: Attempt,
    timeoutMs: number | undefined,
    abandoned: AbortSignal,
  ): Promise<run.Outcome | undefined> {
    const interrupt = new Interrupt(abandoned);
    let timer: ReturnType<typeof setTimeout> | undefined;
    if (timeoutMs !== undefined) {
      timer = setTimeout(() => interrupt.abort(pastTimeout(timeoutMs)), timeoutMs);
    }
    try {
      // A body that does not pass its signal on goes on: its late outcome is discarded.
      const outcome = await Promise.race([body(interrupt.signal), interrupt.aborted]);
      return outcome === ABANDONED ? undefined : outcome;
    } finally {
      clearTimeout(timer);
      interrupt.close();
    }
  }

  failureRead(_chain: string): void {}
}

export const REAL: Clock = new RealClock();

/** A queue whose arrivals a clock can wait on: `signal()` is done once it holds an item. */
export class Inbox<T> {
  #items: T[] = [];
  #signal: Future | undefined;

  push(item: T): void {
    this.#items.push(item);
    this.#signal?.resolve();
  }

  get empty(): boolean {
    return this.#items.length === 0;
  }

  take(): T | undefined {
    return this.#items.shift();
  }

  signal(): Future {
    const future = new Future();
    if (this.#items.length > 0) future.resolve();
    else this.#signal = future;
    return future;
  }
}
