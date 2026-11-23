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
   * `abandoned` was aborted, `"silent"` when its heartbeat timeout passed since its last
   * heartbeat, its body's signal aborted then. */
  attempt(
    name: string,
    key: string,
    body: Attempt,
    timeoutMs: number | undefined,
    abandoned: AbortSignal,
    heartbeat?: Heartbeat,
  ): Promise<run.Outcome | "silent" | undefined>;
  /** The code was given the failure of `chain`, a child it started; the testing environment
   * counts it read. */
  failureRead(chain: string): void;
  /** Whether a replay stops at the end of its history, as the replay tester's do. */
  checksOnly(): boolean;
  /** The replay of `run` reached the end of its history, or diverged before it. */
  checked(run: string, diverged: string | undefined): void;
}

/** An attempt's heartbeat timeout, and the clock's `elapsed` at its last heartbeat, or at its
 * start. */
export interface Heartbeat {
  readonly timeoutMs: number;
  last: number;
}

/** When the attempt is silent for too long, unless it beats again before. */
export function silentAt(heartbeat: Heartbeat | undefined): number | undefined {
  return heartbeat === undefined ? undefined : heartbeat.last + heartbeat.timeoutMs;
}

/** Whether an attempt of `deadline` is late at `now`, its timeout first. */
export function late(
  now: number,
  deadline: number | undefined,
  heartbeat: Heartbeat | undefined,
): "timeout" | "silent" | undefined {
  if (deadline !== undefined && now >= deadline) return "timeout";
  const silent = silentAt(heartbeat);
  return silent !== undefined && now >= silent ? "silent" : undefined;
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

/** The reason of an attempt's signal aborted by its heartbeat timeout. */
export function pastHeartbeat(ms: number): DOMException {
  return new DOMException(`no heartbeat within ${ms} ms`, "TimeoutError");
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
    heartbeat?: Heartbeat,
  ): Promise<run.Outcome | "silent" | undefined> {
    const interrupt = new Interrupt(abandoned);
    let timer: ReturnType<typeof setTimeout> | undefined;
    if (timeoutMs !== undefined) {
      // § aym52h8ch@q203m
      timer = setTimeout(() => interrupt.abort(pastTimeout(timeoutMs)), timeoutMs);
    }
    let silent = false;
    let watching: ReturnType<typeof setTimeout> | undefined;
    if (heartbeat !== undefined) {
      // § vvp37rp40@sjk01
      const deadline = timeoutMs === undefined ? undefined : this.elapsed() + timeoutMs;
      const watch = () => {
        if (interrupt.signal.aborted) return;
        const now = this.elapsed();
        const due = late(now, deadline, heartbeat);
        if (due === undefined) {
          watching = setTimeout(watch, (silentAt(heartbeat) ?? now) - now);
        } else if (due === "silent") {
          silent = true;
          interrupt.abort(pastHeartbeat(heartbeat.timeoutMs));
        }
      };
      watching = setTimeout(watch, heartbeat.timeoutMs);
    }
    try {
      // § v4n6ja42e.a@3sgt1
      const outcome = await Promise.race([body(interrupt.signal), interrupt.aborted]);
      if (outcome === ABANDONED && silent) return "silent";
      // A body that does not pass its signal on goes on: its late outcome is discarded.
      // § one v4n6ja42e.c@zf94n
      return outcome === ABANDONED ? undefined : outcome;
    } finally {
      clearTimeout(timer);
      clearTimeout(watching);
      interrupt.close();
    }
  }

  failureRead(_chain: string): void {}

  checksOnly(): boolean {
    return false;
  }

  checked(_run: string, _diverged: string | undefined): void {}
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
