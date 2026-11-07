// The virtual time of an environment: its instant, the deadlines its tasks wait on, the gate that
// `settle` and `advance` open, and how they know that nothing more can happen.
//
// Every task of a worker (its loop, its store operations, its replays and their attempts) is
// spawned here and waits only through `wait`, so the environment knows at each moment which ones
// are parked and on what. Nothing can happen without the clock moving once every such task is
// parked on a future not done and a deadline not come, and no step body runs.
import { AsyncLocalStorage } from "node:async_hooks";
import { setImmediate } from "node:timers";
import { Future } from "../clock.ts";
import { SubstantialError } from "../errors.ts";
import { REPLAY } from "../replays.ts";

/** What stops a settle: tests lower them. */
export const limits = {
  /** Real ms a step body may run before `settle` raises `Stuck`. */
  stepMs: 10_000,
  /** Replays of one run in one settle past which runs are taken to wake each other forever. */
  replays: 100,
};

/** A run waits on something the environment cannot bring, or a settle cannot end. */
export class Stuck extends SubstantialError {}

/** The worker a task belongs to: a paused or dead one does not act. */
export interface Owner {
  readonly paused: boolean;
  readonly dead: boolean;
}

export interface Task {
  readonly name: string;
  readonly owner: Owner;
  readonly future: Future<unknown>;
}

/** A task parked until one of `futures` is done or the clock reaches `at`, in ms. */
export class Waiter {
  readonly futures: Future<unknown>[];
  at: number | undefined;
  readonly owner: Owner;
  kick: Future | undefined;

  constructor(futures: Future<unknown>[], at: number | undefined, owner: Owner) {
    this.futures = futures;
    this.at = at;
    this.owner = owner;
  }

  ready(now: number): boolean {
    if (this.owner.paused) return false;
    return (this.at !== undefined && this.at <= now) || this.futures.some((f) => f.done);
  }
}

const current = new AsyncLocalStorage<Task>();

/** Throws inside a task of the environment or a step body it started, which a settle there
 * would wait on. */
function outside(): void {
  if (current.getStore() !== undefined) {
    throw new Stuck(
      "a step cannot wait for the environment to settle (result, settle or advance), " +
        "which waits for the step itself: start a child with c.start and await its result, " +
        "or pass the value in",
    );
  }
}

/** A promise that never settles: what a task of a dead worker waits on. */
export function never<T>(): Promise<T> {
  return new Promise(() => {});
}

export class Time {
  ms: number;
  open = false;
  readonly parked: Map<Task, Waiter> = new Map();
  readonly tasks: Set<Task> = new Set();
  /** Step bodies running, with the real time they started at. */
  readonly bodies: Map<Future<unknown>, { name: string; started: number }> = new Map();
  readonly replayed: Map<string, number> = new Map();
  /** Moves past the scans that cannot find anything, up to an instant (`World.skipScans`). */
  skip: (target: number) => void = () => {};
  #changed: Future | undefined;
  #lock: Promise<unknown> = Promise.resolve();

  constructor(ms: number) {
    this.ms = ms;
  }

  signal(): void {
    this.#changed?.resolve();
  }

  /** Has every parked task check again whether it may go on. */
  kick(): void {
    for (const w of this.parked.values()) w.kick?.resolve();
    this.signal();
  }

  async wait(
    owner: Owner,
    futures: Iterable<Future<unknown>>,
    at: number | undefined,
  ): Promise<void> {
    const task = current.getStore();
    if (task === undefined) throw new Error("a wait outside a task of the environment");
    if (owner.dead) return never<void>();
    const w = new Waiter([...futures], at, owner);
    this.parked.set(task, w);
    this.signal();
    try {
      // A wait already due yields to the event loop, so that a loop of such waits lets the other
      // tasks run, a replay waiting for its code to go quiet included.
      if (this.open && w.ready(this.ms)) await new Promise((resolve) => setImmediate(resolve));
      while (!(this.open && w.ready(this.ms))) {
        const kick = new Future();
        w.kick = kick;
        if (w.futures.some((f) => f.done)) await kick.settled;
        else await Promise.race([kick.settled, ...w.futures.map((f) => f.settled)]);
      }
    } finally {
      this.parked.delete(task);
    }
  }

  /** Returns once the gate is open and `owner` not paused. */
  async passing(owner: Owner): Promise<void> {
    if (!this.open || owner.paused) {
      const done = new Future();
      done.resolve();
      await this.wait(owner, [done], undefined);
    }
  }

  /** A task of a worker, which starts once the gate is open. */
  spawn<T>(owner: Owner, fn: () => Promise<T>, name: string): Task {
    const future = new Future<T>();
    const task: Task = { name, owner, future };
    this.tasks.add(task);
    const finish = () => {
      this.tasks.delete(task);
      this.signal();
    };
    current
      .run(task, async () => {
        await this.passing(owner);
        return fn();
      })
      .then(
        (value) => {
          finish();
          future.resolve(value);
        },
        (error) => {
          finish();
          future.reject(error);
        },
      );
    if (name.startsWith(REPLAY)) {
      const run = name.slice(REPLAY.length);
      this.replayed.set(run, (this.replayed.get(run) ?? 0) + 1);
    }
    return task;
  }

  /** A step body, waited for by `settle` until it ends or is forgotten. */
  body<T>(promise: Promise<T>, name: string): Future<T> {
    const future = new Future<T>();
    this.bodies.set(future, { name, started: performance.now() });
    const end = () => {
      this.bodies.delete(future);
      this.signal();
    };
    promise.then(
      (value) => {
        end();
        future.resolve(value);
      },
      (error) => {
        end();
        future.reject(error);
      },
    );
    return future;
  }

  /** A body interrupted: `settle` no longer waits for it, though it may go on. */
  forget(body: Future<unknown>): void {
    this.bodies.delete(body);
    this.signal();
  }

  quiet(): boolean {
    if (this.bodies.size > 0) return false;
    for (const task of this.tasks) {
      const w = this.parked.get(task);
      if (w === undefined || w.ready(this.ms)) return false;
    }
    return true;
  }

  /** What keeps a settle from ending. */
  active(): string[] {
    const names = [...this.bodies.values()].map((b) => b.name);
    for (const task of this.tasks) {
      const w = this.parked.get(task);
      if (w === undefined || w.ready(this.ms)) names.push(task.name);
    }
    return names.sort();
  }

  async settle(): Promise<void> {
    outside();
    return this.#locked(() => this.#settle());
  }

  async advance(ms: number): Promise<void> {
    outside();
    return this.#locked(async () => {
      const target = this.ms + ms;
      await this.#settle();
      while (this.ms < target) {
        this.skip(target);
        let next = target;
        for (const w of this.parked.values()) {
          if (w.at !== undefined && !w.owner.paused && w.at < next) next = w.at;
        }
        if (next <= this.ms)
          throw new Error(`a deadline at ${next} ms is not after now, ${this.ms} ms`);
        this.ms = next;
        await this.#settle();
      }
    });
  }

  #locked(fn: () => Promise<void>): Promise<void> {
    const run = this.#lock.then(fn);
    this.#lock = run.catch(() => {});
    return run;
  }

  async #settle(): Promise<void> {
    // Replays are counted per settle: the test's own calls between settles wake runs too.
    this.replayed.clear();
    this.open = true;
    this.kick();
    try {
      while (!this.quiet()) {
        const changed = new Future();
        this.#changed = changed;
        const budget = this.#budget();
        let timer: ReturnType<typeof setTimeout> | undefined;
        const waits = [changed.settled];
        if (budget !== undefined) waits.push(new Promise((r) => (timer = setTimeout(r, budget))));
        await Promise.race(waits);
        clearTimeout(timer);
        this.#check();
      }
    } finally {
      this.open = false;
    }
  }

  #budget(): number | undefined {
    if (this.bodies.size === 0) return undefined;
    const oldest = Math.min(...[...this.bodies.values()].map((b) => b.started));
    return Math.max(0, oldest + limits.stepMs - performance.now());
  }

  #check(): void {
    const now = performance.now();
    const late = [...this.bodies.values()].filter((b) => now - b.started >= limits.stepMs);
    if (late.length > 0) {
      throw new Stuck(
        `a step body has not returned after ${limits.stepMs / 1000} s: ` +
          `${late.map((b) => b.name).join(", ")}; still active: ${this.active().join(", ")}`,
      );
    }
    for (const [run, n] of this.replayed) {
      if (n > limits.replays) {
        throw new Stuck(
          `run ${run} was replayed more than ${limits.replays} times without the clock moving: ` +
            "runs keep waking each other",
        );
      }
    }
  }
}
