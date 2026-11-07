// The workers of an environment: their clock on the virtual time, `crash`, `pause` and `resume`,
// the holds of step attempts, and the crashes Chaos draws.
import {
  type Attempt,
  abandonedReplay,
  type Clock,
  Future,
  Interrupt,
  pastTimeout,
} from "../clock.ts";
import type { CoreModule } from "../core.ts";
import type { run } from "../proto.ts";
import type { Settings } from "../settings.ts";
import { type Incarnation, LOOP, leave, restart, Worker, type WorkerHooks } from "../worker.ts";
import type { Workflows } from "../workflow.ts";
import { never, type Owner, Stuck, type Task } from "./time.ts";
import type { World } from "./world.ts";

/** The clock of one life of a worker: a crash ends it, a Chaos replacement gets a new one. */
export class WorkerClock implements Clock, Owner {
  readonly world: World;
  readonly pod: Pod;
  readonly id: string;
  readonly tasks: Set<Task> = new Set();
  agent: Task | undefined;
  dead = false;
  /** Interrupts the attempts in flight. */
  readonly #interrupts: Set<(reason: unknown) => void> = new Set();
  /** How many times this life tried each operation on each key, for Chaos. */
  readonly tries: Map<string, number> = new Map();
  /** The instant of its last scan, the store's writes then, and the keys its listings drew Chaos
   * faults for (`World.skipScans`). */
  scannedAt: number | undefined;
  seen = -1;
  scanKeys: Set<string> = new Set();

  constructor(world: World, pod: Pod, id: string) {
    this.world = world;
    this.pod = pod;
    this.id = id;
  }

  get paused(): boolean {
    return this.dead || this.pod.paused;
  }

  failureRead(chain: string): void {
    this.world.checked.add(chain);
  }

  /** Records a listing of due entries, drawn as `key`, made when the store had seen `writes`
   * writes. */
  listed(key: string, writes: number): void {
    const now = this.world.time.ms;
    if (this.scannedAt !== now) {
      this.scannedAt = now;
      this.seen = writes;
      this.scanKeys = new Set([key]);
    } else {
      this.seen = Math.min(this.seen, writes);
      this.scanKeys.add(key);
    }
  }

  now(): number {
    return this.world.time.ms;
  }

  elapsed(): number {
    return this.world.time.ms;
  }

  wait(futures: Iterable<Future<unknown>>, at: number | undefined): Promise<void> {
    return this.world.time.wait(this, futures, at);
  }

  spawn<T>(fn: () => Promise<T>, name: string): Future<T> {
    if (this.dead) return new Future<T>();
    const task = this.world.time.spawn(this, fn, name);
    this.tasks.add(task);
    task.future.settled.then(() => this.tasks.delete(task));
    if (name.startsWith(LOOP)) this.agent = task;
    return task.future as Future<T>;
  }

  async attempt(
    name: string,
    key: string,
    body: Attempt,
    timeoutMs: number | undefined,
    abandoned: AbortSignal,
  ): Promise<run.Outcome | undefined> {
    const world = this.world;
    const deadline = timeoutMs === undefined ? undefined : world.time.ms + timeoutMs;
    const interrupt = new Interrupt(abandoned);
    // Abandoned, by its timeout or its worker's crash or shutdown: interrupted, as in production,
    // and no longer waited for by `settle`. A body may crash its own worker before it returns.
    let outcome: Future<run.Outcome> | undefined;
    const abort = (reason: unknown) => {
      interrupt.abort(reason);
      if (outcome !== undefined) world.time.forget(outcome);
    };
    this.#interrupts.add(abort);
    outcome = world.time.body(body(interrupt.signal), `step ${key}`);
    if (interrupt.signal.aborted) world.time.forget(outcome);
    const stopped = Future.of(interrupt.aborted);
    try {
      await this.wait([outcome, stopped], deadline);
    } finally {
      this.#interrupts.delete(abort);
      interrupt.close();
    }
    if (!outcome.done) {
      abort(stopped.done ? interrupt.signal.reason : pastTimeout(timeoutMs ?? 0));
      return undefined;
    }
    if (world.draw("crash", key, this)) {
      world.counts.crashed += 1;
      this.pod.replace();
      return never();
    }
    const hold = world.holdOf(name);
    if (hold !== undefined) {
      const held: Held = { clock: this, released: new Future(), run: key.split("/")[0] ?? "" };
      hold.entries.push(held);
      try {
        await this.wait([held.released], deadline);
      } finally {
        const i = hold.entries.indexOf(held);
        if (i >= 0) hold.entries.splice(i, 1);
      }
      if (!held.released.done) return undefined;
    }
    return outcome.result();
  }

  crash(): void {
    this.dead = true;
    for (const abort of this.#interrupts) abort(abandonedReplay());
    this.#interrupts.clear();
    for (const task of this.tasks) {
      this.world.time.tasks.delete(task);
      this.world.time.parked.delete(task);
    }
    this.world.time.signal();
  }
}

export interface Held {
  readonly clock: WorkerClock;
  readonly released: Future;
  readonly run: string;
}

/** The next `times` attempts of a step, held once their body returned or threw. */
export class Hold {
  readonly step: string;
  left: number;
  readonly entries: Held[] = [];

  constructor(step: string, times: number) {
    this.step = step;
    this.left = times;
  }

  toString(): string {
    return `hold of ${this.step}: ${this.held} held, ${this.left} to come`;
  }

  /** The number of attempts held now. */
  get held(): number {
    return this.entries.filter((h) => !h.clock.dead).length;
  }

  /** Hands the outcomes of the attempts held to their workers. */
  release(): void {
    for (const h of this.entries) h.released.resolve();
    this.entries.length = 0;
  }

  /** Crashes the workers that hold the attempts held. */
  crash(): void {
    for (const h of [...this.entries]) {
      if (!h.clock.dead && h.clock === h.clock.pod.clock) h.clock.pod.worker?.crash();
    }
    this.entries.length = 0;
  }
}

/** What a worker of the environment keeps across its lives. */
export class Pod {
  readonly world: World;
  readonly module: CoreModule;
  readonly workflows: Workflows;
  readonly settings: Settings;
  readonly place: number;
  replaced = 0;
  paused = false;
  crashed = false;
  clock: WorkerClock | undefined;
  worker: EnvWorker | undefined;

  constructor(world: World, module: CoreModule, workflows: Workflows, settings: Settings) {
    this.world = world;
    this.module = module;
    this.workflows = workflows;
    this.settings = settings;
    this.place = world.pods.length;
    world.pods.push(this);
  }

  incarnate(): Incarnation {
    const { world } = this;
    const id = world.workerId(this.place, this.replaced);
    const clock = new WorkerClock(world, this, id);
    this.clock = clock;
    const store = world.store(clock);
    return { core: this.module.instantiate(), store, clock, id, seed: world.coreSeed };
  }

  /** A fresh worker of the same workflows and settings in place of this one, crashed. */
  replace(): void {
    this.clock?.crash();
    this.replaced += 1;
    if (this.worker !== undefined) restart(this.worker, this.incarnate());
  }
}

/** A worker of an environment: `await using` it, then `crash`, `pause`, `resume` it. */
export class EnvWorker extends Worker {
  readonly #pod: Pod;

  constructor(
    world: World,
    module: CoreModule,
    workflows: Workflows,
    partitions: number,
    settings: Settings,
    hooks: WorkerHooks,
  ) {
    const pod = new Pod(world, module, workflows, settings);
    super(pod.incarnate(), workflows, partitions, settings, {
      // What its loop leaves behind, a held attempt, is abandoned with it.
      stopped: () => {
        pod.clock?.crash();
        hooks.stopped();
      },
      died: (error) => {
        hooks.died(error);
        world.errors.push(error);
      },
    });
    pod.worker = this;
    this.#pod = pod;
  }

  override get alive(): boolean {
    return !this.#pod.crashed && super.alive;
  }

  /** Kills the worker at once, as a SIGKILL would: its claims stay until they expire, and its
   * steps in flight are abandoned, their `stepSignal()` aborted and their outcomes dropped. */
  crash(): void {
    this.#pod.crashed = true;
    this.#pod.clock?.crash();
  }

  /** Freezes the worker: no claim renewed, no store reached; its step bodies go on. */
  pause(): void {
    this.#pod.paused = true;
    this.#pod.world.time.signal();
  }

  /** Lets a paused worker go on from where it was. */
  resume(): void {
    this.#pod.paused = false;
    this.#pod.world.time.kick();
  }

  /** Settles, then leaves as a SIGTERM does: steps that returned are recorded, held ones
   * abandoned, and the runs it leaves have their claims deleted, for another worker. */
  override async shutdown(): Promise<void> {
    const pod = this.#pod;
    if (pod.crashed) return;
    const env = pod.world.env;
    await env.settle();
    if (super.alive) {
      leave(this, 0);
      await env.settle();
      if (super.alive) throw new Stuck(`worker ${pod.clock?.id} could not leave: it is paused`);
    }
    await super.shutdown();
  }
}
