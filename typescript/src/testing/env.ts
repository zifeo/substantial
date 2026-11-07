// The environment: a `Substantial` on a namespace in memory and a virtual clock, its clients, and
// what a test asks of them.
import { fromBinary } from "@bufbuild/protobuf";
import { createInput, type Host, opening, openOn, Substantial } from "../client.ts";
import type { CoreModule } from "../core.ts";
import { Duration } from "../durations.ts";
import {
  InvalidArgument,
  onlyOptions,
  RunFailed,
  SubstantialError,
  WaitTimeout,
} from "../errors.ts";
import { layout } from "../proto.ts";
import {
  type Ended,
  ended,
  type Namespace,
  type ResultOptions,
  Run,
  type RunStatus,
} from "../run.ts";
import type { WorkerOptions } from "../settings.ts";
import type { AnyWorkflow } from "../workflow.ts";
import { Chaos, type FaultStore, type Faults } from "./store.ts";
import { limits, Stuck } from "./time.ts";
import { EnvWorker, Hold } from "./workers.ts";
import { dueMs, World } from "./world.ts";

/** Runs ended failed without the test reading their failure through the environment. */
export class Unchecked extends SubstantialError {}

/** A handle of the environment: `result` settles and never waits. */
class EnvRun<O> extends Run<O> {
  readonly #world: World;
  readonly #mine: boolean;

  constructor(ns: Namespace, workflow: AnyWorkflow, chain: string, world: World, mine: boolean) {
    super(ns, workflow, chain);
    this.#world = world;
    this.#mine = mine;
  }

  /** Settles, then the value the chain ended with; `Stuck` naming what it waits on if it has not
   * ended, `WaitTimeout` instead when a `timeout` is given. The clock does not move. */
  override async result(options: ResultOptions = {}): Promise<O> {
    onlyOptions(options, ["timeout"], "result");
    const { timeout } = options;
    if (timeout !== undefined && !(timeout instanceof Duration)) {
      throw new InvalidArgument(`timeout is a Duration, not ${timeout}`);
    }
    const world = this.#world;
    await world.env.settle();
    let end: Ended;
    try {
      end = await ended(this);
    } catch (e) {
      if (this.#mine && e instanceof RunFailed && e.status === "failed") {
        world.checked.add(this.chain);
      }
      throw e;
    }
    if (end.done) return end.value as O;
    if (timeout !== undefined)
      throw new WaitTimeout(`run ${this.chain} did not end within ${timeout}`);
    throw new Stuck(`run ${this.chain} has not ended: it waits on ${waitsOn(world, end.run)}`);
  }

  override async status(): Promise<RunStatus> {
    const status = await super.status();
    if (this.#mine && status === "failed") this.#world.checked.add(this.chain);
    return status;
  }
}

function startOf(world: World, run: string): layout.Start {
  const bytes = world.objects.get(`runs/${run}/start`);
  if (bytes === undefined) throw new Error(`run ${run} has no start`);
  return fromBinary(layout.StartSchema, bytes);
}

function serves(world: World, start: layout.Start): boolean {
  return world.pods.some(
    (p) => p.worker?.alive && p.workflows.get(start.workflow, start.version) !== undefined,
  );
}

function named(start: layout.Start): string {
  return start.version === undefined
    ? start.workflow
    : `${start.workflow} version ${start.version}`;
}

function waitsOn(world: World, run: string): string {
  const start = startOf(world, run);
  const what: string[] = [];
  if (!serves(world, start)) what.push(`workflow ${named(start)}, which no worker serves`);
  for (const hold of world.holds) {
    for (const h of hold.entries) if (h.run === run) what.push(`step ${hold.step}, held`);
  }
  for (const key of world.keys(`queues/${start.workflow}/`)) {
    const entry = key.slice(key.lastIndexOf("/") + 1);
    if (entry.split("~")[1] === run) {
      what.push(`a timer due at ${new Date(dueMs(entry)).toISOString()}`);
    }
  }
  if (what.length === 0) what.push("a message, or the end of a run it started");
  const runs = world.keys("runs/").flatMap((k) => (k.endsWith("/start") ? [k.split("/")[1]] : []));
  const unserved = runs
    .filter((r) => r !== undefined && r !== run && !world.objects.has(`runs/${r}/result`))
    .map((r) => [r, startOf(world, r as string)] as const)
    .filter(([, s]) => !serves(world, s))
    .map(([r, s]) => `${r} (${named(s)})`);
  if (unserved.length > 0) {
    what.push(`while runs of workflows no worker serves wait: ${unserved.join(", ")}`);
  }
  return what.join("; ");
}

function host(world: World, mine: boolean): Host {
  return {
    now: () => world.time.ms,
    ulid: world.ulid,
    worker: (module, _store, workflows, partitions, settings, hooks) =>
      new EnvWorker(world, module, workflows, partitions, settings, hooks),
    handle: (ns, workflow, chain) => new EnvRun(ns, workflow, chain, world, mine),
  };
}

export interface EnvOptions {
  /** The instant its clock starts at. */
  readonly now: Date;
  readonly chaos?: Chaos;
}

export interface HoldOptions {
  /** How many attempts to hold, 1 when not given. */
  readonly times?: number;
}

let worldOfEnv: (env: Env) => World;

/** What the environment shares with its clients: for tests of the environment itself. */
export function worldOf(env: Env): World {
  return worldOfEnv(env);
}

type SubstantialBase = abstract new (
  module: CoreModule,
  store: FaultStore,
  partitions: number,
  owned: boolean,
  host: Host,
) => Substantial;
// Its instances are a `Substantial`; its own `open` takes no URL, unlike `Substantial.open`.
const Base: SubstantialBase = Substantial as unknown as SubstantialBase;

/** A `Substantial` for tests: a namespace in memory, on a virtual clock, whose workers run in
 * the test's process and move only in `settle` and `advance`. */
export class Env extends Base {
  readonly #world: World;

  private constructor(module: CoreModule, world: World, partitions: number) {
    super(module, world.store(undefined), partitions, false, host(world, true));
    this.#world = world;
  }

  static {
    worldOfEnv = (env) => env.#world;
  }

  /** A fresh namespace in memory whose clock starts at `now`; `await using` it. */
  static async open(options: EnvOptions): Promise<Env> {
    onlyOptions(options, ["now", "chaos"], "Env.open");
    const { now, chaos } = options;
    if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
      throw new InvalidArgument(`an environment starts at a valid Date, not ${now}`);
    }
    if (chaos !== undefined && !(chaos instanceof Chaos)) {
      throw new InvalidArgument(`chaos is a Chaos, not ${chaos}`);
    }
    const world = new World(`testing-${crypto.randomUUID()}`, now.getTime(), chaos);
    const { module, partitions } = await opening(
      world.store(undefined),
      createInput({ partitions: 1 }),
    );
    const env = new Env(module, world, partitions);
    world.env = env;
    return env;
  }

  /** Another `Substantial` on the same namespace and clock, as another pod or the application
   * under test would hold. */
  client(): Promise<Substantial> {
    return openOn(this.#world.store(undefined), host(this.#world, false));
  }

  override worker(workflows: Iterable<AnyWorkflow>, options: WorkerOptions): EnvWorker {
    return super.worker(workflows, options) as EnvWorker;
  }

  /** The virtual time. */
  get now(): Date {
    return new Date(this.#world.time.ms);
  }

  /** What Chaos injected so far. */
  get faults(): Faults {
    return { ...this.#world.counts };
  }

  /** Lets every worker do all it can at the current time, until nothing can happen without the
   * clock moving. */
  async settle(): Promise<void> {
    await this.#world.time.settle();
    this.#raiseDied();
  }

  /** Settles, then moves the clock forward by `d`, settling at each moment a worker has
   * something due. */
  async advance(d: Duration): Promise<void> {
    if (!(d instanceof Duration)) throw new InvalidArgument(`advance takes a Duration, not ${d}`);
    await this.#world.time.advance(d.ms);
    this.#raiseDied();
  }

  #raiseDied(): void {
    const errors = this.#world.errors;
    if (errors.length > 0) throw errors.shift();
  }

  /** Every store operation fails for the next `d` of virtual time. */
  outage(d: Duration): void {
    if (!(d instanceof Duration)) throw new InvalidArgument(`outage takes a Duration, not ${d}`);
    this.#world.outageUntil = this.#world.time.ms + d.ms;
  }

  /** Holds the next `times` attempts of `step` (`"<step>.undo"` for its undo) once their body
   * returned or threw. */
  hold(step: string, options: HoldOptions = {}): Hold {
    onlyOptions(options, ["times"], "hold");
    const { times = 1 } = options;
    if (typeof step !== "string") throw new InvalidArgument(`hold takes a step name, not ${step}`);
    if (!Number.isSafeInteger(times) || times <= 0) {
      throw new InvalidArgument(`times is a positive integer, not ${times}`);
    }
    const hold = new Hold(step, times);
    this.#world.holds.push(hold);
    return hold;
  }

  /** Throws `Unchecked` naming every run that ended failed without the test reading its failure
   * through the environment. */
  async check(): Promise<void> {
    const world = this.#world;
    const unread = [...world.failed]
      .sort(([a], [b]) => (a < b ? -1 : 1))
      .flatMap(([, { chain, failure }]) => (world.checked.has(chain) ? [] : [failure]));
    if (unread.length > 0) {
      throw new Unchecked(`runs ended failed and the test never read it: ${unread.join("; ")}`);
    }
  }

  /** Kills the workers as a SIGKILL would, waits up to 10 s for the step bodies still running,
   * and drops the namespace. */
  override async close(): Promise<void> {
    const world = this.#world;
    for (const pod of world.pods) if (pod.worker?.alive) pod.worker.crash();
    // A hung body is left running past the bound of `Stuck`, so that leaving never hangs.
    const until = performance.now() + limits.stepMs;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      while (world.time.bodies.size > 0 && performance.now() < until) {
        const late = new Promise((resolve) => {
          timer = setTimeout(resolve, until - performance.now());
        });
        await Promise.race([
          Promise.all([...world.time.bodies.keys()].map((f) => f.settled)),
          late,
        ]);
        clearTimeout(timer);
      }
    } finally {
      clearTimeout(timer);
      world.drop();
    }
  }
}
