// A worker: the agent machine of I-23 driven against a store, and the replays it asks for.
import process from "node:process";
import { create } from "@bufbuild/protobuf";
import { type Clock, Inbox, REAL } from "./clock.ts";
import type { Core } from "./core.ts";
import { ProtocolError } from "./errors.ts";
import { AgentMachine } from "./machines.ts";
import { agent, type boundary, run, time } from "./proto.ts";
import type { ReplayEvent } from "./replay.ts";
import { Replays } from "./replays.ts";
import { FOREVER, type Retention, type Settings } from "./settings.ts";
import { perform } from "./stores/open.ts";
import type { Store } from "./stores/store.ts";
import type { Workflows } from "./workflow.ts";

type Event =
  | ReplayEvent
  | { readonly kind: "feed"; readonly id: bigint; readonly result: boundary.OpResult }
  | { readonly kind: "wake" }
  | { readonly kind: "shutdown"; readonly graceMs?: number };

/** The name of a worker's loop task, before its id. */
export const LOOP = "worker ";

// How many times the core reissues an operation that still failed after the adapter's own
// retries (I-26).
const OP_RETRIES = 5;

function randomSeed(): Uint8Array {
  return crypto.getRandomValues(new Uint8Array(32));
}

export function agentConfig(
  worker: string,
  workflows: Workflows,
  partitions: number,
  s: Settings,
  storeWaits: boolean,
  seed?: Uint8Array,
): agent.AgentConfig {
  return create(agent.AgentConfigSchema, {
    prefix: "",
    partitions,
    worker,
    workflows: workflows.served().map(({ retention, ...served }) => ({
      ...served,
      ...(retention === undefined ? {} : { retention: retentionOf(retention) }),
    })),
    concurrency: s.concurrency,
    claimTtlMs: BigInt(s.claimTtl.ms),
    claimSkewMs: BigInt(s.claimSkew.ms),
    maxIdleMs: BigInt(s.maxIdle.ms),
    duePage: s.duePage,
    duePages: s.duePages,
    snapshotEvery: BigInt(s.snapshotEvery),
    opRetries: OP_RETRIES,
    seed: seed ?? randomSeed(),
    settings: {
      commitEveryMs: BigInt(s.commitEvery.ms),
      continueAsNewAfterSegments: BigInt(s.continueAsNewAfter.segments),
      continueAsNewAfterBytes: BigInt(s.continueAsNewAfter.bytes),
      dedupWindowMs: BigInt(s.dedupWindow.ms),
      retention: retentionOf(s.retention),
    },
    storeWaits,
    waitSeconds: s.wait.ms / 1000,
    tombstoneMs: BigInt(s.tombstone.ms),
    retentionMinMs: BigInt(s.retentionMin.ms),
    recheckMs: BigInt(s.recheck.ms),
  });
}

function retentionOf(r: Retention): run.Retention {
  const keep =
    r === FOREVER
      ? { case: "forever" as const, value: {} }
      : { case: "ms" as const, value: BigInt(r.ms) };
  return create(run.RetentionSchema, { keep });
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/** What a worker tells the namespace that made it. */
export interface WorkerHooks {
  /** Its loop ended, whether it shut down or died. */
  stopped(): void;
  /** Its loop died with `error`. */
  died(error: unknown): void;
}

/** What one life of a worker runs on: the test environment gives each its own. */
export interface Incarnation {
  readonly core: Core;
  readonly store: Store;
  readonly clock: Clock;
  readonly id: string;
  /** The core's seed, each replay's derived from it; a fresh one when undefined. */
  readonly seed?: Uint8Array;
}

/** An incarnation on the real clock, with a random id. */
export function realIncarnation(core: Core, store: Store): Incarnation {
  return { core, store, clock: REAL, id: hex(crypto.getRandomValues(new Uint8Array(8))) };
}

let wakeWorker: (worker: Worker) => void;
let restartWorker: (worker: Worker, incarnation: Incarnation) => void;
let leaveWorker: (worker: Worker, graceMs: number) => void;
let workerPending: (worker: Worker) => boolean;

/** Wakes `worker` at once, as after a message sent from this process. */
export function wake(worker: Worker): void {
  wakeWorker(worker);
}

/** Starts `worker` again as `incarnation`, its last life abandoned. */
export function restart(worker: Worker, incarnation: Incarnation): void {
  restartWorker(worker, incarnation);
}

/** Shuts `worker` down with a grace of `graceMs`, without waiting for it. */
export function leave(worker: Worker, graceMs: number): void {
  leaveWorker(worker, graceMs);
}

/** Whether `worker` has events it has not taken yet. */
export function pending(worker: Worker): boolean {
  return workerPending(worker);
}

/** Runs the workflows it is given, on every partition, from its creation: `await using` it,
 * `shutdown()` it, or `serve()` it until a SIGTERM or a Ctrl-C. */
export class Worker {
  readonly #workflows: Workflows;
  readonly #partitions: number;
  readonly #settings: Settings;
  readonly #hooks: WorkerHooks;
  #events = new Inbox<Event>();
  #loop: Promise<void> = Promise.resolve();
  #running = false;
  #failure: { error: unknown } | undefined;
  #wakePending = false;

  constructor(
    incarnation: Incarnation,
    workflows: Workflows,
    partitions: number,
    settings: Settings,
    hooks: WorkerHooks,
  ) {
    this.#workflows = workflows;
    this.#partitions = partitions;
    this.#settings = settings;
    this.#hooks = hooks;
    this.#start(incarnation);
  }

  static {
    wakeWorker = (worker) => worker.#wake();
    restartWorker = (worker, incarnation) => worker.#start(incarnation);
    leaveWorker = (worker, graceMs) => worker.#events.push({ kind: "shutdown", graceMs });
    workerPending = (worker) => !worker.#events.empty;
  }

  #start(incarnation: Incarnation): void {
    const { core, store, clock, id } = incarnation;
    const waits = store.capabilities.wait;
    const s = this.#settings;
    const config = agentConfig(id, this.#workflows, this.#partitions, s, waits, incarnation.seed);
    const machine = new AgentMachine(core, config);
    let first: agent.AgentCommand;
    try {
      first = machine.next(time(clock.now()));
    } catch (e) {
      machine.close();
      throw e;
    }
    const events = new Inbox<Event>();
    this.#events = events;
    this.#wakePending = false;
    this.#failure = undefined;
    this.#running = true;
    const host = { store, stepTimeout: s.stepTimeout };
    const replays = new Replays(core, this.#workflows, events, host, clock);
    const loop = clock.spawn(
      () => this.#run(machine, replays, first, clock, store, events),
      `${LOOP}${id}`,
    );
    this.#loop = loop.settled.then(() => {
      if (this.#events !== events) return;
      this.#running = false;
      try {
        loop.result();
      } catch (error) {
        this.#failure = { error };
        console.error(`substantial: worker ${id} died:`, error);
        this.#hooks.died(error);
      }
      this.#hooks.stopped();
    });
  }

  /** Whether its loop runs: neither shut down nor dead. */
  get alive(): boolean {
    return this.#running;
  }

  /** The error that killed its loop, else `undefined`: what a readiness probe reads. */
  get error(): unknown {
    return this.#failure?.error;
  }

  #wake(): void {
    if (!this.#wakePending) {
      this.#wakePending = true;
      this.#events.push({ kind: "wake" });
    }
  }

  async #ended(): Promise<void> {
    await this.#loop;
    if (this.#failure !== undefined) throw this.#failure.error;
  }

  /** Starts no new replay and lets the steps in flight end within `shutdownGrace`. */
  shutdown(): Promise<void> {
    this.#events.push({ kind: "shutdown" });
    return this.#ended();
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.shutdown();
  }

  /** Runs until a SIGTERM or a Ctrl-C (SIGINT), or until `shutdown()`, then shuts down. */
  async serve(): Promise<void> {
    const terminate = () => this.#events.push({ kind: "shutdown" });
    process.on("SIGTERM", terminate);
    process.on("SIGINT", terminate);
    try {
      await this.#ended();
    } finally {
      process.off("SIGTERM", terminate);
      process.off("SIGINT", terminate);
    }
  }

  async #run(
    machine: AgentMachine,
    replays: Replays,
    first: agent.AgentCommand,
    clock: Clock,
    store: Store,
    events: Inbox<Event>,
  ): Promise<void> {
    let grace = 0;
    let graceEnds: number | undefined;
    try {
      for (let command = first; ; command = machine.next(time(clock.now()))) {
        const c = command.command;
        switch (c.case) {
          case "store": {
            const { id, op } = c.value;
            if (op === undefined) throw new ProtocolError("store command without an op");
            clock.spawn(async () => {
              events.push({ kind: "feed", id, result: await perform(store, op) });
            }, "store op");
            break;
          }
          case "replay":
            if (c.value.input === undefined) throw new ProtocolError("replay without input");
            replays.spawn(c.value.run, c.value.input);
            break;
          case "committed":
            replays.committed(c.value);
            break;
          case "stopReplay":
            replays.stop(c.value);
            break;
          case "waitUntil": {
            let at = Number(c.value.ms);
            const pastGrace = graceEnds !== undefined && replays.running.size > 0;
            if (graceEnds !== undefined && pastGrace) {
              at = Math.min(at, clock.now() + Math.max(0, graceEnds - clock.elapsed()));
            }
            if (events.empty) await clock.wait([events.signal()], at);
            const event = events.take();
            if (event === undefined) {
              if (graceEnds !== undefined && pastGrace && clock.elapsed() >= graceEnds) {
                replays.stopAll();
                graceEnds = clock.elapsed() + grace;
              }
              break;
            }
            switch (event.kind) {
              case "feed":
                machine.feed(event.id, event.result);
                break;
              case "commit":
                machine.commit(event.run, event.commit);
                break;
              case "ended":
                replays.running.delete(event.run);
                machine.replayEnded(event.run, event.end);
                break;
              case "wake":
                this.#wakePending = false;
                machine.wake();
                break;
              case "shutdown":
                if (graceEnds === undefined) {
                  grace = event.graceMs ?? this.#settings.shutdownGrace.ms;
                  graceEnds = clock.elapsed() + grace;
                  machine.shutdown();
                }
                break;
            }
            break;
          }
          case "stop":
            return;
          default:
            throw new ProtocolError("empty agent command");
        }
      }
    } finally {
      replays.stopAll();
      machine.close();
    }
  }
}
