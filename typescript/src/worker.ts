// A worker: the agent machine of I-23 driven against a store, and the replays it asks for.
import process from "node:process";
import { create } from "@bufbuild/protobuf";
import type { Core } from "./core.ts";
import { delay } from "./durations.ts";
import { ProtocolError } from "./errors.ts";
import { AgentMachine } from "./machines.ts";
import { agent, type boundary, now, type run } from "./proto.ts";
import { Channel, dropped, type ReplayEvent, type ReplayHost, replay } from "./replay.ts";
import type { Settings } from "./settings.ts";
import { perform } from "./stores/open.ts";
import type { Store } from "./stores/store.ts";
import type { Workflows } from "./workflow.ts";

type Event =
  | ReplayEvent
  | { readonly kind: "feed"; readonly id: bigint; readonly result: boundary.OpResult }
  | { readonly kind: "wake" }
  | { readonly kind: "shutdown" };

/** Events for the agent loop, taken one at a time. */
class Mailbox<T> {
  #items: T[] = [];
  #waiter: (() => void) | undefined;

  push(item: T): void {
    this.#items.push(item);
    const waiter = this.#waiter;
    this.#waiter = undefined;
    waiter?.();
  }

  /** The next event, or `undefined` once `ms` passed without one. */
  async take(ms: number): Promise<T | undefined> {
    if (this.#items.length === 0 && ms > 0) {
      const timer = delay(ms);
      const arrived = new Promise<void>((resolve) => {
        this.#waiter = resolve;
      });
      await Promise.race([timer.promise, arrived]);
      timer.cancel();
      this.#waiter = undefined;
    }
    return this.#items.shift();
  }
}

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
): agent.AgentConfig {
  return create(agent.AgentConfigSchema, {
    prefix: "",
    partitions,
    worker,
    workflows: workflows.served(),
    concurrency: s.concurrency,
    claimTtlMs: BigInt(s.claimTtl.ms),
    claimSkewMs: BigInt(s.claimSkew.ms),
    maxIdleMs: BigInt(s.maxIdle.ms),
    duePage: s.duePage,
    duePages: s.duePages,
    snapshotEvery: BigInt(s.snapshotEvery),
    opRetries: OP_RETRIES,
    seed: randomSeed(),
    settings: {
      commitEveryMs: BigInt(s.commitEvery.ms),
      continueAsNewAfterSegments: BigInt(s.continueAsNewAfter.segments),
      continueAsNewAfterBytes: BigInt(s.continueAsNewAfter.bytes),
      dedupWindowMs: BigInt(s.dedupWindow.ms),
    },
    storeWaits,
    waitSeconds: s.wait.ms / 1000,
  });
}

/** The replays of a worker, each talking to the agent loop through its channel. */
class Replays {
  readonly running: Map<string, Channel> = new Map();
  readonly #core: Core;
  readonly #workflows: Workflows;
  readonly #events: Mailbox<Event>;
  readonly #host: ReplayHost;

  constructor(core: Core, workflows: Workflows, events: Mailbox<Event>, host: ReplayHost) {
    this.#core = core;
    this.#workflows = workflows;
    this.#events = events;
    this.#host = host;
  }

  spawn(runId: string, input: run.RunInput): void {
    const start = input.start;
    const workflow = start && this.#workflows.get(start.workflow, start.version);
    if (workflow === undefined) {
      console.error(`substantial: replay of ${runId}: this worker does not serve its workflow`);
      this.#events.push({ kind: "ended", run: runId, end: dropped() });
      return;
    }
    if (this.running.has(runId)) console.error(`substantial: a second replay of ${runId}`);
    const channel = new Channel();
    this.running.set(runId, channel);
    const emit = (event: ReplayEvent) => this.#events.push(event);
    void replay(this.#core, workflow, input, now, emit, channel, this.#host);
  }

  committed(c: agent.AgentCommand_Committed): void {
    const channel = this.running.get(c.run);
    if (channel === undefined) console.warn(`substantial: committed for ${c.run}, not in replay`);
    channel?.committed(c);
  }

  stop(runId: string): void {
    const channel = this.running.get(runId);
    if (channel === undefined) console.warn(`substantial: stop for ${runId}, not in replay`);
    channel?.stop();
  }

  stopAll(): void {
    for (const channel of this.running.values()) channel.stop();
  }
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

let wakeWorker: (worker: Worker) => void;

/** Wakes `worker` at once, as after a message sent from this process. */
export function wake(worker: Worker): void {
  wakeWorker(worker);
}

/** Runs the workflows it is given, on every partition, from its creation: `await using` it,
 * `shutdown()` it, or `serve()` it until a SIGTERM or a Ctrl-C. */
export class Worker {
  readonly #id: string;
  readonly #store: Store;
  readonly #settings: Settings;
  readonly #events = new Mailbox<Event>();
  readonly #loop: Promise<void>;
  #failure: { error: unknown } | undefined;
  #wakePending = false;

  constructor(
    core: Core,
    store: Store,
    workflows: Workflows,
    partitions: number,
    settings: Settings,
    hooks: WorkerHooks,
  ) {
    this.#id = hex(crypto.getRandomValues(new Uint8Array(8)));
    this.#store = store;
    this.#settings = settings;
    const config = agentConfig(this.#id, workflows, partitions, settings, store.capabilities.wait);
    const machine = new AgentMachine(core, config);
    let first: agent.AgentCommand;
    try {
      first = machine.next(now());
    } catch (e) {
      machine.close();
      throw e;
    }
    const replays = new Replays(core, workflows, this.#events, {
      store,
      stepTimeout: settings.stepTimeout,
    });
    this.#loop = this.#run(machine, replays, first)
      .catch((error) => {
        this.#failure = { error };
        console.error(`substantial: worker ${this.#id} died:`, error);
        hooks.died(error);
      })
      .finally(() => hooks.stopped());
  }

  static {
    wakeWorker = (worker) => worker.#wake();
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

  async #run(machine: AgentMachine, replays: Replays, first: agent.AgentCommand): Promise<void> {
    const grace = this.#settings.shutdownGrace.ms;
    let graceEnds: number | undefined;
    try {
      for (let command = first; ; command = machine.next(now())) {
        const c = command.command;
        switch (c.case) {
          case "store": {
            const { id, op } = c.value;
            if (op === undefined) throw new ProtocolError("store command without an op");
            void perform(this.#store, op).then((result) => {
              this.#events.push({ kind: "feed", id, result });
            });
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
            let timeout = Number(c.value.ms) - Date.now();
            const pastGrace = graceEnds !== undefined && replays.running.size > 0;
            if (graceEnds !== undefined && pastGrace) {
              timeout = Math.min(timeout, Math.max(0, graceEnds - performance.now()));
            }
            const event = await this.#events.take(timeout);
            if (event === undefined) {
              if (graceEnds !== undefined && pastGrace && performance.now() >= graceEnds) {
                replays.stopAll();
                graceEnds = performance.now() + grace;
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
                  graceEnds = performance.now() + grace;
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
