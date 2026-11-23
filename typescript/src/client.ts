// Creating and opening a namespace (I-16), starting and following runs (I-17), and workers.
import { create } from "@bufbuild/protobuf";
import { inputObject } from "./child.ts";
import { type Core, CoreModule } from "./core.ts";
import { type Budget, effectiveBudget, type Prices } from "./cost.ts";
import {
  ConfigError,
  type Failure,
  InvalidArgument,
  LayoutError,
  onlyOptions,
  ProtocolError,
  RunExists,
  StoreError,
} from "./errors.ts";
import { checkKey, checkSentName } from "./names.ts";
import { Metering, type Observed } from "./observe.ts";
import { outcomeOf, planOutcomes, runPlan, ulid } from "./plan.ts";
import { encodeJson, failureFrom, jsonValue, type layout, plan, time } from "./proto.ts";
import { type Namespace, Run, sentOutcome } from "./run.ts";
import { type Config, type Cron, checkConfig, config, MESSAGE, messageId } from "./schedule.ts";
import {
  type InternalOptions,
  resolveSettings,
  type Settings,
  type WorkerOptions,
} from "./settings.ts";
import type {
  Credentials,
  CredentialsProvider,
  Password,
  PasswordProvider,
} from "./stores/credentials.ts";
import { openStore } from "./stores/open.ts";
import { type Store, StoreFailure } from "./stores/store.ts";
import { realIncarnation, Worker, type WorkerHooks, wakeOn } from "./worker.ts";
import { type AnyWorkflow, Workflow, Workflows } from "./workflow.ts";

export interface OpenOptions {
  /** `"env"` reads the store's standard variables; a provider is called again before its keys
   * or password expire; memory and file stores take none, a Redis server without AUTH none. */
  readonly credentials?: Credentials | Password | "env" | CredentialsProvider | PasswordProvider;
  /** Prices the store's requests (I-27); without them, requests are counted and no money is. */
  readonly prices?: Prices;
  /** Called with each store operation, step attempt, replay and scan of this `Substantial` and
   * its workers, and each run they block, find near a limit or leave for a skewed clock (I-29). */
  readonly observe?: (event: Observed) => unknown;
}

/** The prices and observer of a test's `Substantial`. */
export interface MeteringOptions {
  readonly prices?: Prices;
  readonly observe?: (event: Observed) => unknown;
}

export interface ScheduleOptions {
  /** The config's revision, a positive integer: a run applies only a revision higher than its
   * own. */
  readonly revision: number;
  /** How many fires missed while no worker ran fire when one does, 5 when not given. */
  readonly backfill?: number;
}

export interface StartOptions {
  /** Makes the start idempotent; a ULID when none is given. */
  readonly id?: string;
  /** The run's budget, else its workflow's (I-28); the same id with another budget is
   * `RunExists`. */
  readonly budget?: Budget;
  /** Sent to the chain once the run is started or found, as `run.send` sends it. */
  readonly message?: Message;
}

/** A message a start sends: signal with start. */
export interface Message {
  readonly name: string;
  readonly value?: unknown;
  /** Delivered once per id; a ULID when none is given. */
  readonly id?: string;
}

/** A live run of a workflow. */
export interface RunInfo {
  readonly chain: string;
  readonly run: string;
  /** Absent for a workflow without version. */
  readonly version?: number;
  /** When it is due next; absent when it waits for a message. For a blocked run, when it is
   * tried again. */
  readonly dueAt?: Date;
  /** Its first replay, a timer or a step's retry backoff, a message arrived and due to be
   * consumed, a message yet to arrive, or a fix of its code. */
  readonly waitingOn: "start" | "time" | "inbox" | "message" | "blocked";
  /** For a blocked run, the error of its code that blocks it. */
  readonly error?: Failure;
}

const WAITING: (RunInfo["waitingOn"] | undefined)[] = [
  undefined,
  "start",
  "time",
  "inbox",
  "message",
  "blocked",
];

function runInfo(info: plan.RunInfo): RunInfo {
  const waitingOn = WAITING[info.waiting];
  if (waitingOn === undefined) throw new ProtocolError(`run ${info.run} waiting on nothing`);
  const error = info.blocked?.error;
  if (waitingOn === "blocked" && error === undefined) {
    throw new ProtocolError(`run ${info.run} blocked without its error`);
  }
  return {
    chain: info.chain,
    run: info.run,
    ...(info.version === undefined ? {} : { version: info.version }),
    ...(info.dueAt === undefined ? {} : { dueAt: new Date(Number(info.dueAt.ms)) }),
    waitingOn,
    ...(error === undefined ? {} : { error: failureFrom(error) }),
  };
}

/** What a namespace's clients and workers run on: the real time and random ids, or the test
 * environment's. */
export interface Host {
  now(): number;
  ulid(ms: number): string;
  worker(
    module: CoreModule,
    store: Store,
    workflows: Workflows,
    settings: Settings,
    hooks: WorkerHooks,
    metering: Metering,
  ): Worker;
  handle<O>(ns: Namespace, workflow: AnyWorkflow, chain: string): Run<O>;
}

const REAL_HOST: Host = {
  now: () => Date.now(),
  ulid,
  worker: (module, store, workflows, settings, hooks, metering) =>
    new Worker(realIncarnation(module.instantiate(), store), workflows, settings, hooks, metering),
  handle: (ns, workflow, chain) => new Run(ns, workflow, chain),
};

/** An id of this opener's own, so that two processes opening one namespace at once never share a
 * probe key. */
function probe(): string {
  // § mgfgmd25p.e@70yfx
  return ulid(Date.now());
}

export function createInput(): plan.PlanInput {
  const value = { prefix: "", probe: probe() };
  return create(plan.PlanInputSchema, { plan: { case: "create", value } });
}

export function openInput(): plan.PlanInput {
  const value = { prefix: "", probe: probe() };
  return create(plan.PlanInputSchema, { plan: { case: "open", value } });
}

/** A promise rejected when a worker of the namespace dies, for the waits pending until then. */
function deathSignal(): PromiseWithResolvers<never> {
  const signal = Promise.withResolvers<never>();
  signal.promise.catch(() => {});
  return signal;
}

/** Runs `input`, an open or a create, on `store`: the core of the namespace. */
export async function opening(store: Store, input: plan.PlanInput): Promise<CoreModule> {
  const module = await CoreModule.shipped();
  const outcome = outcomeOf("open", await runPlan(module.instantiate(), store, input), "opened");
  const opened = outcome.opened;
  if (opened.case === "layoutError") throw new LayoutError(opened.value);
  if (opened.case === "configError") throw new ConfigError(opened.value);
  // § one zemxe46k3.b@9ssv3
  if (opened.case === "failed") throw new StoreError(opened.value.message);
  if (opened.case !== "open") throw new LayoutError("empty open outcome");
  return module;
}

let openedOn: (
  store: Store,
  input: plan.PlanInput,
  metering: Metering,
  host?: Host,
) => Promise<Substantial>;
let internalWorker: (
  sub: Substantial,
  workflows: Iterable<AnyWorkflow>,
  options: InternalOptions,
) => Worker;

/** An opened namespace: the store, and the workers of this process serving it. */
export class Substantial {
  readonly #module: CoreModule;
  readonly #core: Core;
  /** Observed already when there is an observer. */
  readonly #store: Store;
  readonly #owned: boolean;
  readonly #workers: Set<Worker> = new Set();
  readonly #host: Host;
  /** Its prices and observer, which its workers take. */
  readonly #metering: Metering;
  #death = deathSignal();

  protected constructor(
    module: CoreModule,
    store: Store,
    owned: boolean,
    metering: Metering,
    host: Host = REAL_HOST,
  ) {
    this.#host = host;
    this.#module = module;
    this.#core = module.instantiate();
    this.#store = store;
    this.#owned = owned;
    this.#metering = metering;
  }

  /** Creates the namespace the store URL names, or opens it if it exists. */
  static async create(url: string, options: OpenOptions = {}): Promise<Substantial> {
    onlyOptions(options, ["credentials", "prices", "observe"], "create", ConfigError);
    return Substantial.#fromUrl(url, options, createInput());
  }

  /** Opens the namespace the store URL names; one that does not exist is a `LayoutError`. */
  static async open(url: string, options: OpenOptions = {}): Promise<Substantial> {
    onlyOptions(options, ["credentials", "prices", "observe"], "open", ConfigError);
    return Substantial.#fromUrl(url, options, openInput());
  }

  static async #fromUrl(
    url: string,
    options: OpenOptions,
    input: plan.PlanInput,
  ): Promise<Substantial> {
    if (typeof url !== "string") throw new ConfigError(`a store is a URL string, not ${url}`);
    const metering = new Metering(options.prices, options.observe);
    return Substantial.#opened(await openStore(url, options.credentials), true, input, metering);
  }

  static async #opened(
    store: Store,
    owned: boolean,
    input: plan.PlanInput,
    metering: Metering,
    host?: Host,
  ): Promise<Substantial> {
    const observed = metering.wrap(store);
    try {
      try {
        await observed.open();
      } catch (e) {
        throw e instanceof StoreFailure ? new StoreError(e.message) : e;
      }
      const module = await opening(observed, input);
      return new Substantial(module, observed, owned, metering, host);
    } catch (e) {
      if (owned) await observed.close();
      throw e;
    }
  }

  static {
    openedOn = (store, input, metering, host) =>
      Substantial.#opened(store, false, input, metering, host);
    internalWorker = (sub, workflows, options) => sub.#worker(workflows, options, true);
  }

  /** Closes the store. */
  async close(): Promise<void> {
    if (this.#owned) await this.#store.close();
  }

  /** `close`, for `await using sub = await s.Substantial.create(...)`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  /** A worker serving `workflows`, with the settings of `preset` and these overrides, running
   * from now on. */
  worker(workflows: Iterable<AnyWorkflow>, options: WorkerOptions): Worker {
    return this.#worker(workflows, options, false);
  }

  #worker(workflows: Iterable<AnyWorkflow>, options: InternalOptions, internal: boolean): Worker {
    const settings = resolveSettings(options, this.#store.capabilities.wait, internal);
    const served = new Workflows(workflows);
    const hooks: WorkerHooks = {
      stopped: () => this.#workers.delete(worker),
      died: (error) => {
        const death = this.#death;
        this.#death = deathSignal();
        death.reject(error);
      },
    };
    const store = this.#store;
    const worker = this.#host.worker(this.#module, store, served, settings, hooks, this.#metering);
    this.#workers.add(worker);
    return worker;
  }
  /** Starts a run of `workflow`, idempotently under `id`. */
  async start<I, O>(
    workflow: Workflow<I, O>,
    input?: I,
    options: StartOptions = {},
  ): Promise<Run<O>> {
    return (await this.#start(workflow, input, options, false)).run;
  }

  /** Starts a run; with `upgrade`, finding the same run under an older version answers that
   * version instead of `RunExists`. */
  async #start<I, O>(
    workflow: Workflow<I, O>,
    input: I | undefined,
    options: StartOptions,
    upgrade: boolean,
  ): Promise<{ run: Run<O>; versioned?: plan.Started_Versioned }> {
    // § bxb0ygj7m@528ra
    if (!(workflow instanceof Workflow)) {
      throw new InvalidArgument(`start takes a workflow, not ${workflow}`);
    }
    // § one h8wg39cct.a@4zwa6
    const json = inputObject(input);
    onlyOptions(options, ["id", "budget", "message"], "start");
    checkKey("workflow name", workflow.name);
    const budget = effectiveBudget(options.budget, workflow.budget);
    const at = time(this.#host.now());
    // § one cq034wpjp@33heg
    const chain =
      options.id === undefined ? this.#host.ulid(Number(at.ms)) : checkKey("run id", options.id);
    const message = options.message === undefined ? undefined : this.#message(options.message, at);
    const value = {
      prefix: "",
      workflow: workflow.name,
      ...(workflow.version === undefined ? {} : { version: workflow.version }),
      input: jsonValue(json),
      id: chain,
      now: at,
      ...(budget === undefined ? {} : { budget }),
      upgrade,
      ...(message === undefined ? {} : { message }),
    };
    const plan_ = create(plan.PlanInputSchema, { plan: { case: "start", value } });
    const outcome = outcomeOf("start", await this.#plan(plan_), "started");
    const started = outcome.started;
    if (started.case === "runExists") {
      throw new RunExists(chain);
    }
    if (started.case === "failed") throw new StoreError(started.value.message);
    this.#wake(workflow.name);
    if (message !== undefined) {
      if (outcome.sent === undefined) throw new ProtocolError(`start of ${chain} sent nothing`);
      sentOutcome(outcome.sent, chain, (w) => this.#wake(w));
    }
    const run = this.run(workflow, chain);
    return started.case === "versioned" ? { run, versioned: started.value } : { run };
  }

  /** A start's message, its id and order made as `run.send` makes them. */
  #message(message: Message, at: layout.Time): plan.StartMessage {
    onlyOptions(message, ["name", "value", "id"], "message");
    const id = message.id === undefined ? undefined : checkKey("message id", message.id);
    const ord = this.#host.ulid(Number(at.ms));
    return create(plan.StartMessageSchema, {
      name: checkSentName(message.name),
      value: jsonValue(encodeJson(message.value, "the message")),
      id: id ?? ord,
      ord,
    });
  }

  /** The live runs of `workflow`, all its versions, each once: the ended ones are not listed. */
  async *runs(workflow: AnyWorkflow): AsyncGenerator<RunInfo, void, undefined> {
    if (!(workflow instanceof Workflow)) {
      throw new InvalidArgument(`runs takes a workflow, not ${workflow}`);
    }
    const value = { prefix: "", workflow: checkKey("workflow name", workflow.name) };
    const input = create(plan.PlanInputSchema, { plan: { case: "runs", value } });
    for await (const outcome of planOutcomes(this.#core, this.#store, input)) {
      const runs = outcomeOf("runs", outcome, "runs").runs;
      if (runs.case === "end") return;
      if (runs.case === "failed") throw new StoreError(runs.value.message);
      if (runs.case !== "page") throw new ProtocolError("empty runs outcome");
      for (const info of runs.value.runs) yield runInfo(info);
    }
  }

  /** Configures the schedule of the run `id` of `workflow`, whose code loops on `c.schedule()`:
   * starts it if needed and sends it this config, applied only if `revision` is higher than its
   * own. `null` disables it. `backfill` is how many fires missed while no worker ran fire when
   * one does, the most recent ones, 5 unless given. A revision already applied with the same
   * config does nothing; a higher one applied, or the same with another config, throws
   * `ConfigError`. */
  async schedule<I, O>(
    workflow: Workflow<I, O>,
    id: string,
    cron: Cron | null,
    options: ScheduleOptions,
  ): Promise<Run<O>> {
    onlyOptions(options, ["revision", "backfill"], "schedule");
    const { revision, backfill = 5 } = options;
    checkConfig(revision, cron, backfill);
    const wanted = config(revision, cron, backfill);
    const published = await this.run(workflow, id).read<Record<string, unknown>>("schedule");
    // § ey8zhz0kq.b@yvntz
    if (published !== undefined) {
      const applied = {
        revision: published.revision ?? null,
        cron: published.cron ?? null,
        tz: published.tz ?? null,
        backfill: published.backfill ?? null,
      };
      if ((applied.revision as number) > revision) {
        throw new ConfigError(
          `schedule ${id}: revision ${applied.revision} is applied already, higher than ${revision}`,
        );
      }
      if (applied.revision === revision) {
        const same = (Object.keys(wanted) as (keyof Config)[]).every(
          (k) => applied[k] === wanted[k],
        );
        if (same) return this.run(workflow, id);
        throw new ConfigError(
          `schedule ${id}: revision ${revision} is applied with another config, ` +
            `${JSON.stringify(applied)}, not ${JSON.stringify(wanted)}`,
        );
      }
    }
    // § end ey8zhz0kq.b
    // § ey8zhz0kq.a@5358n
    const { run, versioned } = await this.#start(workflow, undefined, { id }, true);
    // § sfzkmddwj@yc956
    if (versioned !== undefined) {
      console.info(
        `substantial: schedule ${id} runs version ${versioned.version ?? "none"}: it moves to ` +
          `version ${workflow.version ?? "none"} at its next boundary that upgrades`,
      );
    }
    // § end sfzkmddwj
    await run.send(MESSAGE, wanted, { id: await messageId(wanted) });
    // § end ey8zhz0kq.a
    return run;
  }

  /** A handle on an existing chain. */
  run<I, O>(workflow: Workflow<I, O>, id: string): Run<O> {
    const namespace = {
      store: this.#store,
      plan: (input: plan.PlanInput) => this.#plan(input),
      wake: (workflow: string) => this.#wake(workflow),
      died: () => this.#death.promise,
      now: () => time(this.#host.now()),
      ulid: (ms: number) => this.#host.ulid(ms),
    };
    return this.#host.handle<O>(namespace, workflow, id);
  }

  #plan(input: plan.PlanInput): ReturnType<typeof runPlan> {
    return runPlan(this.#core, this.#store, input);
  }

  /** Wakes the workers of this process serving `workflow`, which then list its due entries at
   * once instead of after `maxIdle`. */
  #wake(workflow: string): void {
    // § c46hgkfse@4qm43
    for (const worker of this.#workers) wakeOn(worker, workflow);
  }
}

/** Creates the namespace on a store object its caller keeps: for tests. */
export async function createOn(store: Store, options: MeteringOptions = {}): Promise<Substantial> {
  return openedOn(store, createInput(), new Metering(options.prices, options.observe));
}

/** Opens the namespace on a store object its caller keeps: for tests, on `host`'s clock. */
export async function openOn(
  store: Store,
  options: MeteringOptions = {},
  host?: Host,
): Promise<Substantial> {
  return openedOn(store, openInput(), new Metering(options.prices, options.observe), host);
}

/** A worker taking every setting, the hidden ones included: for tests. */
export function workerWith(
  sub: Substantial,
  workflows: Iterable<AnyWorkflow>,
  options: InternalOptions,
): Worker {
  return internalWorker(sub, workflows, options);
}
