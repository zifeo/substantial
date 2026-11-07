// Creating and opening a namespace (I-16), starting and following runs (I-17), and workers.
import { create } from "@bufbuild/protobuf";
import { type Core, CoreModule } from "./core.ts";
import {
  ConfigError,
  InvalidArgument,
  LayoutError,
  onlyOptions,
  RunExists,
  StoreError,
} from "./errors.ts";
import { checkKey } from "./names.ts";
import { outcomeOf, runPlan, ulid } from "./plan.ts";
import { encodeJson, jsonValue, now, plan } from "./proto.ts";
import { Run } from "./run.ts";
import { type InternalOptions, resolveSettings, type WorkerOptions } from "./settings.ts";
import type { Credentials, CredentialsProvider } from "./stores/credentials.ts";
import { openStore } from "./stores/open.ts";
import type { Store } from "./stores/store.ts";
import { Worker, wake } from "./worker.ts";
import { type AnyWorkflow, Workflow, Workflows } from "./workflow.ts";

export interface OpenOptions {
  /** `"env"` reads the store's standard variables; a provider is called again before its keys
   * expire; memory and file stores take none. */
  readonly credentials?: Credentials | "env" | CredentialsProvider;
}

export interface CreateOptions extends OpenOptions {
  readonly partitions: number;
}

export interface StartOptions {
  /** Makes the start idempotent; a ULID when none is given. */
  readonly id?: string;
}

function createInput(options: { readonly partitions: number }): plan.PlanInput {
  if ((options as Partial<CreateOptions> | undefined)?.partitions === undefined) {
    throw new ConfigError("create needs partitions, a positive whole number");
  }
  const { partitions } = options;
  if (!Number.isSafeInteger(partitions) || partitions <= 0 || partitions > 0xffff_ffff) {
    throw new ConfigError(`partitions must be a positive whole number, not ${partitions}`);
  }
  const value = { prefix: "", partitions };
  return create(plan.PlanInputSchema, { plan: { case: "create", value } });
}

function openInput(): plan.PlanInput {
  const value = { prefix: "" };
  return create(plan.PlanInputSchema, { plan: { case: "open", value } });
}

/** A promise rejected when a worker of the namespace dies, for the waits pending until then. */
function deathSignal(): PromiseWithResolvers<never> {
  const signal = Promise.withResolvers<never>();
  signal.promise.catch(() => {});
  return signal;
}

let openedOn: (store: Store, input: plan.PlanInput) => Promise<Substantial>;
let internalWorker: (
  sub: Substantial,
  workflows: Iterable<AnyWorkflow>,
  options: InternalOptions,
) => Worker;

/** An opened namespace: the store, its partitions, and the workers of this process serving
 * it. */
export class Substantial {
  readonly partitions: number;
  readonly #module: CoreModule;
  readonly #core: Core;
  readonly #store: Store;
  readonly #owned: boolean;
  readonly #workers: Set<Worker> = new Set();
  #death = deathSignal();

  private constructor(module: CoreModule, store: Store, partitions: number, owned: boolean) {
    this.#module = module;
    this.#core = module.instantiate();
    this.#store = store;
    this.partitions = partitions;
    this.#owned = owned;
  }

  /** Creates the namespace the store URL names, or opens it if it exists with the same
   * `partitions`. */
  static async create(url: string, options: CreateOptions): Promise<Substantial> {
    const input = createInput(options);
    onlyOptions(options, ["partitions", "credentials"], "create", ConfigError);
    return Substantial.#fromUrl(url, options.credentials, input);
  }

  /** Opens the namespace the store URL names; one that does not exist is a `LayoutError`. */
  static async open(url: string, options: OpenOptions = {}): Promise<Substantial> {
    onlyOptions(options, ["credentials"], "open", ConfigError);
    return Substantial.#fromUrl(url, options.credentials, openInput());
  }

  static #fromUrl(
    url: string,
    credentials: OpenOptions["credentials"],
    input: plan.PlanInput,
  ): Promise<Substantial> {
    if (typeof url !== "string") throw new ConfigError(`a store is a URL string, not ${url}`);
    return Substantial.#opened(openStore(url, credentials), true, input);
  }

  static async #opened(store: Store, owned: boolean, input: plan.PlanInput): Promise<Substantial> {
    try {
      const module = await CoreModule.shipped();
      const outcome = outcomeOf(
        "open",
        await runPlan(module.instantiate(), store, input),
        "opened",
      );
      const opened = outcome.opened;
      if (opened.case === "layoutError") throw new LayoutError(opened.value);
      if (opened.case === "failed") throw new StoreError(opened.value.message);
      if (opened.case !== "partitions") throw new LayoutError("empty open outcome");
      return new Substantial(module, store, opened.value, owned);
    } catch (e) {
      if (owned) await store.close();
      throw e;
    }
  }

  static {
    openedOn = (store, input) => Substantial.#opened(store, false, input);
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

  /** A worker serving `workflows` on every partition, with the settings of `preset` and these
   * overrides, running from now on. */
  worker(workflows: Iterable<AnyWorkflow>, options: WorkerOptions): Worker {
    return this.#worker(workflows, options, false);
  }

  #worker(workflows: Iterable<AnyWorkflow>, options: InternalOptions, internal: boolean): Worker {
    const settings = resolveSettings(options, this.#store.capabilities.wait, internal);
    const served = new Workflows(workflows);
    const core = this.#module.instantiate();
    const worker: Worker = new Worker(core, this.#store, served, this.partitions, settings, {
      stopped: () => this.#workers.delete(worker),
      died: (error) => {
        const death = this.#death;
        this.#death = deathSignal();
        death.reject(error);
      },
    });
    this.#workers.add(worker);
    return worker;
  }
  /** Starts a run of `workflow`, idempotently under `id`. */
  async start<I, O>(
    workflow: Workflow<I, O>,
    input?: I,
    options: StartOptions = {},
  ): Promise<Run<O>> {
    if (!(workflow instanceof Workflow)) {
      throw new InvalidArgument(`start takes a workflow, not ${workflow}`);
    }
    const object = input === undefined ? {} : input;
    if (typeof object !== "object" || object === null || Array.isArray(object)) {
      throw new InvalidArgument(`an input is a JSON object, not ${JSON.stringify(object)}`);
    }
    onlyOptions(options, ["id"], "start");
    checkKey("workflow name", workflow.name);
    const at = now();
    const chain = options.id === undefined ? ulid(Number(at.ms)) : checkKey("run id", options.id);
    const value = {
      prefix: "",
      partitions: this.partitions,
      workflow: workflow.name,
      ...(workflow.version === undefined ? {} : { version: workflow.version }),
      input: jsonValue(encodeJson(object, "the input")),
      id: chain,
      now: at,
    };
    const plan_ = create(plan.PlanInputSchema, { plan: { case: "start", value } });
    const started = outcomeOf("start", await this.#plan(plan_), "started").started;
    if (started.case === "runExists") {
      throw new RunExists(`id ${chain} is taken by another workflow, version or input`);
    }
    if (started.case === "failed") throw new StoreError(started.value.message);
    this.#wake();
    return this.run(workflow, chain);
  }

  /** A handle on an existing chain. */
  run<I, O>(workflow: Workflow<I, O>, id: string): Run<O> {
    const namespace = {
      partitions: this.partitions,
      store: this.#store,
      plan: (input: plan.PlanInput) => this.#plan(input),
      wake: () => this.#wake(),
      died: () => this.#death.promise,
    };
    return new Run<O>(namespace, workflow, id);
  }

  #plan(input: plan.PlanInput): ReturnType<typeof runPlan> {
    return runPlan(this.#core, this.#store, input);
  }

  /** Wakes the workers of this process, which then list at once instead of after `maxIdle`. */
  #wake(): void {
    for (const worker of this.#workers) wake(worker);
  }
}

/** Creates the namespace on a store object its caller keeps: for tests. */
export async function createOn(
  store: Store,
  options: { readonly partitions: number },
): Promise<Substantial> {
  return openedOn(store, createInput(options));
}

/** Opens the namespace on a store object its caller keeps: for tests. */
export async function openOn(store: Store): Promise<Substantial> {
  return openedOn(store, openInput());
}

/** A worker taking every setting, the hidden ones included: for tests. */
export function workerWith(
  sub: Substantial,
  workflows: Iterable<AnyWorkflow>,
  options: InternalOptions,
): Worker {
  return internalWorker(sub, workflows, options);
}
