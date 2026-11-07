// Workflows (I-5), the retry policy and permanent errors of steps (I-6).
import { create } from "@bufbuild/protobuf";
import type { Context } from "./context.ts";
import { Duration } from "./durations.ts";
import { ConfigError, InvalidArgument, onlyOptions } from "./errors.ts";
import { checkKey } from "./names.ts";
import { run } from "./proto.ts";
import { checkRetention, FOREVER, type Retention } from "./settings.ts";

const U32 = 2 ** 32;

/** Exponential backoff for a step: every field is required (I-6). */
export interface Retry {
  readonly maxAttempts: number;
  readonly initial: Duration;
  readonly max: Duration;
  readonly multiplier: number;
}

/** The policy of a step's `retry`, checked. */
export function retryPolicy(retry: Retry): run.RetryPolicy {
  onlyOptions(retry, ["maxAttempts", "initial", "max", "multiplier"], "retry");
  const { maxAttempts, initial, max, multiplier } = retry;
  if (!Number.isInteger(maxAttempts) || maxAttempts <= 0 || maxAttempts >= U32) {
    throw new InvalidArgument(`maxAttempts must be a positive integer: ${maxAttempts}`);
  }
  if (!(initial instanceof Duration) || !(max instanceof Duration)) {
    throw new InvalidArgument("initial and max are Durations");
  }
  if (max.ms < initial.ms) {
    throw new InvalidArgument(`invalid backoff from ${initial} to ${max}`);
  }
  if (typeof multiplier !== "number" || !Number.isFinite(multiplier) || multiplier < 1) {
    throw new InvalidArgument(`multiplier must be a number of at least 1: ${multiplier}`);
  }
  return create(run.RetryPolicySchema, {
    maxAttempts,
    initialMs: BigInt(initial.ms),
    maxMs: BigInt(max.ms),
    multiplier,
  });
}

/** An error that fails a step at once, attempts left or not. */
export class Permanent extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

export interface WorkflowOptions {
  /** A run started with a version is replayed only by the code of that version. */
  readonly version?: number;
  /** The timeout of each attempt of its steps without their own, over the worker's. */
  readonly stepTimeout?: Duration;
  /** How long its runs are kept after their chain ends, over the worker's `retention`. */
  readonly retention?: Retention;
}

export type WorkflowFn<I, O> = (c: Context, input: I) => O | PromiseLike<O>;

/** A workflow: a name, an optional version, and a function of a context and a JSON object. */
export class Workflow<I = Record<string, never>, O = unknown> {
  readonly name: string;
  readonly version: number | undefined;
  readonly stepTimeout: Duration | undefined;
  readonly retention: Retention | undefined;
  readonly fn: WorkflowFn<I, O>;

  constructor(name: string, fn: WorkflowFn<I, O>, options: WorkflowOptions = {}) {
    checkKey("workflow name", name);
    if (typeof fn !== "function") throw new InvalidArgument(`a workflow is a function, not ${fn}`);
    onlyOptions(options, ["version", "stepTimeout", "retention"], "workflow");
    const { version, stepTimeout, retention } = options;
    if (version !== undefined && (!Number.isInteger(version) || version < 0 || version >= U32)) {
      throw new InvalidArgument(`a workflow version is an integer in [0, 2**32), not ${version}`);
    }
    if (stepTimeout !== undefined && !(stepTimeout instanceof Duration && stepTimeout.ms > 0)) {
      throw new InvalidArgument(
        `a workflow's stepTimeout is a positive Duration, not ${stepTimeout}`,
      );
    }
    if (retention !== undefined && retention !== FOREVER && !(retention instanceof Duration)) {
      throw new InvalidArgument(
        `a workflow's retention is a Duration or s.FOREVER, not ${String(retention)}`,
      );
    }
    this.name = name;
    this.version = version;
    this.stepTimeout = stepTimeout;
    this.retention = retention;
    this.fn = fn;
  }

  toString(): string {
    return `workflow ${this.name}${this.version === undefined ? "" : ` version ${this.version}`}`;
  }
}

/** `workflow("name", fn, { version, stepTimeout, retention })`: the name is given, as bundlers rename functions. */
export function workflow<I = Record<string, never>, O = unknown>(
  name: string,
  fn: WorkflowFn<I, O>,
  options?: WorkflowOptions,
): Workflow<I, O> {
  return new Workflow(name, fn, options);
}

// biome-ignore lint/suspicious/noExplicitAny: a worker serves workflows of any input and result.
export type AnyWorkflow = Workflow<any, any>;

/** The workflows a worker serves, by name and version. */
export class Workflows {
  readonly #byKey = new Map<string, AnyWorkflow>();

  /** The workflows of a worker whose least retention is `retentionMin`. */
  constructor(workflows: Iterable<AnyWorkflow>, retentionMin: Duration) {
    if (workflows === null || typeof workflows !== "object" || !(Symbol.iterator in workflows)) {
      throw new ConfigError("a worker serves a list of workflows");
    }
    for (const w of workflows) {
      if (!(w instanceof Workflow)) throw new ConfigError(`a worker serves workflows, not ${w}`);
      checkKey("workflow name", w.name);
      const key = Workflows.#key(w.name, w.version);
      if (this.#byKey.has(key)) throw new ConfigError(`${w} given twice`);
      this.#byKey.set(key, w);
    }
    if (this.#byKey.size === 0) throw new ConfigError("a worker needs at least one workflow");
    // The core keeps one retention per name.
    const retentions = new Map<string, Retention | undefined>();
    for (const w of this.#byKey.values()) {
      const what = `the retention of workflow ${JSON.stringify(w.name)}`;
      if (w.retention !== undefined) checkRetention(w.retention, retentionMin, what);
      if (!retentions.has(w.name)) retentions.set(w.name, w.retention);
      if (!sameRetention(retentions.get(w.name), w.retention)) {
        throw new ConfigError(
          `the versions of workflow ${JSON.stringify(w.name)} give different retentions`,
        );
      }
    }
  }

  static #key(name: string, version: number | undefined): string {
    return `${name}/${version ?? ""}`;
  }

  get(name: string, version: number | undefined): AnyWorkflow | undefined {
    return this.#byKey.get(Workflows.#key(name, version));
  }

  /** Each name served: its versions, whether its runs without a version are, and the retention
   * its workflows give. */
  served(): {
    name: string;
    versions: number[];
    unversioned: boolean;
    retention: Retention | undefined;
  }[] {
    const all = [...this.#byKey.values()];
    const names = [...new Set(all.map((w) => w.name))].sort();
    return names.map((name) => {
      const mine = all.filter((w) => w.name === name);
      return {
        name,
        versions: mine
          .flatMap((w) => (w.version === undefined ? [] : [w.version]))
          .sort((a, b) => a - b),
        unversioned: mine.some((w) => w.version === undefined),
        retention: mine[0]?.retention,
      };
    });
  }
}

function sameRetention(a: Retention | undefined, b: Retention | undefined): boolean {
  return a instanceof Duration && b instanceof Duration ? a.ms === b.ms : a === b;
}
