// Workflows (I-5), the retry policy and permanent errors of steps (I-6).
import { create } from "@bufbuild/protobuf";
import type { Context } from "./context.ts";
import { type Budget, budgetProto } from "./cost.ts";
import { Duration } from "./durations.ts";
import { ConfigError, InvalidArgument, onlyOptions } from "./errors.ts";
import { checkKey } from "./names.ts";
import { run } from "./proto.ts";
import { checkRetention } from "./settings.ts";

const U32 = 2 ** 32;

/** Exponential backoff for a step: every field is required (I-6). */
export interface Retry {
  readonly maxAttempts: number;
  readonly initial: Duration;
  readonly max: Duration;
  readonly multiplier: number;
  /** From the start of the first attempt: no attempt starts past it, and each runs at most until
   * it; none when not given. */
  readonly deadline?: Duration;
}

/** The policy of a step's `retry`, checked. */
export function retryPolicy(retry: Retry): run.RetryPolicy {
  onlyOptions(retry, ["maxAttempts", "initial", "max", "multiplier", "deadline"], "retry");
  const { maxAttempts, initial, max, multiplier, deadline } = retry;
  if (deadline !== undefined && !(deadline instanceof Duration && deadline.ms > 0)) {
    throw new InvalidArgument(`a retry's deadline is a positive Duration, not ${deadline}`);
  }
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
    ...(deadline === undefined ? {} : { deadlineMs: BigInt(deadline.ms) }),
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
  /** The longest an attempt of its steps without their own may go without `s.heartbeat()`;
   * none unless set. */
  readonly heartbeatTimeout?: Duration;
  /** How many whole days its runs are kept after they end, over the worker's `retention`; under 1
   * or over 36,500 is a `ConfigError` when a worker starts with it. */
  readonly retention?: number;
  /** The budget of its runs started without one of their own (I-28). */
  readonly budget?: Budget;
}

export type WorkflowFn<I, O> = (c: Context, input: I) => O | PromiseLike<O>;

/** A workflow: a name, an optional version, and a function of a context and a JSON object. */
export class Workflow<I = Record<string, never>, O = unknown> {
  readonly name: string;
  readonly version: number | undefined;
  readonly stepTimeout: Duration | undefined;
  readonly heartbeatTimeout: Duration | undefined;
  readonly retention: number | undefined;
  readonly budget: Budget | undefined;
  readonly fn: WorkflowFn<I, O>;

  constructor(name: string, fn: WorkflowFn<I, O>, options: WorkflowOptions = {}) {
    checkKey("workflow name", name);
    if (typeof fn !== "function") throw new InvalidArgument(`a workflow is a function, not ${fn}`);
    const known = ["version", "stepTimeout", "heartbeatTimeout", "retention", "budget"];
    onlyOptions(options, known, "workflow");
    const { version, stepTimeout, heartbeatTimeout, retention, budget } = options;
    if (version !== undefined && (!Number.isInteger(version) || version < 0 || version >= U32)) {
      throw new InvalidArgument(`a workflow version is an integer in [0, 2**32), not ${version}`);
    }
    if (stepTimeout !== undefined && !(stepTimeout instanceof Duration && stepTimeout.ms > 0)) {
      throw new InvalidArgument(
        `a workflow's stepTimeout is a positive Duration, not ${stepTimeout}`,
      );
    }
    if (
      heartbeatTimeout !== undefined &&
      !(heartbeatTimeout instanceof Duration && heartbeatTimeout.ms > 0)
    ) {
      throw new InvalidArgument(
        `a workflow's heartbeatTimeout is a positive Duration, not ${heartbeatTimeout}`,
      );
    }
    if (retention !== undefined && typeof retention !== "number") {
      throw new InvalidArgument(`a workflow's retention is whole days, not ${String(retention)}`);
    }
    if (budget !== undefined) budgetProto(budget);
    this.name = name;
    this.version = version;
    this.stepTimeout = stepTimeout;
    this.heartbeatTimeout = heartbeatTimeout;
    this.retention = retention;
    this.budget = budget === undefined ? undefined : Object.freeze({ ...budget });
    this.fn = fn;
  }

  toString(): string {
    return `workflow ${this.name}${this.version === undefined ? "" : ` version ${this.version}`}`;
  }
}

/** `workflow("name", fn, { version, stepTimeout, heartbeatTimeout, retention, budget })`: the name is given, as
 * bundlers rename functions. */
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

  constructor(workflows: Iterable<AnyWorkflow>) {
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
    const retentions = new Map<string, number | undefined>();
    for (const w of this.#byKey.values()) {
      const what = `the retention of workflow ${JSON.stringify(w.name)}`;
      if (w.retention !== undefined) checkRetention(w.retention, what);
      if (!retentions.has(w.name)) retentions.set(w.name, w.retention);
      // § one pnctq6tx8.a@7yznt
      if (retentions.get(w.name) !== w.retention) {
        throw new ConfigError(
          `the versions of workflow ${JSON.stringify(w.name)} give different retentions`,
        );
      }
    }
  }

  // § pjrbw31h4@s65nz
  static #key(name: string, version: number | undefined): string {
    return `${name}/${version ?? ""}`;
  }

  get(name: string, version: number | undefined): AnyWorkflow | undefined {
    return this.#byKey.get(Workflows.#key(name, version));
  }
  // § end pjrbw31h4

  /** Each name served: its versions, whether its runs without a version are, and the retention
   * its workflows give. */
  served(): {
    name: string;
    versions: number[];
    unversioned: boolean;
    retention: number | undefined;
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
