// Worker settings (I-18): a preset names every setting, the only defaults there are.
import { Duration, days, hours, milliseconds, minutes, seconds } from "./durations.ts";
import { ConfigError } from "./errors.ts";

const KIB = 1024;
const MIB = 1024 * KIB;
const U32 = 0xffff_ffff;
export const PRESETS = ["script", "service"] as const;
export type Preset = (typeof PRESETS)[number];

/** A run kept until its namespace is deleted; registered with `Symbol.for`, so that two loaded
 * copies of the package agree on it. */
export const FOREVER: unique symbol = Symbol.for("substantial.forever");

/** How long a run is kept after its chain's last run ends, then collected (I-16). */
export type Retention = Duration | typeof FOREVER;

/** The least retention of the presets; a `result` wait follows its chain again within half of
 * it. */
export const RETENTION_MIN = hours(1);

/** The longest retention, 100 years; longer is `FOREVER`. A collection's due key names its year
 * in 4 digits, which far longer retentions would pass. */
export const RETENTION_MAX = days(36_500);

/** Refuses a retention that is neither a Duration from `minimum` to `RETENTION_MAX` nor `FOREVER`. */
export function checkRetention(retention: unknown, minimum: Duration, what: string): void {
  if (retention === FOREVER) return;
  if (!(retention instanceof Duration)) {
    throw new ConfigError(`${what} must be a Duration or s.FOREVER, not ${String(retention)}`);
  }
  if (retention.ms < minimum.ms) {
    throw new ConfigError(
      `${what} must be at least ${minimum.ms} ms or forever, not ${retention.ms} ms`,
    );
  }
  if (retention.ms > RETENTION_MAX.ms) {
    throw new ConfigError(
      `${what} must be at most ${RETENTION_MAX.ms} ms or forever, not ${retention.ms} ms`,
    );
  }
}

/** Adapter retries of transient failures: `attempts` in all, backoff doubling from `initial`
 * to `max`. */
export interface StoreRetry {
  readonly attempts: number;
  readonly initial: Duration;
  readonly max: Duration;
}

/** The compression threshold: whichever of the two the history passes first. */
interface ContinueAsNewAfter {
  readonly segments: number;
  readonly bytes: number;
}

/** Every setting of a worker; only those of `WorkerOptions` are public (D8). */
export interface Settings {
  readonly concurrency: number;
  readonly commitEvery: Duration;
  readonly shutdownGrace: Duration;
  readonly claimTtl: Duration;
  readonly claimSkew: Duration;
  readonly maxIdle: Duration;
  readonly wait: Duration;
  readonly duePage: number;
  readonly duePages: number;
  readonly snapshotEvery: number;
  readonly continueAsNewAfter: ContinueAsNewAfter;
  readonly dedupWindow: Duration;
  readonly stepTimeout: Duration;
  readonly retention: Retention;
  readonly tombstone: Duration;
  readonly retentionMin: Duration;
  readonly recheck: Duration;
}

/** A preset and the settings it overrides; `continueAsNewAfter: 0` compresses at every
 * boundary. */
export interface WorkerOptions {
  readonly preset: Preset;
  readonly concurrency?: number;
  readonly maxIdle?: Duration;
  readonly shutdownGrace?: Duration;
  readonly claimTtl?: Duration;
  /** The timeout of each step attempt, unless its step or its workflow gives one. */
  readonly stepTimeout?: Duration;
  /** Whichever of the two the history passes first. */
  readonly continueAsNewAfter?: { readonly segments: number; readonly bytes: number } | 0;
  /** How long a run is kept after its chain ends, unless its workflow gives one: at least an
   * hour and at most 100 years, or `FOREVER`. */
  readonly retention?: Retention;
}

const PUBLIC = [
  "concurrency",
  "maxIdle",
  "shutdownGrace",
  "claimTtl",
  "continueAsNewAfter",
  "stepTimeout",
  "retention",
];

/** Every setting, overrides of the hidden ones included: for tests only. */
export type InternalOptions = { readonly preset: Preset } & {
  readonly [K in keyof Settings]?: K extends "continueAsNewAfter"
    ? ContinueAsNewAfter | 0
    : Settings[K];
};

/** The adapters' retries of transient failures (I-26). */
export const STORE_RETRY: StoreRetry = Object.freeze({
  attempts: 5,
  initial: milliseconds(100),
  max: seconds(5),
});

/** Every setting of `preset`; `waitingStore` is whether the store's listings can wait. */
export function presetSettings(preset: Preset, waitingStore: boolean): Settings {
  if (!PRESETS.includes(preset)) {
    throw new ConfigError(`unknown preset ${JSON.stringify(preset)}, expected one of ${PRESETS}`);
  }
  const service = preset === "service";
  return {
    concurrency: service ? 16 : 1,
    commitEvery: milliseconds(0),
    shutdownGrace: seconds(service ? 25 : 5),
    claimTtl: seconds(30),
    claimSkew: seconds(5),
    maxIdle: !service ? seconds(1) : waitingStore ? minutes(5) : seconds(30),
    wait: seconds(20),
    duePage: 100,
    duePages: 10,
    snapshotEvery: 64,
    continueAsNewAfter: { segments: 1000, bytes: 8 * MIB },
    dedupWindow: days(7),
    stepTimeout: minutes(5),
    retention: days(30),
    tombstone: days(7),
    retentionMin: RETENTION_MIN,
    recheck: days(1),
  };
}

const INTS = ["concurrency", "duePage", "duePages", "snapshotEvery"];
const POSITIVE = [
  "concurrency",
  "duePage",
  "duePages",
  "snapshotEvery",
  "claimTtl",
  "maxIdle",
  "stepTimeout",
  "tombstone",
  "retentionMin",
  "recheck",
];
const DURATIONS = [
  "commitEvery",
  "shutdownGrace",
  "claimTtl",
  "claimSkew",
  "maxIdle",
  "wait",
  "dedupWindow",
  "stepTimeout",
  "tombstone",
  "retentionMin",
  "recheck",
];

function isCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function exactly(value: unknown, keys: string[], what: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null) {
    throw new ConfigError(`${what} must be an object of ${keys.join(", ")}`);
  }
  const given = Object.keys(value).sort();
  if (given.join() !== [...keys].sort().join()) {
    throw new ConfigError(`${what} takes exactly ${keys.join(", ")}, not ${given.join(", ")}`);
  }
  return value as Record<string, unknown>;
}

function validate(s: Record<string, unknown>): Settings {
  for (const name of INTS) {
    if (!isCount(s[name])) {
      throw new ConfigError(`${name} must be a non-negative integer, not ${String(s[name])}`);
    }
  }
  for (const name of DURATIONS) {
    if (!(s[name] instanceof Duration)) {
      throw new ConfigError(`${name} must be a Duration, not ${String(s[name])}`);
    }
  }
  for (const name of POSITIVE) {
    const value = s[name];
    if ((value instanceof Duration ? value.ms : value) === 0) {
      throw new ConfigError(`${name} must be positive`);
    }
  }
  for (const name of ["concurrency", "duePage", "duePages"]) {
    if ((s[name] as number) > U32) throw new ConfigError(`${name} is too large`);
  }
  if ((s.duePage as number) > 1000) {
    throw new ConfigError(`duePage must be at most 1000, an S3 page: ${s.duePage}`);
  }
  const wait = (s.wait as Duration).ms;
  if (wait < 1000 || wait > 20_000 || wait % 1000 !== 0) {
    throw new ConfigError(`wait must be whole seconds from 1 to 20, not ${s.wait}`);
  }
  const c = exactly(s.continueAsNewAfter, ["segments", "bytes"], "continueAsNewAfter");
  if (!isCount(c.segments) || !isCount(c.bytes)) {
    throw new ConfigError("continueAsNewAfter segments and bytes must be non-negative integers");
  }
  checkRetention(s.retention, s.retentionMin as Duration, "retention");
  return s as unknown as Settings;
}

/** The settings of a worker: its preset's, with the overrides given; an unknown name or an
 * invalid value is refused. Only the public settings are known unless `internal`. */
export function resolveSettings(
  options: InternalOptions,
  waitingStore: boolean,
  internal: boolean,
): Settings {
  if (typeof options !== "object" || options === null || !("preset" in options)) {
    throw new ConfigError("a worker needs a preset, script or service");
  }
  const { preset, ...overrides } = options;
  const base = presetSettings(preset, waitingStore) as unknown as Record<string, unknown>;
  const unknown = Object.keys(overrides).filter(
    (name) => !(name in base) || !(internal || PUBLIC.includes(name)),
  );
  if (unknown.length > 0) {
    throw new ConfigError(`unknown settings ${JSON.stringify(unknown)}`);
  }
  const merged: Record<string, unknown> = { ...base, ...overrides };
  if (merged.continueAsNewAfter === 0) {
    merged.continueAsNewAfter = { segments: 0, bytes: 0 };
  }
  for (const [name, value] of Object.entries(overrides)) {
    if (value === undefined) throw new ConfigError(`${name} is given as undefined`);
  }
  return validate(merged);
}
