// Schedules: `s.Cron`, the fire times of a cron expression in a time zone, and `c.schedule()`, the
// loop that fires them, configured by the `schedule` message `sub.schedule` sends.
//
// The cron arithmetic is the library's own, the same in every host: a local time a clock change
// skips fires at the end of the gap, and a local time it repeats fires at its first occurrence.
import type { Context } from "./context.ts";
import { Duration } from "./durations.ts";
import { InvalidArgument, ProtocolError } from "./errors.ts";
import { RESERVED_STEP as NEXT } from "./names.ts";
import type { Shared } from "./scheduler.ts";
import { callStep } from "./step.ts";

/** The message that carries a schedule's config. */
export const MESSAGE = "schedule";
/** How many fired instants the published `schedule` value keeps. */
const RECENT = 10;

const MINUTE = 60_000;
const HOUR = 3_600_000;
const DAY = 86_400_000;
/** A cron expression none of whose fires comes within this many years never fires. */
const YEARS = 8;

const MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
const DAYS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
const FIELDS: [string, number, number, Map<string, number>][] = [
  ["minute", 0, 59, new Map()],
  ["hour", 0, 23, new Map()],
  ["day of month", 1, 31, new Map()],
  ["month", 1, 12, new Map(MONTHS.map((m, i) => [m, i + 1]))],
  ["day of week", 0, 7, new Map(DAYS.map((d, i) => [d, i]))],
];
const NUMBER = /^[0-9]+$/;

function value(expr: string, field: string, text: string, names: Map<string, number>): number {
  if (NUMBER.test(text)) return Number(text);
  const named = names.get(text.toLowerCase());
  if (named !== undefined) return named;
  throw new InvalidArgument(
    `cron ${JSON.stringify(expr)}: ${JSON.stringify(text)} is not a ${field}`,
  );
}

function partition(text: string, sep: string): [string, boolean, string] {
  const i = text.indexOf(sep);
  return i < 0 ? [text, false, ""] : [text.slice(0, i), true, text.slice(i + 1)];
}

function field(
  expr: string,
  text: string,
  [name, lo, hi, names]: [string, number, number, Map<string, number>],
): Set<number> {
  const values = new Set<number>();
  for (const item of text.split(",")) {
    const [span, slash, stepText] = partition(item, "/");
    let step = 1;
    if (slash) {
      if (!NUMBER.test(stepText) || Number(stepText) === 0) {
        throw new InvalidArgument(
          `cron ${JSON.stringify(expr)}: the step ${JSON.stringify(stepText)} is not positive`,
        );
      }
      step = Number(stepText);
    }
    let a: number;
    let b: number;
    if (span === "*") {
      a = lo;
      b = hi;
    } else if (span.includes("-")) {
      const [first, , last] = partition(span, "-");
      a = value(expr, name, first, names);
      b = value(expr, name, last, names);
    } else {
      a = value(expr, name, span, names);
      b = slash ? hi : a;
    }
    if (!(lo <= a && a <= b && b <= hi)) {
      throw new InvalidArgument(
        `cron ${JSON.stringify(expr)}: ${JSON.stringify(item)} is not a ${name} range of ${lo}-${hi}`,
      );
    }
    for (let v = a; v <= b; v += step) values.add(v);
  }
  return values;
}

function zone(tz: string): Intl.DateTimeFormat {
  // Intl also takes offsets such as `+01:00`, which are not zones of the IANA database.
  if (!/^[+\-−]/.test(tz)) {
    try {
      const format = new Intl.DateTimeFormat("en-US", {
        timeZone: tz,
        hourCycle: "h23",
        year: "numeric",
        month: "numeric",
        day: "numeric",
        hour: "numeric",
        minute: "numeric",
        second: "numeric",
      });
      // Intl ignores case, where the database's names, as Python and Rust read them, do not.
      const resolved = format.resolvedOptions().timeZone;
      if (resolved === tz || resolved.toLowerCase() !== tz.toLowerCase()) return format;
    } catch (e) {
      if (!(e instanceof RangeError)) throw e;
    }
  }
  throw new InvalidArgument(`${JSON.stringify(tz)} is not a time zone of the IANA database`);
}

/** A cron expression of five fields (minute, hour, day of month, month, day of week) and the IANA
 * time zone it reads its times in: `new s.Cron("0 3 * * *", "Europe/Zurich")`. */
export class Cron {
  readonly expr: string;
  readonly tz: string;

  constructor(expr: string, tz: string) {
    if (typeof expr !== "string" || typeof tz !== "string") {
      throw new InvalidArgument(
        `a cron takes an expression and a time zone, strings: ${String(expr)}, ${String(tz)}`,
      );
    }
    compile(expr, tz);
    this.expr = expr;
    this.tz = tz;
  }

  toString(): string {
    return `Cron(${JSON.stringify(this.expr)}, ${JSON.stringify(this.tz)})`;
  }
}

interface Compiled {
  readonly minutes: Set<number>;
  readonly hours: Set<number>;
  readonly days: Set<number>;
  readonly months: Set<number>;
  readonly weekdays: Set<number>;
  /** Vixie cron: when either day field starts with `*`, a day matches both, otherwise either. */
  readonly either: boolean;
  readonly zone: Intl.DateTimeFormat;
}

/** Whether the day of the local time `w` (ms, read as UTC) matches. */
function day(c: Compiled, w: Date): boolean {
  const dom = c.days.has(w.getUTCDate());
  const dow = c.weekdays.has(w.getUTCDay());
  return c.either ? dom || dow : dom && dow;
}

function compile(expr: string, tz: string): Compiled {
  const fields = expr.split(/\s+/).filter((f) => f !== "");
  if (fields.length !== 5) {
    throw new InvalidArgument(
      `cron ${JSON.stringify(expr)} has ${fields.length} fields, not 5: minute hour day-of-month month day-of-week`,
    );
  }
  const sets = fields.map((text, i) => field(expr, text, FIELDS[i] as (typeof FIELDS)[number]));
  const [minutes, hours, days, months, dows] = sets as [
    Set<number>,
    Set<number>,
    Set<number>,
    Set<number>,
    Set<number>,
  ];
  const weekdays = new Set([...dows].map((d) => d % 7));
  const either = !fields[2]?.startsWith("*") && !fields[4]?.startsWith("*");
  const c = { minutes, hours, days, months, weekdays, either, zone: zone(tz) };
  nextWall(c, Date.UTC(2000, 0, 1), expr);
  return c;
}

/** The first local time at or after `w` (ms, read as UTC), on the minute, that matches. */
function nextWall(c: Compiled, wall: number, expr: string): number {
  let w = wall;
  const limit = new Date(w).getUTCFullYear() + YEARS;
  for (let d = new Date(w); d.getUTCFullYear() <= limit; d = new Date(w)) {
    const [year, month, date, hour] = [
      d.getUTCFullYear(),
      d.getUTCMonth(),
      d.getUTCDate(),
      d.getUTCHours(),
    ];
    if (!c.months.has(month + 1)) w = Date.UTC(year, month + 1, 1);
    else if (!day(c, d)) w = Date.UTC(year, month, date) + DAY;
    else if (!c.hours.has(hour)) w = Date.UTC(year, month, date, hour) + HOUR;
    else if (!c.minutes.has(d.getUTCMinutes())) w = Math.floor(w / MINUTE) * MINUTE + MINUTE;
    else return w;
  }
  throw new InvalidArgument(`cron ${JSON.stringify(expr)} never fires`);
}

/** The offset of `zone` at the instant `t`, in ms. */
function offset(zone: Intl.DateTimeFormat, t: number): number {
  const s = Math.floor(t / 1000) * 1000;
  const parts: Record<string, number> = {};
  for (const p of zone.formatToParts(s)) if (p.type !== "literal") parts[p.type] = Number(p.value);
  const { year, month, day, hour, minute, second } = parts as Record<string, number>;
  return Date.UTC(year as number, (month as number) - 1, day, hour, minute, second) - s;
}

/** The instant of the local time `w` (ms, read as UTC): the earlier of two in a repeated hour,
 * the end of the gap for a time a clock change skips. */
function resolve(zone: Intl.DateTimeFormat, w: number): number {
  const before = offset(zone, w - DAY);
  const after = offset(zone, w + DAY);
  const valid = [...new Set([before, after])]
    .filter((o) => offset(zone, w - o) === o)
    .map((o) => w - o);
  if (valid.length > 0) return Math.min(...valid);
  let lo = w - Math.max(before, after);
  let hi = w - Math.min(before, after);
  const first = offset(zone, lo);
  while (hi - lo > 1) {
    const mid = Math.floor((lo + hi) / 2);
    if (offset(zone, mid) === first) lo = mid;
    else hi = mid;
  }
  return hi;
}

/** The first fire time of `cron` strictly after `after`, both in ms since the epoch. */
export function nextFire(cron: Cron, after: number): number {
  return next(compile(cron.expr, cron.tz), cron.expr, after);
}

function next(c: Compiled, expr: string, after: number): number {
  // No local time before the earliest reading of `after` can resolve after it.
  const least = Math.min(...[-DAY, 0, DAY].map((d) => offset(c.zone, after + d)));
  let w = Math.floor((after + least) / MINUTE) * MINUTE;
  for (;;) {
    w = nextWall(c, w, expr);
    const t = resolve(c.zone, w);
    if (t > after) return t;
    w += MINUTE;
  }
}

/** The form of instants in a schedule's values: `2026-11-15T03:00:00.000Z`. */
export function iso(ms: number): string {
  return new Date(ms).toISOString();
}

export function parse(text: string): number {
  const ms = typeof text === "string" ? Date.parse(text) : Number.NaN;
  if (Number.isNaN(ms)) throw new ProtocolError(`a schedule instant ${JSON.stringify(text)}`);
  return ms;
}

export interface Skipped {
  count: number;
  oldest: string | null;
  newest: string | null;
}

export interface Plan {
  due: string[];
  skipped: Skipped;
  next: string;
}

/** The fires due by `now` after `after`, at most the `backfill` most recent, those before them
 * skipped, and the next fire after `now`. */
export function plan(expr: string, tz: string, after: string, now: string, backfill: number): Plan {
  const c = compile(expr, tz);
  const end = parse(now);
  const due: number[] = [];
  let count = 0;
  let oldest: number | undefined;
  let newest: number | undefined;
  let t = parse(after);
  for (t = next(c, expr, t); t <= end; t = next(c, expr, t)) {
    due.push(t);
    if (due.length > backfill) {
      newest = due.shift() as number;
      oldest ??= newest;
      count += 1;
    }
  }
  const skipped = {
    count,
    oldest: oldest === undefined ? null : iso(oldest),
    newest: newest === undefined ? null : iso(newest),
  };
  return { due: due.map(iso), skipped, next: iso(t) };
}

/** The value of a `schedule` message. */
export interface Config {
  readonly revision: number;
  readonly cron: string | null;
  readonly tz: string | null;
  readonly backfill: number;
}

export function config(revision: number, cron: Cron | null, backfill: number): Config {
  return {
    revision,
    cron: cron === null ? null : cron.expr,
    tz: cron === null ? null : cron.tz,
    backfill,
  };
}

export function checkConfig(revision: unknown, cron: unknown, backfill: unknown): void {
  if (!Number.isSafeInteger(revision) || (revision as number) < 1) {
    throw new InvalidArgument(`a schedule's revision is a positive integer, not ${revision}`);
  }
  if (cron !== null && !(cron instanceof Cron)) {
    throw new InvalidArgument(`a schedule takes an s.Cron or null, not ${cron}`);
  }
  if (!Number.isSafeInteger(backfill) || (backfill as number) < 1) {
    throw new InvalidArgument(`backfill is a positive integer, not ${backfill}`);
  }
}

interface State {
  revision: number;
  cron: string | null;
  tz: string | null;
  backfill: number | null;
  after: string | null;
  next: string | null;
  pending: string[];
  last: string | null;
  recent: string[];
  skipped: Skipped;
}

function initial(): State {
  return {
    revision: 0,
    cron: null,
    tz: null,
    backfill: null,
    after: null,
    next: null,
    pending: [],
    last: null,
    recent: [],
    skipped: { count: 0, oldest: null, newest: null },
  };
}

function published(st: State): Record<string, unknown> {
  return {
    revision: st.revision,
    cron: st.cron,
    tz: st.tz,
    backfill: st.backfill,
    last: st.last,
    recent: st.recent,
    skipped: st.skipped,
    next: st.pending[0] ?? st.next,
  };
}

const KEYS = ["backfill", "cron", "revision", "tz"];

function received(value: unknown): [number, Cron | null, number] {
  const keys =
    typeof value === "object" && value !== null && !Array.isArray(value)
      ? Object.keys(value).sort()
      : [];
  if (keys.length !== KEYS.length || keys.some((k, i) => k !== KEYS[i])) {
    throw new InvalidArgument(
      `a schedule message is a config of sub.schedule, not ${JSON.stringify(value)}`,
    );
  }
  const v = value as Config;
  const cron = v.cron === null ? null : new Cron(v.cron, v.tz as string);
  if (cron === null && v.tz !== null) {
    throw new InvalidArgument(`a schedule message without cron has no tz: ${JSON.stringify(v)}`);
  }
  checkConfig(v.revision, cron, v.backfill);
  return [v.revision, cron, v.backfill];
}

/** Applies the config of the highest revision received, if higher than the state's. */
async function apply(c: Context, st: State, configs: unknown[]): Promise<void> {
  const all = configs.map(received);
  configs.length = 0;
  let highest = all[0] as [number, Cron | null, number];
  for (const r of all) if (r[0] > highest[0]) highest = r;
  const [revision, cron, backfill] = highest;
  if (revision <= st.revision) return;
  const expr = cron === null ? null : cron.expr;
  const tz = cron === null ? null : cron.tz;
  st.revision = revision;
  st.backfill = backfill;
  if (expr !== st.cron || tz !== st.tz) {
    // Another cron starts from now: it fires nothing it would have fired before.
    st.cron = expr;
    st.tz = tz;
    st.pending = [];
    st.next = null;
    st.after = cron === null ? null : iso((await c.now()).getTime());
    // The plan that follows publishes it, with its next fire.
    if (cron !== null) return;
  }
  c.publish(MESSAGE, published(st));
}

/** The loop of `c.schedule()`: its state is the loop's, its config the last `schedule` message
 * of a higher revision. */
export async function* schedule(c: Context, shared: Shared): AsyncGenerator<Date, void, undefined> {
  const configs: unknown[] = [];
  c.handle(MESSAGE, (v) => {
    configs.push(v);
  });
  for await (const st of c.iterate(initial())) {
    while (st.pending.length === 0) {
      if (configs.length > 0) {
        await apply(c, st, configs);
        continue;
      }
      if (st.cron === null) {
        await c.waitUntil(() => configs.length > 0);
        continue;
      }
      const now = (await c.now()).getTime();
      const args = [st.cron, st.tz, st.after, iso(now), st.backfill];
      const before = st.next;
      const fires = await callStep<Plan>(
        shared,
        NEXT,
        plan as (...args: unknown[]) => Plan,
        args,
        {},
      );
      const skipped = fires.skipped;
      if (fires.due.length > 0) {
        st.pending = fires.due;
        st.after = fires.due.at(-1) as string;
      }
      if (skipped.count > 0) {
        const was = st.skipped;
        st.skipped = {
          count: was.count + skipped.count,
          oldest: was.oldest ?? skipped.oldest,
          newest: skipped.newest,
        };
      }
      st.next = fires.next;
      if (st.pending.length === 0) {
        if (st.next !== before) c.publish(MESSAGE, published(st));
        const wait = new Duration(parse(st.next) - now);
        await c.waitUntil(() => configs.length > 0, { timeout: wait });
      }
    }
    const at = st.pending.shift() as string;
    st.last = at;
    st.recent = [...st.recent, at].slice(-RECENT);
    c.publish(MESSAGE, published(st));
    yield new Date(parse(at));
    // Before the loop's boundary: only its state crosses a compression.
    if (configs.length > 0) await apply(c, st, configs);
  }
}
