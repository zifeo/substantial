// What an environment and its clients share: the namespace, the virtual time, the workers, the
// holds, the seed and what Chaos injected.
import { fromBinary } from "@bufbuild/protobuf";
import { ulids } from "../plan.ts";
import { layout } from "../proto.ts";
import { dropNamespace, expiriesOf, MemoryStore, objectsOf } from "../stores/memory.ts";
import { pending } from "../worker.ts";
import type { Env } from "./env.ts";
import { type Chaos, derive, FaultStore, unit } from "./store.ts";
import { Time, type Waiter } from "./time.ts";
import { type Hold, type Pod, WorkerClock } from "./workers.ts";

const U64 = (1n << 64n) - 1n;

/** The phase the core draws from its seed, a worker's first idle wait, `maxIdle` the others:
 * SplitMix64 of the seed's first 8 bytes, big-endian, modulo `maxIdle`. */
export function idlePhase(seed: Uint8Array, maxIdle: number): number {
  const first = new DataView(seed.buffer, seed.byteOffset, 8).getBigUint64(0);
  let z = (first + 0x9e3779b97f4a7c15n) & U64;
  z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & U64;
  z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & U64;
  return Number((z ^ (z >> 31n)) % BigInt(maxIdle));
}

/** The earliest due entries of a workflow: of any kind, and of those that wake a run, a timer or
 * a message, which every due entry is. */
interface Due {
  readonly any: number;
  readonly wake: number | undefined;
}

function earlier(a: number | undefined, b: number | undefined): number | undefined {
  if (a === undefined) return b;
  return b === undefined ? a : Math.min(a, b);
}

/** The time of a due entry, `<yyyymmdd>t<hhmmssmmm>z~<chain>~<tag>`. */
export function dueMs(entry: string): number {
  const n = (from: number, to: number) => Number(entry.slice(from, to));
  return Date.UTC(n(0, 4), n(4, 6) - 1, n(6, 8), n(9, 11), n(11, 13), n(13, 15), n(15, 18));
}

/** Whether a head's run ended: its last segment holds an end. */
export function endedHead(head: layout.Head): boolean {
  const ends = ["completed", "failed", "cancelled", "continued"];
  return head.segments.at(-1)?.events.some((e) => ends.includes(e.kind.case ?? "")) ?? false;
}

export class World {
  readonly ns: string;
  readonly memory: MemoryStore;
  readonly #objects: ReadonlyMap<string, Uint8Array>;
  readonly #expiries: ReadonlyMap<string, number>;
  readonly time: Time;
  readonly chaos: Chaos | undefined;
  readonly seed: number;
  /** One core seed for every worker: a run's random then depends on the run alone. */
  readonly coreSeed: Uint8Array;
  readonly ulid: (ms: number) => string;
  readonly counts = { failed: 0, ambiguous: 0, crashed: 0 };
  writes = 0;
  outageUntil: number;
  readonly pods: Pod[] = [];
  readonly holds: Hold[] = [];
  readonly errors: unknown[] = [];
  /** The chains whose failure the test read through the environment. */
  readonly checked: Set<string> = new Set();
  /** Each run that ended failed, its chain and its failure, from the write of its result. */
  readonly failed: Map<string, { chain: string; failure: string }> = new Map();
  env!: Env;
  #due: { writes: number; due: Map<string, Due> } = { writes: -1, due: new Map() };
  /** For the replay tester: each run its replays checked against their history, with the
   * divergence they met; `undefined` in every other environment, whose replays go on. */
  checks: Map<string, string | undefined> | undefined;
  /** Off only in the test that compares a scenario with and without skipping. */
  skipping = true;
  skipped = 0;

  constructor(ns: string, ms: number, chaos: Chaos | undefined) {
    this.ns = ns;
    this.time = new Time(ms);
    this.memory = new MemoryStore(ns, () => this.time.ms);
    const objects = objectsOf(ns);
    const expiring = expiriesOf(ns);
    if (objects === undefined || expiring === undefined) throw new Error(`no namespace ${ns}`);
    this.#objects = objects;
    this.#expiries = expiring;
    this.time.skip = (target) => this.skipScans(target);
    this.chaos = chaos;
    this.seed = chaos?.seed ?? 0;
    // § hz0ve24df@8h6yk
    this.coreSeed = derive(this.seed, "core");
    this.ulid = ulids((at) => BigInt(`0x${derive(this.seed, "ulid", at).toString("hex", 0, 10)}`));
    // § end hz0ve24df
    this.outageUntil = ms;
  }

  store(clock: WorkerClock | undefined): FaultStore {
    return new FaultStore(this, this.memory, clock);
  }

  /** Records a failed end as its head is written: its expiry may remove it before `check`. */
  written(key: string, data: Uint8Array): void {
    const [root, id, ...rest] = key.split("/");
    if (root !== "runs" || id === undefined || rest.length > 0) return;
    const found = fromBinary(layout.RunSchema, data);
    if (found.kind.case !== "head") return;
    const head = found.kind.value;
    const events = head.segments.at(-1)?.events ?? [];
    const failed = events.find((e) => e.kind.case === "failed")?.kind;
    if (failed?.case !== "failed") return;
    const error = failed.value.error;
    const failure = `${head.run} (${error?.type}: ${error?.message})`;
    this.failed.set(head.run, { chain: head.start?.chain ?? "", failure });
  }

  /** The value of `key` unless it expired, as a test sees what the store keeps. */
  object(key: string): Uint8Array | undefined {
    const at = this.#expiries.get(key);
    return at !== undefined && this.time.ms >= at ? undefined : this.#objects.get(key);
  }

  /** The head of the run `id`, its chain's through the pointer of a later run. */
  head(id: string): layout.Head {
    const found = this.#run(`runs/${id}`);
    if (found.kind.case === "head") return found.kind.value;
    if (found.kind.case !== "pointer") throw new Error(`runs/${id}: neither head nor pointer`);
    const chain = this.#run(`runs/${found.kind.value.chain}`);
    if (chain.kind.case !== "head") throw new Error(`runs/${found.kind.value.chain}: no head`);
    return chain.kind.value;
  }

  #run(key: string): layout.Run {
    const bytes = this.object(key);
    if (bytes === undefined) throw new Error(`no ${key}`);
    return fromBinary(layout.RunSchema, bytes);
  }

  /** The current runs of the chains not ended, with their start. */
  pending(): [string, layout.Start][] {
    const found: [string, layout.Start][] = [];
    for (const key of this.keys("runs/")) {
      const run = this.#run(key);
      if (run.kind.case !== "head") continue;
      const head = run.kind.value;
      const start = head.start;
      if (start !== undefined && key === `runs/${start.chain}` && !endedHead(head)) {
        found.push([head.run, start]);
      }
    }
    return found;
  }

  drop(): void {
    dropNamespace(this.ns);
  }

  workerId(place: number, replaced: number): string {
    // § hz0ve24df@kqg30
    return derive(this.seed, "worker", place, replaced).toString("hex", 0, 8);
  }

  /** Whether Chaos faults this try of `kind` on `key` by the worker of `clock`. */
  draw(kind: string, key: string, clock: WorkerClock): boolean {
    if (this.chaos === undefined) return false;
    const n = (clock.tries.get(`${kind} ${key}`) ?? 0) + 1;
    clock.tries.set(`${kind} ${key}`, n);
    // § hz0ve24df.a@azpyq
    return unit(this.seed, kind, key, clock.id, n) < this.chaos.rate;
  }

  /** How many of the next `scans` scans of `clock` Chaos lets through, one after the other: those
   * whose every listing succeeds at its first try. */
  #succeeding(clock: WorkerClock, scans: number): number {
    const chaos = this.chaos;
    if (chaos === undefined) return scans;
    for (let i = 0; i < scans; i++) {
      for (const key of clock.scanKeys) {
        const n = (clock.tries.get(`list ${key}`) ?? 0) + i + 1;
        if (unit(this.seed, "list", key, clock.id, n) < chaos.rate) return i;
      }
    }
    return scans;
  }

  holdOf(step: string): Hold | undefined {
    for (const hold of this.holds) {
      if (hold.step === step && hold.left > 0) {
        hold.left -= 1;
        return hold;
      }
    }
    return undefined;
  }

  /** Every key that did not expire and its value. */
  snapshot(): Map<string, Uint8Array> {
    return new Map(this.keys("").map((k) => [k, this.object(k) as Uint8Array]));
  }

  /** The keys under `prefix` that did not expire, in order, as a test sees what the store
   * keeps. */
  keys(prefix: string): string[] {
    return [...this.#objects.keys()]
      .filter((k) => k.startsWith(prefix) && this.object(k) !== undefined)
      .sort();
  }

  /** The earliest due entries of each workflow, as of the last write. */
  earliestDue(): Map<string, Due> {
    if (this.#due.writes !== this.writes) {
      const due = new Map<string, Due>();
      for (const key of this.#objects.keys()) {
        if (!key.startsWith("queues/")) continue;
        const workflow = key.split("/")[1] ?? "";
        const entry = key.slice(key.lastIndexOf("/") + 1);
        const at = dueMs(entry);
        const was = due.get(workflow);
        due.set(workflow, {
          any: Math.min(was?.any ?? at, at),
          wake: earlier(was?.wake, at),
        });
      }
      this.#due = { writes: this.writes, due };
    }
    return this.#due.due;
  }

  /** Moves past the scans that cannot find anything before `target` or the next deadline of
   * anything else, keeping the instants of the scans left.
   *
   * A worker's scan is skipped when the worker is idle, its deadline is the one its last scan set
   * (`maxIdle` after it, or the worker's phase after its first), nothing was written since that
   * scan, and no due entry it lists is due by then: the listing would return the keys it
   * returned, none of them due, and set the deadline that skipping sets.
   *
   * Under Chaos, a scan is skipped only when the draw of each of its listings succeeds, and the
   * skip counts their tries as the scan would: a scan whose draw fails runs, and may back off,
   * which moves the scans after it. Never in an outage, where every scan fails. */
  skipScans(target: number): void {
    if (!this.skipping || this.time.ms < this.outageUntil) return;
    const due = this.earliestDue();
    const idle: [WorkerClock, Waiter, number, Due | undefined][] = [];
    let bound = target;
    for (const [task, w] of this.time.parked) {
      if (w.at === undefined || w.owner.paused) continue;
      const clock = w.owner;
      if (clock instanceof WorkerClock && task === clock.agent) {
        const mi = clock.pod.settings.maxIdle.ms;
        const served = clock.pod.workflows.served().flatMap((s) => due.get(s.name) ?? []);
        const first: Due | undefined =
          served.length === 0
            ? undefined
            : {
                any: Math.min(...served.map((d) => d.any)),
                wake: served.map((d) => d.wake).reduce(earlier, undefined),
              };
        const worker = clock.pod.worker;
        if (
          clock.scannedAt !== undefined &&
          (w.at === clock.scannedAt + mi ||
            w.at === clock.scannedAt + idlePhase(this.coreSeed, mi)) &&
          clock.seen === this.writes &&
          worker !== undefined &&
          !pending(worker) &&
          [...clock.tasks].every((t) => t === task || t.future.done) &&
          (first === undefined || first.any > w.at)
        ) {
          idle.push([clock, w, mi, first]);
          if (first?.wake !== undefined) bound = Math.min(bound, first.wake);
          continue;
        }
      }
      bound = Math.min(bound, w.at);
    }
    for (const [clock, w, mi, first] of idle) {
      const limit = first === undefined ? bound : Math.min(bound, first.any - 1);
      const at = w.at as number;
      if (limit < at) continue;
      const scans = this.#succeeding(clock, Math.floor((limit - at) / mi) + 1);
      if (scans === 0) continue;
      for (const key of clock.scanKeys) {
        clock.tries.set(`list ${key}`, (clock.tries.get(`list ${key}`) ?? 0) + scans);
      }
      this.skipped += scans;
      const last = at + (scans - 1) * mi;
      clock.scannedAt = last;
      const after = last + mi;
      w.at = first?.wake === undefined ? after : Math.min(after, first.wake);
    }
  }
}
