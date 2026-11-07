// The store of an environment: its namespace in memory, seen by each worker and client through
// the faults the test asks for, an outage or Chaos.
import { createHash } from "node:crypto";
import { InvalidArgument, onlyOptions } from "../errors.ts";
import { STORE_RETRY } from "../settings.ts";
import type { MemoryStore } from "../stores/memory.ts";
import { Ambiguous, create, Transient, withRetry } from "../stores/retry.ts";
import {
  type Capabilities,
  type Create,
  type Listing,
  type ListOptions,
  refuseWait,
  type Store,
} from "../stores/store.ts";
import { never } from "./time.ts";
import type { WorkerClock } from "./workers.ts";
import type { World } from "./world.ts";

/** Faults drawn from `seed`: each worker store operation fails with probability `rate`, half of
 * the failed creates applied anyway, and each step attempt that returns crashes its worker with
 * probability `rate`. */
export class Chaos {
  readonly seed: number;
  readonly rate: number;

  constructor(options: { readonly seed: number; readonly rate: number }) {
    onlyOptions(options, ["seed", "rate"], "Chaos");
    const { seed, rate } = options;
    if (!Number.isSafeInteger(seed))
      throw new InvalidArgument(`a Chaos seed is an integer, not ${seed}`);
    if (typeof rate !== "number" || !(rate >= 0 && rate <= 1)) {
      throw new InvalidArgument(`a Chaos rate is a number from 0 to 1, not ${rate}`);
    }
    this.seed = seed;
    this.rate = rate;
  }
}

/** What Chaos injected: store operations `failed`, creates applied with their answer lost
 * (`ambiguous`), and workers `crashed` after a step returned. */
export interface Faults {
  readonly failed: number;
  readonly ambiguous: number;
  readonly crashed: number;
}

/** 32 bytes from `seed` and what they name, never from a place in a sequence. */
export function derive(seed: number, ...parts: (string | number)[]): Buffer {
  return createHash("sha256")
    .update(JSON.stringify([seed, ...parts]))
    .digest();
}

/** A draw in [0, 1) from `seed` and what it names. */
export function unit(seed: number, ...parts: (string | number)[]): number {
  return Number(derive(seed, ...parts).readBigUInt64BE(0) >> 11n) / 2 ** 53;
}

/** The namespace as a worker (`clock`) or a client (`undefined`) sees it. */
export class FaultStore implements Store {
  readonly capabilities: Capabilities = { wait: false };
  readonly #world: World;
  readonly #memory: MemoryStore;
  readonly #clock: WorkerClock | undefined;

  constructor(world: World, memory: MemoryStore, clock: WorkerClock | undefined) {
    this.#world = world;
    this.#memory = memory;
    this.#clock = clock;
  }

  /** A worker's backoff waits in virtual time; a client's takes none. */
  #sleep = async (ms: number): Promise<void> => {
    const clock = this.#clock;
    if (clock !== undefined) await this.#world.time.wait(clock, [], clock.now() + Math.ceil(ms));
  };

  async #try<T>(kind: string, key: string, apply: () => Promise<T>): Promise<T> {
    const world = this.#world;
    const clock = this.#clock;
    if (clock !== undefined) {
      // A paused worker does not reach the store.
      await world.time.passing(clock);
      if (clock.dead) return never();
    }
    if (world.time.ms < world.outageUntil) throw new Transient("the store is out");
    if (clock !== undefined && world.draw(kind, key, clock)) {
      const n = clock.tries.get(`${kind} ${key}`) ?? 0;
      const applied = kind === "create" && unit(world.seed, "applied", key, clock.id, n) < 0.5;
      // A create that found its key applied nothing: it counts as failed.
      if (applied && (await apply()) === "created") {
        world.counts.ambiguous += 1;
        throw new Ambiguous("chaos: a create applied, its answer lost");
      }
      world.counts.failed += 1;
      throw new Transient(`chaos: a ${kind} failed`);
    }
    return apply();
  }

  #retry<T>(kind: string, key: string, apply: () => Promise<T>): Promise<T> {
    return withRetry(STORE_RETRY, () => this.#try(kind, key, apply), this.#sleep);
  }

  async #create(key: string, data: Uint8Array): Promise<Create> {
    const created = await this.#memory.create(key, data);
    if (created === "created") {
      this.#world.writes += 1;
      this.#world.written(key, data);
    }
    return created;
  }

  create(key: string, data: Uint8Array): Promise<Create> {
    return create(
      STORE_RETRY,
      data,
      () => this.#try("create", key, () => this.#create(key, data)),
      () => this.#try("get", key, () => this.#memory.get(key)),
      this.#sleep,
    );
  }

  get(key: string): Promise<Uint8Array | undefined> {
    return this.#retry("get", key, () => this.#memory.get(key));
  }

  async list(prefix: string, options: ListOptions): Promise<Listing> {
    refuseWait(options.wait);
    const writes = this.#world.writes;
    const { startAfter } = options;
    const key = startAfter === undefined ? prefix : `${prefix}>${startAfter}`;
    const listing = await this.#retry("list", key, () => this.#memory.list(prefix, options));
    if (this.#clock !== undefined && prefix.startsWith("queues/")) this.#clock.listed(key, writes);
    return listing;
  }

  delete(key: string): Promise<void> {
    return this.#retry("delete", key, async () => {
      await this.#memory.delete(key);
      this.#world.writes += 1;
    });
  }

  async open(): Promise<void> {}

  async close(): Promise<void> {}
}
