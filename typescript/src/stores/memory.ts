import { ConfigError } from "../errors.ts";
import {
  type Capabilities,
  type Cas,
  type Create,
  checkAllDays,
  checkDays,
  compareKeys,
  DAY_MS,
  digestEtag,
  inRange,
  type Listing,
  type ListOptions,
  refuseWait,
  type Store,
  type StoreObject,
} from "./store.ts";

const namespaces = new Map<string, Map<string, Uint8Array>>();
/** The instant each key of a namespace expires at, in ms since the epoch, for those that do. */
const expiries = new Map<string, Map<string, number>>();

/** Milliseconds since the epoch: the wall clock, or a test's. */
export type Clock = () => number;

/** The objects of the namespace `ns`, read at once: for the test environment. */
export function objectsOf(ns: string): ReadonlyMap<string, Uint8Array> | undefined {
  return namespaces.get(ns);
}

/** The instant each expiring key of the namespace `ns` expires at: for the test environment. */
export function expiriesOf(ns: string): ReadonlyMap<string, number> | undefined {
  return expiries.get(ns);
}

/** Forgets the namespace `ns`. */
export function dropNamespace(ns: string): void {
  namespaces.delete(ns);
  expiries.delete(ns);
}

/** `memory://<ns>`: a namespace held by the process, for tests. Every store opened on the same
 * `<ns>` in a process shares its objects, its expiries judged by `clock`. */
export class MemoryStore implements Store {
  readonly capabilities: Capabilities = { wait: false };
  readonly #objects: Map<string, Uint8Array>;
  readonly #expiries: Map<string, number>;
  readonly #clock: Clock;

  constructor(ns: string, clock: Clock = Date.now) {
    if (typeof ns !== "string" || ns === "") {
      throw new ConfigError(`a memory store needs a namespace, not ${ns}`);
    }
    this.#clock = clock;
    // § e9ern38bz.d@p14ng
    let objects = namespaces.get(ns);
    if (objects === undefined) {
      objects = new Map();
      namespaces.set(ns, objects);
    }
    this.#objects = objects;
    let expiring = expiries.get(ns);
    if (expiring === undefined) {
      expiring = new Map();
      expiries.set(ns, expiring);
    }
    this.#expiries = expiring;
  }

  /** The value of `key` unless it expired, which removes it: exact, as a store that expires keys
   * itself. */
  #live(key: string): Uint8Array | undefined {
    // § r4rsq6spn.b@zsygv
    const at = this.#expiries.get(key);
    if (at !== undefined && this.#clock() >= at) {
      this.#objects.delete(key);
      this.#expiries.delete(key);
    }
    return this.#objects.get(key);
  }

  #write(key: string, data: Uint8Array, expiry: number | undefined): void {
    // § r4rsq6spn.a@9ng1a
    this.#objects.set(key, data.slice());
    if (expiry === undefined) this.#expiries.delete(key);
    else this.#expiries.set(key, this.#clock() + expiry * DAY_MS);
  }

  async create(key: string, data: Uint8Array, expiry: number | undefined): Promise<Create> {
    checkDays(expiry);
    // § dfd6vt90h@v0yzg
    if (this.#live(key) !== undefined) return "exists";
    this.#write(key, data, expiry);
    return { etag: digestEtag(data) };
  }

  async cas(key: string, data: Uint8Array, etag: string, expiry: number | undefined): Promise<Cas> {
    checkDays(expiry);
    // § kkad6aqxp@vmybc
    const current = this.#live(key);
    // § one kkad6aqxp.a@b8g5z
    if (current === undefined || digestEtag(current) !== etag) return "mismatch";
    this.#write(key, data, expiry);
    return { etag: digestEtag(data) };
  }

  async get(key: string): Promise<StoreObject | undefined> {
    const data = this.#live(key);
    return data === undefined ? undefined : { data: data.slice(), etag: digestEtag(data) };
  }

  async list(prefix: string, options: ListOptions): Promise<Listing> {
    refuseWait(options.wait);
    const keys = [...this.#objects.keys()].filter((k) => k.startsWith(prefix)).sort(compareKeys);
    return { keys: inRange(keys, prefix, options.startAfter, options.maxKeys) };
  }

  async delete(key: string): Promise<void> {
    this.#objects.delete(key);
    this.#expiries.delete(key);
  }

  async checkExpiry(days: readonly number[]): Promise<void> {
    checkAllDays(days);
  }

  async open(): Promise<void> {}

  async close(): Promise<void> {}
}
