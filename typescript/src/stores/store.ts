// The store contract (I-26): six operations, the only ones Substantial performs on a store, the
// expiry in whole days being part of every write.
import { createHash } from "node:crypto";
import { ConfigError } from "../errors.ts";

/** Created, with the ETag of the value written, which a later `cas` names, or "exists". */
export type Create = { readonly etag: string } | "exists";

/** A key's value and its ETag, which a later `cas` names. */
export interface StoreObject {
  readonly data: Uint8Array;
  readonly etag: string;
}

/** Written, with the new ETag, or "mismatch": the key's value is not the one named, or there is
 * none. */
export type Cas = { readonly etag: string } | "mismatch";

/** The longest expiry a write may carry, 100 years of days. */
export const EXPIRY_MAX_DAYS = 36_500;
export const DAY_MS = 86_400_000;

export interface Capabilities {
  /** Whether `list` can wait for a change. */
  readonly wait: boolean;
}

export interface ListWait {
  readonly seconds: number;
  readonly etag?: string;
}

export interface ListOptions {
  readonly startAfter?: string;
  readonly maxKeys: number;
  readonly wait?: ListWait;
  /** Aborts the listing once its answer is no longer wanted, as a `result` wait given up. */
  readonly signal?: AbortSignal;
}

export interface Listing {
  readonly keys: string[];
  readonly etag?: string;
}

/** An adapter scoped to its namespace: keys are relative to it. */
export interface Store {
  // § karttrcdb@05gbd
  readonly capabilities: Capabilities;
  /** Connects, throwing `ConfigError` on a store the adapter cannot use; the operations connect
   * too when it was not called. */
  open(): Promise<void>;
  /** Atomic and exclusive: of concurrent creates of a key, exactly one is created. */
  create(key: string, data: Uint8Array, expiry: number | undefined): Promise<Create>;
  /** Replaces the value whose ETag is `etag`, atomically with every other operation on `key`. */
  // § one kkad6aqxp@2yzxz
  cas(key: string, data: Uint8Array, etag: string, expiry: number | undefined): Promise<Cas>;
  get(key: string): Promise<StoreObject | undefined>;
  /** Keys under `prefix` and after `startAfter`, in byte order of the full key. */
  list(prefix: string, options: ListOptions): Promise<Listing>;
  /** Removes a key; deleting a missing key succeeds. */
  delete(key: string): Promise<void>;
  /** Throws `ConfigError` unless the store expires a key written with each of `days`. */
  checkExpiry(days: readonly number[]): Promise<void>;
  /** Releases what the adapter holds; it is not used after. */
  close(): Promise<void>;
}

/** The store failed after the adapter's own retries. */
export class StoreFailure extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StoreFailure";
  }
}

/** Compares keys by code point, which is the byte order of their UTF-8, unlike `<` on UTF-16. */
export function compareKeys(a: string, b: string): number {
  let i = 0;
  let j = 0;
  while (i < a.length && j < b.length) {
    const x = a.codePointAt(i) as number;
    const y = b.codePointAt(j) as number;
    if (x !== y) return x - y;
    i += x > 0xffff ? 2 : 1;
    j += y > 0xffff ? 2 : 1;
  }
  return a.length - i - (b.length - j);
}

/** The keys of a sorted run that lie under `prefix` and after `startAfter`, up to `maxKeys`. */
export function inRange(
  keys: Iterable<string>,
  prefix: string,
  startAfter: string | undefined,
  maxKeys: number,
): string[] {
  // § dfd6vt90h.b@ae726
  const out: string[] = [];
  for (const key of keys) {
    if (out.length >= maxKeys) break;
    if (key.startsWith(prefix) && (startAfter === undefined || compareKeys(key, startAfter) > 0)) {
      out.push(key);
    }
  }
  return out;
}

export function refuseWait(wait: ListWait | undefined): void {
  if (wait !== undefined) {
    throw new StoreFailure("this store cannot wait on a listing");
  }
}

/** Refuses an expiry outside 1 to `EXPIRY_MAX_DAYS` days, which no store could keep. */
export function checkDays(expiry: number | undefined): void {
  // § r4rsq6spn.c@zsph4
  if (
    expiry !== undefined &&
    !(Number.isInteger(expiry) && expiry >= 1 && expiry <= EXPIRY_MAX_DAYS)
  ) {
    throw new StoreFailure(`an expiry of ${expiry} days is outside 1 to ${EXPIRY_MAX_DAYS}`);
  }
}

/** `checkDays` for every expiry of `days`, as `ConfigError`. */
export function checkAllDays(days: readonly number[]): void {
  for (const d of days) {
    try {
      checkDays(d);
    } catch (e) {
      if (e instanceof StoreFailure) throw new ConfigError(e.message);
      throw e;
    }
  }
}

/** The ETag of the stores that make their own: the hex of the first 16 bytes of the SHA-256 of
 * the value. */
export function digestEtag(data: Uint8Array): string {
  return createHash("sha256").update(data).digest().subarray(0, 16).toString("hex");
}
