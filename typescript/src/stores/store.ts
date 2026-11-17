// The store contract (I-26): four operations, the only ones Substantial performs on a store.

export type Create = "created" | "exists";

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
  readonly capabilities: Capabilities;
  /** Connects, throwing `ConfigError` on a store the adapter cannot use; the operations connect
   * too when it was not called. */
  open(): Promise<void>;
  /** Atomic and exclusive: of concurrent creates of a key, exactly one gets "created". */
  create(key: string, data: Uint8Array): Promise<Create>;
  get(key: string): Promise<Uint8Array | undefined>;
  /** Keys under `prefix` and after `startAfter`, in byte order of the full key. */
  list(prefix: string, options: ListOptions): Promise<Listing>;
  /** Removes a key; deleting a missing key succeeds. */
  delete(key: string): Promise<void>;
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
