import { ConfigError } from "../errors.ts";
import {
  type Capabilities,
  type Create,
  compareKeys,
  inRange,
  type Listing,
  type ListOptions,
  refuseWait,
  type Store,
} from "./store.ts";

const namespaces = new Map<string, Map<string, Uint8Array>>();

/** The objects of the namespace `ns`, read at once: for the test environment. */
export function objectsOf(ns: string): ReadonlyMap<string, Uint8Array> | undefined {
  return namespaces.get(ns);
}

/** Forgets the namespace `ns`. */
export function dropNamespace(ns: string): void {
  namespaces.delete(ns);
}

/** `memory://<ns>`: a namespace held by the process, for tests. Every store opened on the same
 * `<ns>` in a process shares its objects. */
export class MemoryStore implements Store {
  readonly capabilities: Capabilities = { wait: false };
  readonly #objects: Map<string, Uint8Array>;

  constructor(ns: string) {
    if (typeof ns !== "string" || ns === "") {
      throw new ConfigError(`a memory store needs a namespace, not ${ns}`);
    }
    let objects = namespaces.get(ns);
    if (objects === undefined) {
      objects = new Map();
      namespaces.set(ns, objects);
    }
    this.#objects = objects;
  }

  async create(key: string, data: Uint8Array): Promise<Create> {
    if (this.#objects.has(key)) return "exists";
    this.#objects.set(key, data.slice());
    return "created";
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    return this.#objects.get(key)?.slice();
  }

  async list(prefix: string, options: ListOptions): Promise<Listing> {
    refuseWait(options.wait);
    const keys = [...this.#objects.keys()].filter((k) => k.startsWith(prefix)).sort(compareKeys);
    return { keys: inRange(keys, prefix, options.startAfter, options.maxKeys) };
  }

  async delete(key: string): Promise<void> {
    this.#objects.delete(key);
  }

  async open(): Promise<void> {}

  async close(): Promise<void> {}
}
