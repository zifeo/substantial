import { mkdirSync } from "node:fs";
import { link, mkdir, open, readdir, readFile, stat, unlink } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { ConfigError, StoreError } from "../errors.ts";
import type { StoreRetry } from "../settings.ts";
import * as retry from "./retry.ts";
import { Ambiguous, Refused, Transient } from "./retry.ts";
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

const TMP = ".tmp";
const encoder = new TextEncoder();

function componentError(c: string): string | undefined {
  if (c === "") return "empty";
  if (c === "." || c === "..") return "relative";
  if (encoder.encode(c).length > 255) return "longer than 255 bytes";
  if (/[A-Z]/.test(c)) return "uppercase, which a file system may fold";
  if (c.includes("\0")) return "holds a NUL byte";
  return undefined;
}

export function checkKey(key: string): void {
  key.split("/").forEach((c, i) => {
    const why =
      componentError(c) ?? (i === 0 && c === TMP ? "reserved for temporaries" : undefined);
    if (why !== undefined) {
      throw new Refused(`invalid key component ${JSON.stringify(c)}: ${why}`);
    }
  });
}

function code(e: unknown): string | undefined {
  return (e as NodeJS.ErrnoException)?.code;
}

function describe(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

async function syncDir(path: string): Promise<void> {
  const handle = await open(path, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function isDir(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isDirectory();
  } catch {
    return false;
  }
}

/** Creates the missing directories between `root` and `directory`, each fsynced in its parent. */
async function ensureDirs(root: string, directory: string): Promise<void> {
  const missing: string[] = [];
  for (let at = directory; at !== root && !(await isDir(at)); at = dirname(at)) {
    missing.push(at);
  }
  for (const d of missing.reverse()) {
    try {
      await mkdir(d);
    } catch (e) {
      if (code(e) === "ENOTDIR") throw new Refused(`${d} lies under another key: ${describe(e)}`);
      if (code(e) !== "EEXIST") throw new Transient(`create directory ${d}: ${describe(e)}`);
    }
    try {
      await syncDir(dirname(d));
    } catch (e) {
      throw new Transient(`sync directory ${dirname(d)}: ${describe(e)}`);
    }
  }
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

async function createKey(root: string, key: string, data: Uint8Array): Promise<Create> {
  checkKey(key);
  const path = join(root, ...key.split("/"));
  const tmp = join(root, TMP, hex(crypto.getRandomValues(new Uint8Array(16))));
  try {
    try {
      const handle = await open(tmp, "wx");
      try {
        await handle.writeFile(data);
        await handle.sync();
      } finally {
        await handle.close();
      }
    } catch (e) {
      throw new Transient(`write ${tmp}: ${describe(e)}`);
    }
    await ensureDirs(root, dirname(path));
    try {
      await link(tmp, path);
    } catch (e) {
      if (code(e) === "EEXIST") return "exists";
      if (code(e) === "ENOTDIR") throw new Refused(`key ${key} lies under another key`);
      // On NFS a link that succeeded may still report an error.
      throw new Ambiguous(`link ${path}: ${describe(e)}`);
    }
    try {
      await syncDir(dirname(path));
    } catch (e) {
      throw new Ambiguous(`sync directory ${dirname(path)}: ${describe(e)}`);
    }
    return "created";
  } finally {
    await unlink(tmp).catch(() => {});
  }
}

const ABSENT = ["ENOENT", "EISDIR", "ENOTDIR"];

async function getKey(root: string, key: string): Promise<Uint8Array | undefined> {
  checkKey(key);
  const path = join(root, ...key.split("/"));
  try {
    return new Uint8Array(await readFile(path));
  } catch (e) {
    if (ABSENT.includes(code(e) ?? "")) return undefined;
    throw new Transient(`read ${path}: ${describe(e)}`);
  }
}

async function deleteKey(root: string, key: string): Promise<void> {
  checkKey(key);
  const path = join(root, ...key.split("/"));
  try {
    await unlink(path);
  } catch (e) {
    if (!ABSENT.includes(code(e) ?? "")) throw new Transient(`delete ${path}: ${describe(e)}`);
  }
}

async function walk(
  directory: string,
  base: string,
  prefix: string,
  keys: string[],
): Promise<void> {
  let entries: import("node:fs").Dirent[];
  try {
    entries = await readdir(directory, { withFileTypes: true });
  } catch (e) {
    if (code(e) === "ENOENT" || code(e) === "ENOTDIR") return;
    throw new Transient(`list ${directory}: ${describe(e)}`);
  }
  for (const entry of entries) {
    const name = entry.name;
    if (componentError(name) !== undefined || (base === "" && name === TMP)) continue;
    const key = `${base}${name}`;
    if (entry.isDirectory()) {
      const sub = `${key}/`;
      if (sub.startsWith(prefix) || prefix.startsWith(sub)) {
        await walk(join(directory, name), sub, prefix, keys);
      }
    } else if (entry.isFile() && key.startsWith(prefix)) {
      keys.push(key);
    }
  }
}

/** Walks the deepest directory holding `prefix`, keeps the keys in range, sorts by full key. */
async function listKeys(root: string, prefix: string, options: ListOptions): Promise<Listing> {
  const slash = prefix.lastIndexOf("/");
  const dirKey = slash >= 0 ? prefix.slice(0, slash) : "";
  if (dirKey !== "") checkKey(dirKey);
  const keys: string[] = [];
  const directory = dirKey === "" ? root : join(root, ...dirKey.split("/"));
  await walk(directory, dirKey === "" ? "" : `${dirKey}/`, prefix, keys);
  keys.sort(compareKeys);
  return { keys: inRange(keys, prefix, options.startAfter, options.maxKeys) };
}

/** `file:<path>`: the directory is the namespace, one file per key, created with `link()`. For
 * development, CI and one machine. */
export class FsStore implements Store {
  readonly capabilities: Capabilities = { wait: false };
  readonly #root: string;
  readonly #retry: StoreRetry;

  constructor(path: string, storeRetry: StoreRetry) {
    if (typeof path !== "string") throw new ConfigError(`a file store needs a path, not ${path}`);
    this.#root = resolve(path);
    this.#retry = storeRetry;
    try {
      mkdirSync(join(path, TMP), { recursive: true });
    } catch (e) {
      throw new StoreError(`cannot prepare ${path}: ${describe(e)}`);
    }
  }

  create(key: string, data: Uint8Array): Promise<Create> {
    const bytes = data.slice();
    return retry.create(
      this.#retry,
      bytes,
      () => createKey(this.#root, key, bytes),
      () => getKey(this.#root, key),
    );
  }

  get(key: string): Promise<Uint8Array | undefined> {
    return retry.withRetry(this.#retry, () => getKey(this.#root, key));
  }

  async list(prefix: string, options: ListOptions): Promise<Listing> {
    refuseWait(options.wait);
    return retry.withRetry(this.#retry, () => listKeys(this.#root, prefix, options));
  }

  delete(key: string): Promise<void> {
    return retry.withRetry(this.#retry, () => deleteKey(this.#root, key));
  }

  async close(): Promise<void> {}
}
