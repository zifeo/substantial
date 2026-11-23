import { type Dirent, mkdirSync } from "node:fs";
import {
  type FileHandle,
  link,
  lstat,
  mkdir,
  open,
  readdir,
  readFile,
  rmdir,
  stat,
  unlink,
} from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { ConfigError, StoreError } from "../errors.ts";
import type { StoreRetry } from "../settings.ts";
import type { Clock } from "./memory.ts";
import * as retry from "./retry.ts";
import { Ambiguous, Refused, Transient } from "./retry.ts";
import {
  type Capabilities,
  type Cas,
  type Create,
  checkAllDays,
  checkDays,
  compareKeys,
  DAY_MS,
  inRange,
  type Listing,
  type ListOptions,
  refuseWait,
  type Store,
  type StoreObject,
} from "./store.ts";

const TMP = ".tmp";
/** The bytes before a version's value: the instant it expires at, ms since the epoch big-endian,
 * 0 for none, `TOMBSTONE` for a tombstone, then the key's incarnation, drawn at each create, which
 * an ETag names so that no ETag of a removed key matches the key created again. */
const HEADER = 24;
/** The header of a tombstone, the version a delete or the sweep writes. */
const TOMBSTONE = -1;
/** A version is removed only once its successor is this old, and a tombstone, latest of its key,
 * once it is this old itself; an operation that decides on a read stops and reads again once it
 * is this old: an older read could predate the removal of a version it relies on. */
const SUPERSEDED_KEPT_MS = 600_000;
const encoder = new TextEncoder();

function componentError(c: string): string | undefined {
  if (c === "") return "empty";
  if (c === "." || c === "..") return "relative";
  if (encoder.encode(c).length > 255) return "longer than 255 bytes";
  if (/[A-Z]/.test(c)) return "uppercase, which a file system may fold";
  if (c.includes("\0")) return "holds a NUL byte";
  if (/^\.[0-9]+$/.test(c)) return "a dot and digits, the name of a version";
  return undefined;
}

export function checkKey(key: string): void {
  key.split("/").forEach((c, i) => {
    const why =
      componentError(c) ?? (i === 0 && c === TMP ? "reserved for temporary files" : undefined);
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

/** Creates the missing directories between `root` and `directory`, each fsynced in its parent;
 * false when a sweep removed one of them meanwhile, for the caller to try again. */
async function ensureDirs(root: string, directory: string): Promise<boolean> {
  // § c7pz5rk26.b@76drq
  const missing: string[] = [];
  for (let at = directory; at !== root && !(await isDir(at)); at = dirname(at)) {
    missing.push(at);
  }
  for (const d of missing.reverse()) {
    try {
      await mkdir(d);
    } catch (e) {
      if (code(e) === "ENOTDIR") throw new Refused(`${d} lies under another key: ${describe(e)}`);
      if (code(e) !== "EEXIST") {
        // macOS answers EINVAL in a directory being removed.
        if (!(await isDir(dirname(d)))) return false;
        throw new Transient(`create directory ${d}: ${describe(e)}`);
      }
    }
    try {
      await syncDir(dirname(d));
    } catch (e) {
      if (code(e) === "ENOENT") return false;
      throw new Transient(`sync directory ${dirname(d)}: ${describe(e)}`);
    }
  }
  return true;
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

const ABSENT = ["ENOENT", "ENOTDIR"];

/** The name of version `n` in its key's directory, never a key component. */
function version(n: number): string {
  return `.${String(n).padStart(20, "0")}`;
}

function versionOf(name: string): number | undefined {
  return /^\.[0-9]{20}$/.test(name) ? Number(name.slice(1)) : undefined;
}

/** The ETag of version `n` of the incarnation `inc`. */
function etagOf(inc: Uint8Array, n: number): string {
  return `${hex(inc)}.${n}`;
}

function parseEtag(etag: string): { inc: string; n: number } | undefined {
  const match = /^([0-9a-f]{32})\.([0-9]+)$/.exec(etag);
  return match === null ? undefined : { inc: match[1] as string, n: Number(match[2]) };
}

/** The versions the directory of a key lists, lowest first. */
async function versions(directory: string): Promise<number[]> {
  let names: string[];
  try {
    names = await readdir(directory);
  } catch (e) {
    if (ABSENT.includes(code(e) ?? "")) return [];
    throw new Transient(`list ${directory}: ${describe(e)}`);
  }
  return names
    .map(versionOf)
    .filter((n): n is number => n !== undefined)
    .sort((a, b) => a - b);
}

/** A version as its file holds it: `at` the expiry instant, 0 for none, `TOMBSTONE` for a
 * tombstone, and the key's incarnation. */
interface Version {
  readonly data: Uint8Array;
  readonly at: number;
  readonly inc: Uint8Array;
}

/** A version, `undefined` when it is missing. */
async function readVersion(path: string): Promise<Version | undefined> {
  // § qf98sbvzj@ctyep
  let file: Uint8Array;
  try {
    file = new Uint8Array(await readFile(path));
  } catch (e) {
    if (ABSENT.includes(code(e) ?? "")) return undefined;
    throw new Transient(`read ${path}: ${describe(e)}`);
  }
  if (file.length < HEADER) {
    throw new Refused(`${path} is not a file of this store: shorter than its header`);
  }
  const at = Number(new DataView(file.buffer, file.byteOffset, 8).getBigInt64(0));
  return { data: file.subarray(HEADER), at, inc: file.subarray(8, HEADER) };
}

async function exists(path: string): Promise<boolean> {
  try {
    await lstat(path);
    return true;
  } catch (e) {
    if (ABSENT.includes(code(e) ?? "")) return false;
    throw new Transient(`stat ${path}: ${describe(e)}`);
  }
}

/** Whether a version of header `at` reads as missing at `now`: a tombstone, or past its expiry. */
function gone(at: number, now: number): boolean {
  return at === TOMBSTONE || (at > 0 && now >= at);
}

interface Latest {
  readonly n: number;
  readonly version: Version;
}

/** The latest version of a key, read so that no later one existed when it answers.
 *
 * A version goes only once its successor is `SUPERSEDED_KEPT_MS` old, the lowest first, so the
 * latest one and every one written during the listing stay: the listing shows the latest at its
 * start, and the next ones are found by name. A version gone when read was superseded, and is
 * listed again. */
async function latest(directory: string): Promise<Latest | undefined> {
  // § j4b9rkhg8.c@1dstr
  for (;;) {
    const started = performance.now();
    const listed = await versions(directory);
    let n = listed[listed.length - 1];
    if (n === undefined) return undefined;
    for (;;) {
      const found = await readVersion(join(directory, version(n)));
      if (found === undefined) break;
      if (await exists(join(directory, version(n + 1)))) {
        n += 1;
        continue;
      }
      if (performance.now() - started >= SUPERSEDED_KEPT_MS) break;
      return { n, version: found };
    }
  }
}

/** The value as a version file holds it, its header first so all of it lands together. */
async function writeTemporary(
  root: string,
  data: Uint8Array,
  at: number,
  inc: Uint8Array,
): Promise<string> {
  const tmp = join(root, TMP, hex(crypto.getRandomValues(new Uint8Array(16))));
  const file = new Uint8Array(HEADER + data.length);
  new DataView(file.buffer).setBigInt64(0, BigInt(at));
  file.set(inc, 8);
  file.set(data, HEADER);
  try {
    const handle = await open(tmp, "wx");
    try {
      await handle.writeFile(file);
      await handle.sync();
    } finally {
      await handle.close();
    }
  } catch (e) {
    await unlink(tmp).catch(() => {});
    throw new Transient(`write ${tmp}: ${describe(e)}`);
  }
  return tmp;
}

/** What linking a version found: linked, the version taken, or the key's directory gone. */
type Linked = "linked" | "taken" | "no directory";

/** Writes version `n` of the key directory by linking `tmp` to it, which fails if the version
 * exists. */
async function linkVersion(tmp: string, directory: string, n: number): Promise<Linked> {
  // § j4b9rkhg8@x8k9g
  const path = join(directory, version(n));
  try {
    await link(tmp, path);
  } catch (e) {
    if (code(e) === "EEXIST") return "taken";
    if (code(e) === "ENOTDIR") throw new Refused(`${directory} lies under another key`);
    // macOS answers EINVAL in a directory being removed.
    if (!(await isDir(directory))) return "no directory";
    // On NFS a link that succeeded may still report an error.
    throw new Ambiguous(`link ${path}: ${describe(e)}`);
  }
  try {
    // § one kkad6aqxp.c@6e2c5
    await syncDir(directory);
  } catch (e) {
    throw new Ambiguous(`sync directory ${directory}: ${describe(e)}`);
  }
  return "linked";
}

/** When the file at `path` was linked, by the store's clock: its ctime, which a link sets. */
async function linkedAt(path: string): Promise<number | undefined> {
  try {
    return (await lstat(path)).ctimeMs;
  } catch (e) {
    if (code(e) === "ENOENT") return undefined;
    throw new Transient(`stat ${path}: ${describe(e)}`);
  }
}

/** Removes the versions below the latest whose successor is `SUPERSEDED_KEPT_MS` old at `now`,
 * the store's clock, the lowest first, so a version is gone only once every version below it is. */
async function clean(directory: string, now: number): Promise<void> {
  // § j4b9rkhg8.b@prs85
  const listed = await versions(directory);
  for (let i = 0; i + 1 < listed.length; i += 1) {
    const [low, high] = [listed[i] as number, listed[i + 1] as number];
    const linked = await linkedAt(join(directory, version(high)));
    if (high !== low + 1 || linked === undefined || linked > now - SUPERSEDED_KEPT_MS) return;
    try {
      await unlink(join(directory, version(low)));
    } catch (e) {
      if (code(e) !== "ENOENT") throw new Transient(`delete ${directory}: ${describe(e)}`);
    }
  }
}

/** Links `tmp` as the version after the latest of `directory`, given the latest when it reads as
 * present, or as version 0 of a directory made again when a sweep removed it; `undefined` when
 * `decide` refuses. */
async function writeAfterLatest(
  root: string,
  directory: string,
  tmp: string,
  now: number,
  decide: (live: Version | undefined) => boolean,
): Promise<number | undefined> {
  for (;;) {
    if (!(await ensureDirs(root, directory))) continue;
    const started = performance.now();
    const found = await latest(directory);
    const live = found === undefined || gone(found.version.at, now) ? undefined : found.version;
    if (!decide(live)) return undefined;
    const target = found === undefined ? 0 : found.n + 1;
    if (performance.now() - started >= SUPERSEDED_KEPT_MS) continue;
    if ((await linkVersion(tmp, directory, target)) === "linked") return target;
  }
}

async function createKey(
  root: string,
  key: string,
  data: Uint8Array,
  at: number,
  now: number,
): Promise<Create> {
  // § c7pz5rk26@xwfrc
  checkKey(key);
  const directory = join(root, ...key.split("/"));
  // § one j4b9rkhg8.d@3p6hf
  const inc = crypto.getRandomValues(new Uint8Array(16));
  const tmp = await writeTemporary(root, data, at, inc);
  let created: number | undefined;
  try {
    created = await writeAfterLatest(root, directory, tmp, now, (live) => live === undefined);
  } finally {
    await unlink(tmp).catch(() => {});
  }
  if (created === undefined) return "exists";
  await clean(directory, now);
  return { etag: etagOf(inc, created) };
}

async function casKey(
  root: string,
  key: string,
  data: Uint8Array,
  etag: string,
  at: number,
  now: number,
): Promise<Cas> {
  // § j4b9rkhg8@9ekyz
  checkKey(key);
  const directory = join(root, ...key.split("/"));
  const parsed = parseEtag(etag);
  if (parsed === undefined) return "mismatch";
  const { inc, n } = parsed;
  const incBytes = Uint8Array.from(Buffer.from(inc, "hex"));
  const tmp = await writeTemporary(root, data, at, incBytes);
  try {
    const started = performance.now();
    // The successor absent, then the version present: no later one was written by then.
    if (await exists(join(directory, version(n + 1)))) return "mismatch";
    const found = await readVersion(join(directory, version(n)));
    // § one kkad6aqxp.a@5fs4e
    if (found === undefined || hex(found.inc) !== inc || gone(found.at, now)) return "mismatch";
    if (performance.now() - started >= SUPERSEDED_KEPT_MS) {
      throw new Transient(`cas ${key}: read too long ago`);
    }
    if ((await linkVersion(tmp, directory, n + 1)) !== "linked") return "mismatch";
  } finally {
    await unlink(tmp).catch(() => {});
  }
  await clean(directory, now);
  return { etag: etagOf(incBytes, n + 1) };
}

async function getKey(root: string, key: string, now: number): Promise<StoreObject | undefined> {
  // § dfd6vt90h.a@srwhx
  checkKey(key);
  const found = await latest(join(root, ...key.split("/")));
  if (found === undefined || gone(found.version.at, now)) return undefined;
  return { data: found.version.data, etag: etagOf(found.version.inc, found.n) };
}

/** Writes a tombstone after the latest version of `directory` when that one reads as present at
 * `now`, or, with `onlyExpired`, as past its expiry: a write naming an earlier version then finds
 * its successor taken. */
async function bury(
  root: string,
  directory: string,
  now: number,
  onlyExpired: boolean,
): Promise<boolean> {
  // § j4b9rkhg8.a@9fc2f
  let tmp: string | undefined;
  try {
    for (;;) {
      const started = performance.now();
      const found = await latest(directory);
      if (found === undefined) return false;
      const at = found.version.at;
      const wanted = onlyExpired ? at > 0 && now >= at : !gone(at, now);
      if (!wanted) return false;
      tmp ??= await writeTemporary(root, new Uint8Array(), TOMBSTONE, found.version.inc);
      if (performance.now() - started >= SUPERSEDED_KEPT_MS) continue;
      const linked = await linkVersion(tmp, directory, found.n + 1);
      if (linked === "no directory") return false;
      if (linked === "linked") break;
    }
  } finally {
    if (tmp !== undefined) await unlink(tmp).catch(() => {});
  }
  await clean(directory, now);
  return true;
}

async function deleteKey(root: string, key: string, now: number): Promise<void> {
  // § dfd6vt90h.d@2fmja
  checkKey(key);
  await bury(root, join(root, ...key.split("/")), now, false);
}

/** Removes the directory of a key whose only version is a tombstone `SUPERSEDED_KEPT_MS` old,
 * then each parent left empty, up to the root: a key deleted leaves nothing behind.
 *
 * The tombstone goes first: a create that read it links the version after it, which stays. A
 * directory goes only by `rmdir`, which fails while it holds anything, so it never takes a
 * version linked into it; a link that finds its directory gone makes it again. */
async function reclaim(root: string, directory: string, now: number): Promise<void> {
  // § w8a4smsae@ynfyx
  const listed = await versions(directory);
  if (listed.length === 1) {
    const path = join(directory, version(listed[0] as number));
    const linked = await linkedAt(path);
    let buried = false;
    try {
      const header = new Uint8Array((await readFile(path)).subarray(0, 8));
      buried = Number(new DataView(header.buffer).getBigInt64(0)) === TOMBSTONE;
    } catch (e) {
      if (code(e) !== "ENOENT") throw new Transient(`read ${path}: ${describe(e)}`);
    }
    if (!(buried && linked !== undefined && linked <= now - SUPERSEDED_KEPT_MS)) return;
    await unlink(path).catch(() => {});
  }
  // § one w8a4smsae.a@vhh6e
  for (let at = directory; at !== root; at = dirname(at)) {
    try {
      await rmdir(at);
    } catch {
      // Not empty, as a key or a parent of keys, or removed by another sweep.
      return;
    }
  }
}

/** The directories under `directory` that could hold a key, the store's own skipped at the root,
 * each before those under it. */
async function keyDirs(directory: string, top: boolean, out: string[]): Promise<void> {
  let entries: Dirent[];
  try {
    entries = await readdir(directory, { withFileTypes: true });
  } catch (e) {
    if (ABSENT.includes(code(e) ?? "")) return;
    throw new Transient(`list ${directory}: ${describe(e)}`);
  }
  for (const entry of entries) {
    if (componentError(entry.name) !== undefined || (top && entry.name === TMP)) continue;
    if (entry.isDirectory()) {
      const path = join(directory, entry.name);
      out.push(path);
      await keyDirs(path, false, out);
    }
  }
}

/** Buries every key past its expiry at `now`, removes the versions old enough, and the keys
 * buried long enough with their empty parents; how many keys it buried. Every step is a link
 * that may fail or a removal that may find nothing, so sweeps of several processes may overlap. */
async function sweep(root: string, now: number): Promise<number> {
  // § qf98sbvzj.a@4g7q3
  const directories: string[] = [];
  await keyDirs(root, true, directories);
  let buried = 0;
  // The deepest first, so a parent is tried once its children are gone.
  for (const directory of directories.reverse()) {
    if (await bury(root, directory, now, true)) buried += 1;
    else {
      await clean(directory, now);
      await reclaim(root, directory, now);
    }
  }
  return buried;
}

/** The header of the latest version a key's directory lists, `undefined` without one: what a
 * listing needs to leave out a key whose latest version is a tombstone. */
async function listedHeader(directory: string): Promise<number | undefined> {
  for (;;) {
    const listed = await versions(directory);
    const n = listed[listed.length - 1];
    if (n === undefined) return undefined;
    const path = join(directory, version(n));
    let handle: FileHandle;
    try {
      handle = await open(path, "r");
    } catch (e) {
      // Superseded and removed: list again.
      if (code(e) === "ENOENT") continue;
      throw new Transient(`read ${path}: ${describe(e)}`);
    }
    try {
      const header = new Uint8Array(HEADER);
      await handle.read(header, 0, HEADER, 0);
      return Number(new DataView(header.buffer).getBigInt64(0));
    } finally {
      await handle.close();
    }
  }
}

async function walk(
  directory: string,
  base: string,
  prefix: string,
  keys: string[],
): Promise<void> {
  let entries: Dirent[];
  try {
    entries = await readdir(directory, { withFileTypes: true });
  } catch (e) {
    if (ABSENT.includes(code(e) ?? "")) return;
    throw new Transient(`list ${directory}: ${describe(e)}`);
  }
  for (const entry of entries) {
    const name = entry.name;
    if (componentError(name) !== undefined || (base === "" && name === TMP)) continue;
    if (!entry.isDirectory()) continue;
    const key = `${base}${name}`;
    const path = join(directory, name);
    if (key.startsWith(prefix)) {
      const header = await listedHeader(path);
      if (header !== undefined && header !== TOMBSTONE) keys.push(key);
    }
    const sub = `${key}/`;
    if (sub.startsWith(prefix) || prefix.startsWith(sub)) await walk(path, sub, prefix, keys);
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

/** `file:<path>`: the directory is the namespace, one directory of numbered versions per key,
 * each written with `link()`. For development, CI and one machine. */
export class FsStore implements Store {
  readonly capabilities: Capabilities = { wait: false };
  readonly #root: string;
  readonly #retry: StoreRetry;
  readonly #clock: Clock;
  /** When the last sweep began. */
  #swept: number | undefined;

  constructor(path: string, storeRetry: StoreRetry, clock: Clock = Date.now) {
    if (typeof path !== "string") throw new ConfigError(`a file store needs a path, not ${path}`);
    this.#root = resolve(path);
    this.#retry = storeRetry;
    this.#clock = clock;
    try {
      mkdirSync(join(path, TMP), { recursive: true });
    } catch (e) {
      throw new StoreError(`cannot prepare ${path}: ${describe(e)}`);
    }
    this.#sweepWhenDue();
  }

  /** Buries the expired keys now and removes the versions old enough; how many keys it buried. */
  sweep(): Promise<number> {
    return sweep(this.#root, this.#clock());
  }

  /** Starts a sweep at open and then daily, in the background. */
  #sweepWhenDue(): void {
    // § qf98sbvzj.a@b0ng0
    const now = this.#clock();
    if (this.#swept !== undefined && now - this.#swept < DAY_MS) return;
    this.#swept = now;
    sweep(this.#root, now).catch((e) => {
      console.warn(`substantial: the sweep of the file store ${this.#root} failed: ${describe(e)}`);
    });
  }

  #now(): number {
    this.#sweepWhenDue();
    return this.#clock();
  }

  async create(key: string, data: Uint8Array, expiry: number | undefined): Promise<Create> {
    checkDays(expiry);
    const bytes = data.slice();
    return retry.create(
      this.#retry,
      bytes,
      () => {
        const now = this.#now();
        const at = expiry === undefined ? 0 : now + expiry * DAY_MS;
        return createKey(this.#root, key, bytes, at, now);
      },
      () => getKey(this.#root, key, this.#now()),
    );
  }

  async cas(key: string, data: Uint8Array, etag: string, expiry: number | undefined): Promise<Cas> {
    checkDays(expiry);
    const bytes = data.slice();
    return retry.cas(
      this.#retry,
      bytes,
      etag,
      () => {
        const now = this.#now();
        const at = expiry === undefined ? 0 : now + expiry * DAY_MS;
        return casKey(this.#root, key, bytes, etag, at, now);
      },
      () => getKey(this.#root, key, this.#now()),
    );
  }

  get(key: string): Promise<StoreObject | undefined> {
    return retry.withRetry(this.#retry, () => getKey(this.#root, key, this.#now()));
  }

  async list(prefix: string, options: ListOptions): Promise<Listing> {
    refuseWait(options.wait);
    return retry.withRetry(this.#retry, () => listKeys(this.#root, prefix, options));
  }

  delete(key: string): Promise<void> {
    return retry.withRetry(this.#retry, () => deleteKey(this.#root, key, this.#now()));
  }

  async checkExpiry(days: readonly number[]): Promise<void> {
    checkAllDays(days);
  }

  async open(): Promise<void> {}

  async close(): Promise<void> {}
}
