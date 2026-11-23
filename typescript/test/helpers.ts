// Stores and helpers shared by the tests. The S3 and Redis stores are those of
// `dev/compose.yaml`, at the URLs of `dev/stores.env` unless the environment sets them; a test
// that needs one it cannot reach fails.
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, readFileSync } from "node:fs";
import { rm } from "node:fs/promises";
import { connect, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type TestContext, test } from "node:test";
import { parseEnv } from "node:util";
import { fromBinary } from "@bufbuild/protobuf";
import { workerWith } from "../src/client.ts";
import {
  type Failure,
  milliseconds,
  type Run,
  type Substantial,
  seconds,
  type Worker,
} from "../src/mod.ts";
import { layout } from "../src/proto.ts";
import { type InternalOptions, STORE_RETRY } from "../src/settings.ts";
import { FsStore } from "../src/stores/fs.ts";
import { MemoryStore } from "../src/stores/memory.ts";
import { openStore } from "../src/stores/open.ts";
import {
  type Cas,
  type Create,
  type Listing,
  type ListOptions,
  type Store,
  StoreFailure,
  type StoreObject,
} from "../src/stores/store.ts";
import type { AnyWorkflow } from "../src/workflow.ts";

export const LONG = seconds(10);
const REDIS_URLS = {
  redis: "SUBSTANTIAL_REDIS_URL",
  "redis-sentinel": "SUBSTANTIAL_REDIS_SENTINEL_URL",
  "redis-cluster": "SUBSTANTIAL_REDIS_CLUSTER_URL",
} as const;
export type Topology = keyof typeof REDIS_URLS;
export const ADAPTERS = [
  "memory",
  "file",
  "s3",
  "redis",
  "redis-sentinel",
  "redis-cluster",
] as const;
export type Adapter = (typeof ADAPTERS)[number];
export const UP = "docker compose -f dev/compose.yaml up -d --wait";

const STORES = new URL("../../dev/stores.env", import.meta.url);
for (const [name, value] of Object.entries(parseEnv(readFileSync(STORES, "utf8")))) {
  process.env[name] ??= value;
}

const reached = new Map<string, Promise<string>>();

/** The URL `variable` names, once each of its hosts accepts a connection. */
function reach(variable: string): Promise<string> {
  let url = reached.get(variable);
  if (url === undefined) {
    url = reachEach(variable);
    reached.set(variable, url);
  }
  return url;
}

async function reachEach(variable: string): Promise<string> {
  const url = process.env[variable];
  if (!url) throw new Error(`${variable} is not set: see dev/stores.env and run \`${UP}\``);
  const hosts = url.slice(url.indexOf("://") + 3).split("/")[0] ?? "";
  for (const address of hosts.split(",")) {
    const at = address.lastIndexOf(":");
    const port = Number(address.slice(at + 1));
    if (at <= 0 || !Number.isInteger(port) || port <= 0) {
      throw new Error(`${variable} names ${JSON.stringify(address)}, not a host:port`);
    }
    await new Promise<void>((resolve, reject) => {
      const socket = connect({ host: address.slice(0, at), port, timeout: 2000 });
      socket.once("connect", () => {
        socket.destroy();
        resolve();
      });
      socket.once("timeout", () => socket.destroy(new Error("timed out")));
      socket.once("error", (e) =>
        reject(
          new Error(
            `${variable} names ${address}, which does not answer (${e.message}): run \`${UP}\``,
          ),
        ),
      );
    });
  }
  return url;
}

/** The lock every test process of the machine takes, whatever its host, as Python's
 * `failover_exclusion` does: shared by a test that uses the Redis stores, exclusive by one that
 * stops their containers or changes a server's configuration. */
const FAILOVER_LOCK = join(tmpdir(), "substantial-tests-failover.lock");
/** Holds an flock(2) of `argv[1]` until its stdin closes, so the lock goes with this process. */
const HOLD = `import fcntl, sys
f = open(sys.argv[1], "a")
fcntl.flock(f, fcntl.LOCK_EX if sys.argv[2] == "exclusive" else fcntl.LOCK_SH)
print("locked", flush=True)
sys.stdin.read()
`;

/** Takes the failover lock, through a Python process, since Node offers no flock; the returned
 * function releases it. */
export async function failoverLock(exclusive: boolean): Promise<() => Promise<void>> {
  const mode = exclusive ? "exclusive" : "shared";
  const child = spawn("python3", ["-c", HOLD, FAILOVER_LOCK, mode], {
    stdio: ["pipe", "pipe", "inherit"],
  });
  const exited = once(child, "exit");
  const [line] = (await Promise.race([once(child.stdout, "data"), exited])) as [unknown];
  if (String(line).trim() !== "locked") throw new Error(`cannot take ${FAILOVER_LOCK} ${mode}`);
  child.stdout.destroy();
  // The pipe closes when this process exits, which releases a lock still held.
  (child.stdin as unknown as Socket).unref();
  child.unref();
  return async () => {
    child.ref();
    child.stdin.end();
    await exited;
  };
}

let failovers = false;
let shared: Promise<unknown> | undefined;

/** Marks this process as one whose tests each take the failover lock exclusively. */
export function holdsFailovers(): void {
  failovers = true;
}

/** A fresh namespace under the one the variable of `topology` names. A process using one holds
 * the failover lock shared until it exits, unless its tests take it exclusively. */
export async function redisUrl(topology: Topology): Promise<string> {
  if (!failovers) {
    shared ??= failoverLock(false);
    await shared;
  }
  const url = await reach(REDIS_URLS[topology]);
  const q = url.indexOf("?");
  return `${url.slice(0, q)}/${token()}${url.slice(q)}`;
}

export function token(bytes = 8): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(bytes)), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
}

/** The versioned bucket, which RustFS serves short of the store contract: only to check that. */
export const S3_VERSIONED_URL = "SUBSTANTIAL_S3_VERSIONED_URL";

/** A fresh namespace under the one `variable` names, as a URL of `scheme`. */
export async function s3Url(
  scheme: "s3" | "h3" = "s3",
  variable = "SUBSTANTIAL_S3_URL",
): Promise<string> {
  const match = /^(s3|s3\+http):\/\/([^?]+)\?(.+)$/.exec(await reach(variable));
  if (match === null) throw new Error(`${variable} is not an s3:// URL with a query`);
  const plain = match[1] === "s3+http" ? "+http" : "";
  return `${scheme}${plain}://${match[2]}/${token()}?${match[3]}`;
}

export function tempDir(): string {
  return mkdtempSync(join(tmpdir(), "substantial-ts-"));
}

/** A fresh namespace in an adapter of this host, and what removes it. */
export async function freshStore(
  adapter: Adapter,
): Promise<{ store: Store; dispose: () => Promise<void> }> {
  if (adapter === "memory") {
    return { store: new MemoryStore(`test-${token()}`), dispose: async () => {} };
  }
  if (adapter === "file") {
    const dir = tempDir();
    return {
      store: new FsStore(join(dir, "ns"), STORE_RETRY),
      dispose: () => rm(dir, { recursive: true, force: true }),
    };
  }
  const store =
    adapter === "s3"
      ? await openStore(await s3Url(), "env")
      : await openStore(await redisUrl(adapter), undefined);
  return { store, dispose: () => store.close() };
}

/** A create's outcome without the ETag it carries. */
export function kind(outcome: Create): "created" | "exists" {
  return outcome === "exists" ? "exists" : "created";
}

/** The value of `key`, without its ETag. */
export async function value(store: Store, key: string): Promise<Uint8Array | undefined> {
  return (await store.get(key))?.data;
}

/** A test run on a fresh store of each adapter. */
export function eachAdapter(
  title: string,
  fn: (store: Store, adapter: Adapter, t: TestContext) => Promise<void>,
  adapters: readonly Adapter[] = ADAPTERS,
): void {
  for (const adapter of adapters) {
    test(`${title} [${adapter}]`, async (t) => {
      const { store, dispose } = await freshStore(adapter);
      try {
        await fn(store, adapter, t);
      } finally {
        await dispose();
      }
    });
  }
}

type Overrides = Omit<InternalOptions, "preset">;

/** The script preset, listing often so that tests need not wait for `maxIdle`; hidden settings
 * may be overridden too. */
export function script(
  sub: Substantial,
  workflows: AnyWorkflow[],
  overrides: Overrides = {},
): Worker {
  return workerWith(sub, workflows, { preset: "script", maxIdle: milliseconds(50), ...overrides });
}

export async function eventually(
  what: string,
  check: () => Promise<boolean> | boolean,
  withinMs = 5000,
): Promise<void> {
  const deadline = performance.now() + withinMs;
  while (!(await check())) {
    if (performance.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/** The error blocking `run` of `workflow`, once the listing of its live runs says it is
 * blocked. */
export async function blockedBy(
  sub: Substantial,
  workflow: AnyWorkflow,
  run: Run<unknown>,
  withinMs = 10_000,
): Promise<Failure> {
  let error: Failure | undefined;
  await eventually(
    `run ${run.chain} blocked`,
    async () => {
      for await (const info of sub.runs(workflow)) {
        if (info.chain === run.chain && info.waitingOn === "blocked") error = info.error;
      }
      return error !== undefined;
    },
    withinMs,
  );
  return error as Failure;
}

/** A store around another that records what it is asked, and can fail or freeze on demand. */
export class Recording implements Store {
  readonly inner: Store;
  readonly listed: string[] = [];
  readonly created: [string, string][] = [];
  readonly cased: [string, string][] = [];
  /** Every create and cas asked, with the expiry it carries. */
  readonly writes: [string, number | undefined][] = [];
  /** The days of every `checkExpiry` asked. */
  readonly checked: number[][] = [];
  failing = false;
  /** When set, the first write of a segment freezes every operation until this settles. */
  freezeOnSegment: Promise<void> | undefined;
  #frozen: Promise<void> | undefined;
  /** When set, a delete of this key waits until its promise settles. */
  holdDelete: [string, Promise<void>] | undefined;

  constructor(inner: Store) {
    this.inner = inner;
  }

  get capabilities(): Store["capabilities"] {
    return this.inner.capabilities;
  }

  async #gate(): Promise<void> {
    await this.#frozen;
    if (this.failing) throw new StoreFailure("injected");
  }

  /** Whether a cas of `data` on `key` writes a segment: a chain's head holding more segments
   * than the one the store holds, a lease or a clearing write adding none. */
  async #writesSegment(key: string, data: Uint8Array): Promise<boolean> {
    const [root, id, ...rest] = key.split("/");
    if (root !== "runs" || id === undefined || rest.length > 0) return false;
    const segments = (bytes: Uint8Array | undefined) => {
      if (bytes === undefined) return 0;
      const run = fromBinary(layout.RunSchema, bytes);
      return run.kind.case === "head" ? run.kind.value.segments.length : 0;
    };
    return segments(data) > segments((await this.inner.get(key))?.data);
  }

  async create(key: string, data: Uint8Array, expiry: number | undefined): Promise<Create> {
    this.writes.push([key, expiry]);
    await this.#gate();
    const outcome = await this.inner.create(key, data, expiry);
    this.created.push([key, kind(outcome)]);
    return outcome;
  }

  async cas(key: string, data: Uint8Array, etag: string, expiry: number | undefined): Promise<Cas> {
    this.writes.push([key, expiry]);
    if (this.freezeOnSegment !== undefined && (await this.#writesSegment(key, data))) {
      this.#frozen = this.freezeOnSegment;
      this.freezeOnSegment = undefined;
    }
    await this.#gate();
    const outcome = await this.inner.cas(key, data, etag, expiry);
    this.cased.push([key, outcome === "mismatch" ? outcome : "written"]);
    return outcome;
  }

  checkExpiry(days: readonly number[]): Promise<void> {
    this.checked.push([...days]);
    return this.inner.checkExpiry(days);
  }

  async get(key: string): Promise<StoreObject | undefined> {
    await this.#gate();
    return this.inner.get(key);
  }

  async list(prefix: string, options: ListOptions): Promise<Listing> {
    this.listed.push(prefix);
    await this.#gate();
    return this.inner.list(prefix, options);
  }

  async delete(key: string): Promise<void> {
    if (this.holdDelete !== undefined && this.holdDelete[0] === key) await this.holdDelete[1];
    await this.#gate();
    await this.inner.delete(key);
  }

  open(): Promise<void> {
    return this.inner.open();
  }

  close(): Promise<void> {
    return this.inner.close();
  }
}
