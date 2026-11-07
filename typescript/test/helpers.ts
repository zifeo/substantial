// Stores and helpers shared by the tests; S3 only when `SUBSTANTIAL_S3_URL` is set (see
// `dev/rustfs.compose.yaml`).
import { mkdtempSync } from "node:fs";
import { rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { workerWith } from "../src/client.ts";
import { milliseconds, type Substantial, seconds, type Worker } from "../src/mod.ts";
import { type InternalOptions, STORE_RETRY } from "../src/settings.ts";
import { FsStore } from "../src/stores/fs.ts";
import { MemoryStore } from "../src/stores/memory.ts";
import { openStore } from "../src/stores/open.ts";
import { type Listing, type ListOptions, type Store, StoreFailure } from "../src/stores/store.ts";
import type { AnyWorkflow } from "../src/workflow.ts";

export const LONG = seconds(10);
export const ADAPTERS = ["memory", "file", "s3"] as const;
export type Adapter = (typeof ADAPTERS)[number];
const S3_URL = process.env.SUBSTANTIAL_S3_URL;
export const NO_S3 = S3_URL === undefined ? "SUBSTANTIAL_S3_URL is not set" : false;

export function token(bytes = 8): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(bytes)), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
}

/** A fresh namespace under the one `SUBSTANTIAL_S3_URL` names, as a URL of `scheme`. */
export function s3Url(scheme: "s3" | "h3" = "s3"): string {
  if (S3_URL === undefined) throw new Error("SUBSTANTIAL_S3_URL is not set");
  const match = /^(s3|s3\+http):\/\/([^?]+)\?(.+)$/.exec(S3_URL);
  if (match === null) throw new Error(`SUBSTANTIAL_S3_URL is not an s3:// URL with a query`);
  const plain = match[1] === "s3+http" ? "+http" : "";
  return `${scheme}${plain}://${match[2]}/${token()}?${match[3]}`;
}

export function tempDir(): string {
  return mkdtempSync(join(tmpdir(), "substantial-ts-"));
}

/** A fresh namespace in an adapter of this host, and what removes it. */
export function freshStore(adapter: Adapter): { store: Store; dispose: () => Promise<void> } {
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
  const store = openStore(s3Url(), "env");
  return { store, dispose: () => store.close() };
}

/** A test run on a fresh store of each adapter; on S3 only when `SUBSTANTIAL_S3_URL` is set. */
export function eachAdapter(
  title: string,
  fn: (store: Store, adapter: Adapter) => Promise<void>,
  adapters: readonly Adapter[] = ADAPTERS,
): void {
  for (const adapter of adapters) {
    test(`${title} [${adapter}]`, { skip: adapter === "s3" && NO_S3 }, async () => {
      const { store, dispose } = freshStore(adapter);
      try {
        await fn(store, adapter);
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

/** A store around another that records what it is asked, and can fail or freeze on demand. */
export class Recording implements Store {
  readonly inner: Store;
  readonly listed: string[] = [];
  readonly created: [string, string][] = [];
  failing = false;
  /** When set, the first create of a segment freezes every operation for this many ms. */
  freezeOnSegment: number | undefined;
  #frozenUntil: number | undefined;

  constructor(inner: Store) {
    this.inner = inner;
  }

  get capabilities(): Store["capabilities"] {
    return this.inner.capabilities;
  }

  async #gate(): Promise<void> {
    if (this.#frozenUntil !== undefined) {
      const left = this.#frozenUntil - performance.now();
      if (left > 0) await new Promise((resolve) => setTimeout(resolve, left));
    }
    if (this.failing) throw new StoreFailure("injected");
  }

  async create(key: string, data: Uint8Array): Promise<"created" | "exists"> {
    if (key.includes("/log/") && this.freezeOnSegment !== undefined) {
      this.#frozenUntil = performance.now() + this.freezeOnSegment;
      this.freezeOnSegment = undefined;
    }
    await this.#gate();
    const outcome = await this.inner.create(key, data);
    this.created.push([key, outcome]);
    return outcome;
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    await this.#gate();
    return this.inner.get(key);
  }

  async list(prefix: string, options: ListOptions): Promise<Listing> {
    this.listed.push(prefix);
    await this.#gate();
    return this.inner.list(prefix, options);
  }

  async delete(key: string): Promise<void> {
    await this.#gate();
    await this.inner.delete(key);
  }

  close(): Promise<void> {
    return this.inner.close();
  }
}
