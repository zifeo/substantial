// The Redis adapter, over `@redis/client`, an optional peer dependency.
import { Buffer } from "node:buffer";
import type { RedisClientOptions, RedisFunctions, RedisModules, RedisScripts } from "@redis/client";
import { read } from "../core.ts";
import { delay } from "../durations.ts";
import { ConfigError, SubstantialError } from "../errors.ts";
import type { StoreRetry } from "../settings.ts";
import { type CredentialsArg, Passwords, resolvePassword } from "./credentials.ts";
import {
  ATTEMPT_MS,
  CONNECT_MS,
  MAX_WAIT_SECONDS,
  RECOUNT_EVERY_MS,
  type RedisLocation,
  WAITAOF_MS,
} from "./redis.ts";
import * as retry from "./retry.ts";
import { Ambiguous, Fault, Refused, Transient } from "./retry.ts";
import {
  type Capabilities,
  type Create,
  compareKeys,
  type Listing,
  type ListOptions,
  type Store,
} from "./store.ts";

/** `@redis/client`, loaded when a Redis store is opened. */
export type RedisModule = typeof import("@redis/client");
type Client = ReturnType<RedisModule["createClient"]>;
type Pool = ReturnType<RedisModule["createClientPool"]>;
type Arg = string | Buffer;
/** A connection of a pool, held for the commands that must share it. */
interface Connection {
  sendCommand(args: Arg[], options?: object): Promise<unknown>;
}

// A subscription silent this long gets a PING; one silent twice as long is lost.
const PING_EVERY_MS = 10_000;

interface Script {
  readonly text: Buffer;
  readonly sha: string;
}

interface Scripts {
  readonly create: Script;
  readonly delete: Script;
}

async function shipped(name: string): Promise<Script> {
  // typescript/src/stores and typescript/dist/stores sit at the same depth, under the root that
  // holds redis/.
  const text = Buffer.from(await read(new URL(`../../../redis/${name}`, import.meta.url)));
  const digest = await crypto.subtle.digest("SHA-1", text);
  const sha = Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
  return { text, sha };
}

let scripts: Promise<Scripts> | undefined;

/** The scripts of `redis/`, as the package ships them, read once per process. */
function shippedScripts(): Promise<Scripts> {
  scripts ??= Promise.all([shipped("create.lua"), shipped("delete.lua")]).then(([c, d]) => ({
    create: c,
    delete: d,
  }));
  return scripts;
}

/** A failure of the code rather than of the server or the network: thrown as it is. */
function isBug(e: unknown): boolean {
  return (
    !(e instanceof Error) ||
    e instanceof SubstantialError ||
    e instanceof Fault ||
    e instanceof TypeError ||
    e instanceof RangeError ||
    e instanceof ReferenceError ||
    e instanceof SyntaxError
  );
}

/** A refused password or permission, in `e` or its causes. */
function isRefusal(e: unknown): boolean {
  for (let c = e; c instanceof Error; c = c.cause) {
    if (/^(WRONGPASS|NOAUTH|NOPERM)\b|called without any password configured/.test(c.message)) {
      return true;
    }
  }
  return false;
}

function describe(e: unknown): string {
  return e instanceof Error ? `${e.name}: ${e.message}` : String(e);
}

class AttemptTimeout extends Error {
  override readonly name = "TimeoutError";
}

/** `run`, failed with `AttemptTimeout` once `ms` pass first. */
async function within<T>(ms: number, what: string, run: () => Promise<T>): Promise<T> {
  const late = delay(ms);
  try {
    return await Promise.race([
      run(),
      late.promise.then(() => {
        throw new AttemptTimeout(`${what}: no answer within ${ms} ms`);
      }),
    ]);
  } finally {
    late.cancel();
  }
}

/** The fields of an `INFO` answer; a `slave<n>` field is an object of its own fields. */
function parseInfo(text: unknown): Map<string, string | Map<string, string>> {
  const info = new Map<string, string | Map<string, string>>();
  for (const line of String(text).split("\r\n")) {
    const colon = line.indexOf(":");
    if (line.startsWith("#") || colon < 0) continue;
    const name = line.slice(0, colon);
    const value = line.slice(colon + 1);
    if (/^slave[0-9]+$/.test(name)) {
      info.set(name, new Map(value.split(",").map((f) => f.split("=", 2) as [string, string])));
    } else {
      info.set(name, value);
    }
  }
  return info;
}

/** `host:port`, the host in brackets when it is an IPv6 address. */
function address(host: string, port: number): string {
  return host.includes(":") ? `[${host}]:${port}` : `${host}:${port}`;
}

function hostPort(where: string): [string, number] {
  const colon = where.lastIndexOf(":");
  return [where.slice(0, colon).replace(/^\[(.*)\]$/, "$1"), Number(where.slice(colon + 1))];
}

/** A connection pool to the primary holding the namespace, and the replicas a create waits for
 * there. */
export interface Primary {
  readonly pool: Pool;
  readonly where: string;
  readonly scripts: Scripts;
  replicas: number;
  countedAt: number;
}

/** Finds the primary of a namespace and opens a checked connection pool to it. */
class Connector {
  readonly #location: RedisLocation;
  readonly #redis: RedisModule;
  readonly #passwords: Passwords | undefined;
  readonly #slot: number;

  constructor(location: RedisLocation, credentials: CredentialsArg, redis: RedisModule) {
    const password = resolvePassword(credentials);
    this.#passwords = password === undefined ? undefined : new Passwords(password, ATTEMPT_MS);
    this.#location = location;
    this.#redis = redis;
    this.#slot = slotOf(location.namespace);
  }

  #options(
    where: string,
    db: number,
    auth: boolean,
  ): RedisClientOptions<RedisModules, RedisFunctions, RedisScripts, 2> {
    const [host, port] = hostPort(where);
    const common = { connectTimeout: CONNECT_MS, reconnectStrategy: false as const };
    const passwords = this.#passwords;
    return {
      socket: this.#location.tls
        ? { host, port, tls: true, ...common }
        : { host, port, tls: false, ...common },
      database: db,
      RESP: 2,
      // The client's own retries and queues are off: `storeRetry` is the only retry.
      disableOfflineQueue: true,
      commandOptions: { timeout: ATTEMPT_MS },
      ...(auth && passwords !== undefined
        ? {
            credentialsProvider: {
              type: "async-credentials-provider" as const,
              credentials: async () => {
                const p = await passwords.get();
                return p.username === undefined
                  ? { password: p.password }
                  : { username: p.username, password: p.password };
              },
            },
          }
        : {}),
    };
  }

  /** A client to `where`, connected by its caller and never reconnecting. */
  client(where: string, db: number, auth: boolean): Client {
    const client = this.#redis.createClient(this.#options(where, db, auth));
    // Failures reach the commands; an `error` event without a listener would end the process.
    client.on("error", () => {});
    return client as Client;
  }

  async #address(): Promise<string> {
    const loc = this.#location;
    const nodes = loc.nodes.map(([h, p]) => address(h, p));
    if (loc.topology === "standalone") return nodes[0] as string;
    const failures: string[] = [];
    for (const node of nodes) {
      // Sentinels take no password here: only the servers holding data do.
      const seed = this.client(node, 0, loc.topology === "cluster");
      try {
        const found = await within(CONNECT_MS, node, async () => {
          await seed.connect();
          if (loc.topology === "sentinel") {
            const answer = (await seed.sendCommand([
              "SENTINEL",
              "GET-MASTER-ADDR-BY-NAME",
              loc.master as string,
            ])) as [string, string] | null;
            if (answer) return address(answer[0], Number(answer[1]));
            failures.push(`${node}: monitors no ${loc.master}`);
            return undefined;
          }
          const slots = (await seed.sendCommand(["CLUSTER", "SLOTS"])) as [
            number,
            number,
            [string, number],
          ][];
          for (const [start, end, primary] of slots) {
            if (start <= this.#slot && this.#slot <= end) return address(primary[0], primary[1]);
          }
          failures.push(`${node}: no node serves slot ${this.#slot}`);
          return undefined;
        });
        if (found !== undefined) return found;
      } catch (e) {
        if (isBug(e)) throw e;
        if (isRefusal(e)) throw new Refused(`${node}: ${describe(e)}`);
        failures.push(`${node}: ${describe(e)}`);
      } finally {
        seed.destroy();
      }
    }
    throw new Transient(`no ${loc.topology} node named the primary: ${failures.join("; ")}`);
  }

  async connect(scripts: Scripts): Promise<Primary> {
    const where = await this.#address();
    const pool = this.#redis.createClientPool(this.#options(where, this.#location.db, true), {
      minimum: 1,
      // No cap, as in every host: a create holds its connection for its WAITAOF.
      maximum: Number.POSITIVE_INFINITY,
      acquireTimeout: 0,
    }) as Pool;
    pool.on("error", () => {});
    let info: Map<string, string | Map<string, string>>;
    try {
      await within(ATTEMPT_MS, `connect ${where}`, () => pool.connect());
      info = parseInfo(
        await pool.sendCommand(["INFO", "persistence", "memory", "replication", "cluster"]),
      );
      this.#check(where, info);
      try {
        await pool.sendCommand(["WAITAOF", "1", "0", String(WAITAOF_MS)]);
      } catch (e) {
        if (!(e instanceof this.#redis.ErrorReply) || isRefusal(e)) throw e;
        throw new ConfigError(`${where} has no WAITAOF, which needs Redis 7.2: ${e.message}`);
      }
      for (const script of [scripts.create, scripts.delete]) {
        const loaded = await pool.sendCommand(["SCRIPT", "LOAD", script.text]);
        if (String(loaded) !== script.sha) {
          throw new Refused(`${where} loaded a script as ${loaded}, not ${script.sha}`);
        }
      }
    } catch (e) {
      pool.destroy();
      throw e;
    }
    return { pool, where, scripts, replicas: this.replicas(info), countedAt: performance.now() };
  }

  /** The checks at open (I-26): a server that loses or evicts keys is not a store. */
  #check(where: string, info: Map<string, unknown>): void {
    const { topology } = this.#location;
    if (info.get("aof_enabled") !== "1") {
      throw new ConfigError(`${where} has no AOF: set appendonly yes and appendfsync always`);
    }
    const policy = info.get("maxmemory_policy");
    if (policy !== "noeviction") {
      throw new ConfigError(`${where} may evict keys, its maxmemory-policy is ${policy}`);
    }
    if ((info.get("cluster_enabled") === "1") !== (topology === "cluster")) {
      throw new ConfigError(`${where} is ${topology !== "cluster" ? "" : "not "}a cluster node`);
    }
    if (info.get("role") !== "master") {
      if (topology === "standalone")
        throw new ConfigError(`${where} is a replica: name its primary`);
      throw new Transient(`${where} is no longer the primary`);
    }
  }

  /** The replicas a create waits for: those of the URL, or every one online. */
  replicas(info: Map<string, unknown>): number {
    if (this.#location.replicas !== "all") return this.#location.replicas;
    let online = 0;
    for (const [name, value] of info) {
      if (name.startsWith("slave") && value instanceof Map && value.get("state") === "online") {
        online += 1;
      }
    }
    return online;
  }
}

const CRC16_TABLE = Array.from({ length: 256 }, (_, i) => {
  let crc = i << 8;
  for (let bit = 0; bit < 8; bit += 1) crc = crc & 0x8000 ? (crc << 1) ^ 0x1021 : crc << 1;
  return crc & 0xffff;
});

/** The cluster slot of the keys of a namespace, all tagged `{<ns>}`: CRC16 (XMODEM) of the tag,
 * as the cluster specification defines it. */
export function slotOf(namespace: string): number {
  let crc = 0;
  for (const byte of Buffer.from(namespace)) {
    crc = ((crc << 8) & 0xffff) ^ (CRC16_TABLE[((crc >> 8) ^ byte) & 0xff] as number);
  }
  return crc % 16384;
}

/** A flag that can be awaited until it is set. */
class Flag {
  #set = false;
  #waiting = Promise.withResolvers<void>();

  get isSet(): boolean {
    return this.#set;
  }

  set(): void {
    if (this.#set) return;
    this.#set = true;
    this.#waiting.resolve();
  }

  clear(): void {
    if (!this.#set) return;
    this.#set = false;
    this.#waiting = Promise.withResolvers<void>();
  }

  wait(): Promise<void> {
    return this.#waiting.promise;
  }
}

/** The store's one subscription to the channel of its namespace, where every create and delete
 * publishes its key, demultiplexed to the waiting listings whose prefix the key has.
 *
 * A lost subscription wakes every listing waiting on it, so each lists again once subscribed
 * anew: no notification is missed between a listing's page and its wait. */
class Notifications {
  readonly #primary: () => Promise<Primary>;
  readonly #lost: (primary: Primary) => void;
  readonly #client: (primary: Primary) => Client;
  readonly #channel: string;
  readonly #sharded: boolean;
  readonly #retry: StoreRetry;
  readonly #waiters = new Map<string, Set<Flag>>();
  readonly #ready = new Flag();
  readonly #stopped = Promise.withResolvers<never>();
  #loop: Promise<void> | undefined;
  #closed = false;
  #error = "not subscribed yet";

  constructor(
    primary: () => Promise<Primary>,
    lost: (primary: Primary) => void,
    client: (primary: Primary) => Client,
    channel: string,
    sharded: boolean,
    storeRetry: StoreRetry,
  ) {
    this.#primary = primary;
    this.#lost = lost;
    this.#client = client;
    this.#channel = channel;
    this.#sharded = sharded;
    this.#retry = storeRetry;
    this.#stopped.promise.catch(() => {});
  }

  /** A flag set once a key under `prefix` is created or deleted, or the subscription is lost,
   * from now on. */
  async watch(prefix: string): Promise<Flag> {
    if (this.#closed) throw new Refused("the store is closed");
    this.#loop ??= this.#run();
    if (!this.#ready.isSet) {
      const late = delay(2 * CONNECT_MS);
      try {
        const ready = await Promise.race([
          this.#ready.wait().then(() => true),
          late.promise.then(() => false),
        ]);
        if (!ready) throw new Transient(`no subscription to ${this.#channel}: ${this.#error}`);
      } finally {
        late.cancel();
      }
    }
    const flag = new Flag();
    const flags = this.#waiters.get(prefix) ?? new Set();
    flags.add(flag);
    this.#waiters.set(prefix, flags);
    return flag;
  }

  unwatch(prefix: string, flag: Flag): void {
    const flags = this.#waiters.get(prefix);
    if (flags === undefined) return;
    flags.delete(flag);
    if (flags.size === 0) this.#waiters.delete(prefix);
  }

  #wake(key: string | undefined): void {
    for (const [prefix, flags] of this.#waiters) {
      if (key === undefined || key.startsWith(prefix)) for (const flag of flags) flag.set();
    }
  }

  async #run(): Promise<void> {
    let wait = this.#retry.initial.ms;
    while (!this.#closed) {
      let primary: Primary | undefined;
      let client: Client | undefined;
      try {
        primary = await Promise.race([this.#primary(), this.#stopped.promise]);
        client = this.#client(primary);
        await this.#listen(client);
      } catch (e) {
        if (this.#closed) break;
        this.#error = describe(e);
        if (primary !== undefined) this.#lost(primary);
      } finally {
        if (this.#ready.isSet) wait = this.#retry.initial.ms;
        this.#ready.clear();
        this.#wake(undefined);
        client?.destroy();
      }
      const pause = delay(wait);
      try {
        await Promise.race([pause.promise, this.#stopped.promise]);
      } catch {
        break;
      } finally {
        pause.cancel();
      }
      wait = Math.min(wait * 2, this.#retry.max.ms);
    }
  }

  async #listen(client: Client): Promise<void> {
    const lost = Promise.withResolvers<never>();
    lost.promise.catch(() => {});
    client.on("error", (e: unknown) => lost.reject(e));
    client.on("end", () => lost.reject(new Transient("the subscription's connection closed")));
    // An unsubscribe the server sent, as when the slot moved to another node.
    client.on("sharded-channel-moved", () =>
      lost.reject(new Transient(`the subscription to ${this.#channel} ended: its slot moved`)),
    );
    const ended = [lost.promise, this.#stopped.promise];
    let heard = performance.now();
    const listener = (key: string) => {
      heard = performance.now();
      this.#wake(key);
    };
    const subscribe = this.#sharded ? "SSUBSCRIBE" : "SUBSCRIBE";
    await Promise.race([
      ...ended,
      within(CONNECT_MS, `${subscribe} ${this.#channel}`, async () => {
        await client.connect();
        await (this.#sharded
          ? client.sSubscribe(this.#channel, listener)
          : client.subscribe(this.#channel, listener));
      }),
    ]);
    this.#ready.set();
    for (;;) {
      const quiet = PING_EVERY_MS - (performance.now() - heard);
      if (quiet > 0) {
        const pause = delay(quiet);
        try {
          await Promise.race([...ended, pause.promise]);
        } finally {
          pause.cancel();
        }
        continue;
      }
      try {
        await Promise.race([...ended, within(PING_EVERY_MS, "PING", () => client.ping())]);
      } catch (e) {
        if (e instanceof AttemptTimeout) {
          throw new Transient(`the subscription answered no PING in ${PING_EVERY_MS} ms`);
        }
        throw e;
      }
      heard = performance.now();
    }
  }

  async close(): Promise<void> {
    this.#closed = true;
    this.#stopped.reject(new Refused("the store is closed"));
    await this.#loop;
  }
}

/** Whether `flag` was set before `deadline`, a `performance.now()`; throws once `signal`
 * aborts the listing `what`. */
async function woken(
  what: string,
  flag: Flag,
  deadline: number,
  signal: AbortSignal | undefined,
): Promise<boolean> {
  const abandoned = Promise.withResolvers<"abandoned">();
  const abandon = () => abandoned.resolve("abandoned");
  signal?.addEventListener("abort", abandon);
  try {
    // A timer may fire up to a millisecond before its delay on this clock.
    for (let left = deadline - performance.now(); left > 0; left = deadline - performance.now()) {
      const pause = delay(Math.ceil(left));
      try {
        const ended = await Promise.race([
          flag.wait().then(() => "woken" as const),
          pause.promise.then(() => "paused" as const),
          abandoned.promise,
        ]);
        if (ended === "woken") return true;
        if (ended === "abandoned") throw new Refused(`${what}: abandoned`);
      } finally {
        pause.cancel();
      }
    }
    return false;
  } finally {
    signal?.removeEventListener("abort", abandon);
  }
}

/** `redis://`, `redis+sentinel://` and `redis+cluster://`: the namespace `<ns>` is the hash tag
 * of its keys, so they live on one slot. A key is the string `{<ns>}:o:<key>` and a member of the
 * sorted set `{<ns>}:i`, its index, listed by lex range; every create and delete publishes the
 * key on `{<ns>}:c`, which waiting listings subscribe to.
 *
 * A create is the script `create.lua`, then `WAITAOF` on the same connection: acknowledged once
 * fsynced on the primary and on `replicas`. The client's own retries are off: `storeRetry` is
 * the only retry. */
export class RedisStore implements Store {
  readonly capabilities: Capabilities = { wait: true };
  readonly #location: RedisLocation;
  readonly #redis: RedisModule;
  readonly #retry: StoreRetry;
  readonly #waitaofMs: number;
  readonly #object: string;
  readonly #index: string;
  readonly #fence: string;
  readonly #channel: string;
  readonly #publish: string;
  readonly #connector: Connector;
  readonly #notifications: Notifications;
  #current: Primary | undefined;
  #connecting: Promise<Primary> | undefined;
  #closed = false;

  /** `waitaofMs` is how long a create waits for its fsyncs. */
  constructor(
    location: RedisLocation,
    credentials: CredentialsArg,
    storeRetry: StoreRetry,
    redis: RedisModule,
    waitaofMs: number,
  ) {
    this.#location = location;
    this.#redis = redis;
    this.#retry = storeRetry;
    this.#waitaofMs = waitaofMs;
    const tag = `{${location.namespace}}`;
    this.#object = `${tag}:o:`;
    this.#index = `${tag}:i`;
    this.#fence = `${tag}:f`;
    this.#channel = `${tag}:c`;
    const cluster = location.topology === "cluster";
    this.#publish = cluster ? "SPUBLISH" : "PUBLISH";
    this.#connector = new Connector(location, credentials, redis);
    this.#notifications = new Notifications(
      () => this.#primary(),
      (p) => this.#lost(p),
      (p) => this.#connector.client(p.where, location.db, true),
      this.#channel,
      cluster,
      storeRetry,
    );
  }

  /** The primary connected to, for tests. */
  get current(): Primary | undefined {
    return this.#current;
  }

  async #primary(): Promise<Primary> {
    if (this.#closed) throw new Refused("the store is closed");
    if (this.#current !== undefined) return this.#current;
    this.#connecting ??= shippedScripts()
      .then((s) => this.#connector.connect(s))
      .then(
        (primary) => {
          this.#connecting = undefined;
          if (this.#closed) {
            primary.pool.destroy();
            throw new Refused("the store is closed");
          }
          this.#current = primary;
          return primary;
        },
        (e) => {
          this.#connecting = undefined;
          throw e;
        },
      );
    return this.#connecting;
  }

  /** Forgets a primary after a failure on it, so the next attempt finds it again. */
  #lost(primary: Primary): void {
    if (this.#current === primary) {
      this.#current = undefined;
      primary.pool.destroy();
    }
  }

  /** One attempt: a failure without an answer is `Ambiguous` for a create and `Transient`
   * otherwise, an error answer `Transient`, a refused password `Refused`. */
  async #attempt<T>(what: string, op: (p: Primary) => Promise<T>, create = false): Promise<T> {
    let primary: Primary | undefined;
    try {
      primary = await this.#primary();
      const p = primary;
      return await within(ATTEMPT_MS, what, () => op(p));
    } catch (e) {
      if (isBug(e)) throw e;
      const text = `${what}: ${describe(e)}`;
      if (isRefusal(e)) throw new Refused(text);
      if (primary !== undefined) this.#lost(primary);
      if (e instanceof this.#redis.ErrorReply || primary === undefined || !create) {
        throw new Transient(text);
      }
      throw new Ambiguous(text);
    }
  }

  /** `WAITAOF` on the connection of the last write: one short of the primary and its replicas
   * leaves that write, and those before it, not durable yet.
   *
   * A separate round trip: with the write in the same pipeline, replicas acknowledge their fsync
   * only at their next second. */
  async #durable(primary: Primary, conn: Connection, what: string, created: boolean) {
    const fault = (message: string) => (created ? new Ambiguous(message) : new Transient(message));
    let replicas: number;
    try {
      replicas = await this.#replicas(primary);
    } catch (e) {
      if (e instanceof Transient) throw fault(`${what}: ${e.message}`);
      throw e;
    }
    let waited: string;
    try {
      const [local, remote] = (await conn.sendCommand([
        "WAITAOF",
        "1",
        String(replicas),
        String(this.#waitaofMs),
      ])) as [number, number];
      if (local >= 1 && remote >= replicas) return;
      waited = `[${local}, ${remote}]`;
    } catch (e) {
      if (!(e instanceof this.#redis.ErrorReply)) throw e;
      waited = describe(e);
    }
    if (this.#location.replicas === "all") await this.#count(primary);
    throw fault(`${what}: WAITAOF 1 ${replicas} answered ${waited}`);
  }

  /** The replicas a create waits for, counted again every `RECOUNT_EVERY_MS` for `all`.
   *
   * On Sentinel and Cluster, a primary with no replica online acknowledges nothing: just
   * promoted, its replicas still syncing, it could itself be replaced by one of them, which
   * would lack what it acknowledged alone. */
  async #replicas(primary: Primary): Promise<number> {
    const { replicas, topology } = this.#location;
    if (replicas !== "all") return replicas;
    if (performance.now() - primary.countedAt >= RECOUNT_EVERY_MS) await this.#count(primary);
    if (primary.replicas === 0 && topology !== "standalone") {
      throw new Transient(`${primary.where} has no replica online`);
    }
    return primary.replicas;
  }

  async #count(primary: Primary): Promise<void> {
    const info = parseInfo(await primary.pool.sendCommand(["INFO", "replication"]));
    primary.replicas = this.#connector.replicas(info);
    primary.countedAt = performance.now();
  }

  #key(key: string): string {
    return `${this.#object}${key}`;
  }

  async #put(primary: Primary, key: string, data: Uint8Array): Promise<Create> {
    await this.#replicas(primary);
    return primary.pool.execute(async (conn: Connection) => {
      const created = await conn.sendCommand([
        "EVALSHA",
        primary.scripts.create.sha,
        "3",
        this.#key(key),
        this.#index,
        this.#fence,
        key,
        Buffer.from(data.buffer, data.byteOffset, data.byteLength),
        this.#channel,
        this.#publish,
      ]);
      const outcome: Create = created === 1 ? "created" : "exists";
      await this.#durable(primary, conn, `create ${key}`, outcome === "created");
      return outcome;
    });
  }

  /** A read answered once what it saw is durable: a fence written after it on this connection,
   * then fsynced, covers it, as the AOF and the replication stream are one ordered log. A key
   * once read never disappears in a failover, nor does one once missing come back, as history
   * keys are deleted too, by collections. `resolving` marks the get that resolves an ambiguous
   * create, ambiguous again when the fence falls short. */
  #read(
    primary: Primary,
    what: string,
    command: Arg[],
    options: object,
    resolving: boolean,
  ): Promise<unknown> {
    return primary.pool.execute(async (conn: Connection) => {
      const [found] = await Promise.all([
        conn.sendCommand(command, options),
        conn.sendCommand(["INCR", this.#fence]),
      ]);
      await this.#durable(primary, conn, what, resolving);
      return found;
    });
  }

  async #get(primary: Primary, key: string, resolving: boolean): Promise<Uint8Array | undefined> {
    const bytes = { typeMapping: { [this.#redis.RESP_TYPES.BLOB_STRING]: Buffer } };
    const found = await this.#read(
      primary,
      `get ${key}`,
      ["GET", this.#key(key)],
      bytes,
      resolving,
    );
    return found === null ? undefined : new Uint8Array(found as Buffer);
  }

  async #page(
    primary: Primary,
    prefix: string,
    after: string | undefined,
    count: number,
  ): Promise<string[]> {
    let lower = Buffer.from(`[${prefix}`);
    if (after !== undefined && compareKeys(after, prefix) >= 0) lower = Buffer.from(`(${after}`);
    // UTF-8 has no byte 0xff, so every key under the prefix sorts before this bound.
    const upper = Buffer.concat([Buffer.from(`(${prefix}`), Buffer.from([0xff])]);
    const command = ["ZRANGE", this.#index, lower, upper, "BYLEX", "LIMIT", "0", String(count)];
    return (await this.#read(primary, `list ${prefix}`, command, {}, false)) as string[];
  }

  async #listOnce(prefix: string, options: ListOptions): Promise<Listing> {
    const { startAfter, maxKeys, wait, signal } = options;
    const what = `list ${prefix}`;
    if (signal?.aborted) throw new Refused(`${what}: abandoned`);
    if (wait === undefined) {
      return { keys: await this.#attempt(what, (p) => this.#page(p, prefix, startAfter, maxKeys)) };
    }
    if (!(wait.seconds >= 0 && wait.seconds <= MAX_WAIT_SECONDS)) {
      throw new Refused(`a waiting listing waits 0 to ${MAX_WAIT_SECONDS} s, not ${wait.seconds}`);
    }
    const deadline = performance.now() + wait.seconds * 1000;
    for (;;) {
      // Watching before listing: a key landing after the page wakes the wait.
      const flag = wait.seconds ? await this.#notifications.watch(prefix) : undefined;
      try {
        const page = await this.#attempt(what, (p) =>
          this.#page(p, prefix, startAfter, maxKeys + 1),
        );
        const keys = page.slice(0, maxKeys);
        const digest = await crypto.subtle.digest(
          "SHA-256",
          new TextEncoder().encode(JSON.stringify([keys, page.length > maxKeys])),
        );
        const etag = Array.from(new Uint8Array(digest).slice(0, 8), (b) =>
          b.toString(16).padStart(2, "0"),
        ).join("");
        if (flag === undefined || etag !== wait.etag) return { keys, etag };
        if (!(await woken(what, flag, deadline, signal))) return { keys, etag };
      } finally {
        if (flag !== undefined) this.#notifications.unwatch(prefix, flag);
      }
    }
  }

  async open(): Promise<void> {
    await retry.withRetry(this.#retry, () => this.#attempt("connect", async () => {}));
  }

  create(key: string, data: Uint8Array): Promise<Create> {
    const bytes = data.slice();
    return retry.create(
      this.#retry,
      bytes,
      () => this.#attempt(`create ${key}`, (p) => this.#put(p, key, bytes), true),
      () => this.#attempt(`get ${key}`, (p) => this.#get(p, key, true)),
    );
  }

  get(key: string): Promise<Uint8Array | undefined> {
    return retry.withRetry(this.#retry, () =>
      this.#attempt(`get ${key}`, (p) => this.#get(p, key, false)),
    );
  }

  list(prefix: string, options: ListOptions): Promise<Listing> {
    return retry.withRetry(this.#retry, () => this.#listOnce(prefix, options));
  }

  delete(key: string): Promise<void> {
    return retry.withRetry(this.#retry, () =>
      this.#attempt(`delete ${key}`, (p) =>
        p.pool.execute(async (conn: Connection) => {
          await conn.sendCommand([
            "EVALSHA",
            p.scripts.delete.sha,
            "3",
            this.#key(key),
            this.#index,
            this.#fence,
            key,
            this.#channel,
            this.#publish,
          ]);
          await this.#durable(p, conn, `delete ${key}`, false);
        }),
      ),
    );
  }

  async close(): Promise<void> {
    await this.#notifications.close();
    this.#closed = true;
    if (this.#current !== undefined) this.#lost(this.#current);
  }
}
