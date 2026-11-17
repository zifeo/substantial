// Ambiguous creates on Redis (I-26), injected by a proxy that drops a connection; only when
// `SUBSTANTIAL_REDIS_URL` names a standalone server (see `dev/redis.compose.yaml`).
import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { createServer, connect as dial, type Server, type Socket } from "node:net";
import { test } from "node:test";
import * as redis from "@redis/client";
import { STORE_RETRY } from "../src/settings.ts";
import { openStore } from "../src/stores/open.ts";
import { parseRedisUrl } from "../src/stores/redis.ts";
import { RedisStore } from "../src/stores/redis_store.ts";
import { StoreFailure } from "../src/stores/store.ts";
import { noRedis, redisUrl } from "./helpers.ts";

const bytes = (text: string) => new TextEncoder().encode(text);
const skip = noRedis("redis");

/** A TCP proxy to the server that drops the connection of the next request holding `marker`:
 * after the server got it, so it runs and its answer is lost, or before. */
class Dropping {
  readonly #host: string;
  readonly #port: number;
  readonly #sockets = new Set<Socket>();
  #server: Server | undefined;
  marker: Buffer | undefined;
  forward = false;
  dropped = 0;
  port = 0;

  constructor(host: string, port: number) {
    this.#host = host;
    this.#port = port;
  }

  drop(marker: string, forward: boolean): void {
    this.marker = Buffer.from(marker);
    this.forward = forward;
  }

  #serve(client: Socket): void {
    const upstream = dial(this.#port, this.#host);
    for (const socket of [client, upstream]) {
      this.#sockets.add(socket);
      socket.on("close", () => this.#sockets.delete(socket));
      socket.on("error", () => {});
    }
    const end = () => {
      client.destroy();
      upstream.destroy();
    };
    client.on("close", end);
    upstream.on("close", end);
    let muted = false;
    client.on("data", (data: Buffer) => {
      if (muted) return;
      if (this.marker !== undefined && data.includes(this.marker)) {
        this.marker = undefined;
        this.dropped += 1;
        muted = true;
        if (this.forward) {
          upstream.write(data);
          setTimeout(end, 200);
        } else {
          end();
        }
        return;
      }
      upstream.write(data);
    });
    upstream.on("data", (data: Buffer) => {
      if (!muted) client.write(data);
    });
  }

  async start(): Promise<void> {
    const server = createServer((client) => this.#serve(client));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    this.#server = server;
    this.port = (server.address() as { port: number }).port;
  }

  async close(): Promise<void> {
    for (const socket of this.#sockets) socket.destroy();
    await new Promise((resolve) => this.#server?.close(resolve));
  }
}

interface Proxied {
  url: string;
  proxy: Dropping;
  fence: () => Promise<number>;
  admin: ReturnType<typeof redis.createClient>;
  fenceKey: string;
}

async function proxied(fn: (p: Proxied) => Promise<void>): Promise<void> {
  const url = redisUrl("redis");
  const location = parseRedisUrl(url);
  const [[host, port]] = location.nodes as [[string, number]];
  const proxy = new Dropping(host, port);
  await proxy.start();
  const admin = redis.createClient({ socket: { host, port } });
  await admin.connect();
  const fenceKey = `{${location.namespace}}:f`;
  try {
    await fn({
      url: url.replace(`${host}:${port}`, `127.0.0.1:${proxy.port}`),
      proxy,
      fence: async () => Number((await admin.get(fenceKey)) ?? 0),
      admin: admin as ReturnType<typeof redis.createClient>,
      fenceKey,
    });
  } finally {
    admin.destroy();
    await proxy.close();
  }
}

for (const [id, forward] of [
  ["landed", true],
  ["lost", false],
] as const) {
  test(`an unanswered create is resolved by a fenced get [${id}]`, { skip }, () =>
    proxied(async ({ url, proxy, fence }) => {
      const store = await openStore(url, undefined);
      try {
        await store.open();
        const before = await fence();
        proxy.drop("EVALSHA", forward);
        assert.equal(await store.create("runs/a/log/0", bytes("one")), "created");
        assert.equal(proxy.dropped, 1);
        assert.equal(await fence(), before + 1, "the get resolving it wrote the fence");
        assert.deepEqual(await store.get("runs/a/log/0"), bytes("one"));
        assert.equal(await store.create("runs/a/log/0", bytes("two")), "exists");
      } finally {
        await store.close();
      }
    }),
  );
}

/** A store of `url` whose creates and reads wait 50 ms for their fsyncs. */
function impatient(url: string): RedisStore {
  return new RedisStore(parseRedisUrl(url), undefined, STORE_RETRY, redis, 50);
}

test("a create the replicas never fsync is not acknowledged", { skip }, () =>
  proxied(async ({ url, admin, fenceKey }) => {
    const store = impatient(url.replace("replicas=0", "replicas=1"));
    try {
      await assert.rejects(
        store.create("runs/a/log/0", bytes("one")),
        (e) => e instanceof StoreFailure && /WAITAOF 1 1 answered \[1, 0\]/.test(e.message),
      );
      const landed = await admin.get(fenceKey.replace(/:f$/, ":o:runs/a/log/0"));
      assert.equal(landed, "one", "it landed, never acknowledged");
    } finally {
      await store.close();
    }
  }),
);

test("reads and deletes answer once what they saw is durable", { skip }, () =>
  proxied(async ({ url, fence }) => {
    const store = await openStore(url, undefined);
    try {
      await store.create("runs/a/start", bytes("one"));
      const before = await fence();
      assert.deepEqual(await store.get("runs/a/start"), bytes("one"));
      assert.equal(await store.get("runs/b/start"), undefined);
      assert.deepEqual((await store.list("runs/", { maxKeys: 10 })).keys, ["runs/a/start"]);
      await store.delete("runs/b/start");
      assert.equal(await fence(), before + 4, "each read and a delete of nothing");
    } finally {
      await store.close();
    }
    const unreplicated = impatient(url.replace("replicas=0", "replicas=1"));
    try {
      const short = (e: unknown) =>
        e instanceof StoreFailure && /WAITAOF 1 1 answered \[1, 0\]/.test(e.message);
      for (const key of ["runs/a/start", "runs/b/start"]) {
        await assert.rejects(unreplicated.get(key), short);
        await assert.rejects(unreplicated.list(key, { maxKeys: 10 }), short);
        await assert.rejects(unreplicated.delete(key), short);
      }
    } finally {
      await unreplicated.close();
    }
  }),
);
