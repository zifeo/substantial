// What the Redis adapter does beyond the shared contract; all but URL parsing and credentials run
// only when `SUBSTANTIAL_REDIS_URL` names a standalone server (see `dev/redis.compose.yaml`), or,
// for the last one, `SUBSTANTIAL_REDIS_SENTINEL_URL` a Sentinel. The checks at open, which
// change the server's configuration, are in `serial/redis_checks.test.ts`.
import assert from "node:assert/strict";
import { test } from "node:test";
import { createClient } from "@redis/client";
import {
  ConfigError,
  type Context,
  minutes,
  Password,
  StoreError,
  Substantial,
  workflow,
} from "../src/mod.ts";
import { openStore } from "../src/stores/open.ts";
import { parseRedisUrl, type RedisLocation } from "../src/stores/redis.ts";
import type { RedisStore } from "../src/stores/redis_store.ts";
import { StoreFailure } from "../src/stores/store.ts";
import { LONG, noRedis, redisUrl, script, token } from "./helpers.ts";

const bytes = (text: string) => new TextEncoder().encode(text);

test("urls need every part", () => {
  const ok = parseRedisUrl("redis://h:6379/ns/sub?db=2&replicas=1");
  assert.deepEqual(ok, {
    topology: "standalone",
    tls: false,
    nodes: [["h", 6379]],
    namespace: "ns/sub",
    db: 2,
    replicas: 1,
    master: undefined,
  } satisfies RedisLocation);
  const sentinel = parseRedisUrl("rediss+sentinel://a:1,[::1]:2/ns?master=m&db=0&replicas=all");
  assert.deepEqual(sentinel, {
    topology: "sentinel",
    tls: true,
    nodes: [
      ["a", 1],
      ["::1", 2],
    ],
    namespace: "ns",
    db: 0,
    replicas: "all",
    master: "m",
  } satisfies RedisLocation);
  const cluster = parseRedisUrl("redis+cluster://a:1,b:2/ns?replicas=all");
  assert.deepEqual(
    [cluster.topology, cluster.nodes, cluster.replicas],
    [
      "cluster",
      [
        ["a", 1],
        ["b", 2],
      ],
      "all",
    ],
  );
  assert.equal(parseRedisUrl("redis://h:1/ns?db=0&replicas=all").replicas, "all");
  for (const url of [
    "redis://h:1/ns?db=0",
    "redis://h:1/ns?replicas=0",
    "redis://h/ns?db=0&replicas=0",
    "redis://h:1,g:2/ns?db=0&replicas=0",
    "redis://h:1/?db=0&replicas=0",
    "redis://h:1/a{b}?db=0&replicas=0",
    "redis://h:1/a//b?db=0&replicas=0",
    "redis://h:1/ns?db=x&replicas=0",
    "redis://h:1/ns?db=\u0661&replicas=0",
    "redis://h:1/ns?db=0&replicas=\u0661",
    "redis://h:\u0661/ns?db=0&replicas=0",
    "redis://h:1/ns?db=0&replicas=-1",
    "redis://h:1/ns?db=0&db=1&replicas=0",
    "redis://h:1/ns?db=0&replicas=0&timeout=1",
    "redis://u:p@h:1/ns?db=0&replicas=0",
    "redis://h:1/ns?db=0&replicas=0#x",
    "redis+sentinel://a:1/ns?db=0&replicas=all",
    "redis+sentinel://a:1/ns?master=&db=0&replicas=all",
    "redis+sentinel://a:1/ns?master=m&db=0&replicas=1",
    "redis+cluster://a:1/ns?replicas=0",
    "redis+cluster://a:1/ns?db=0&replicas=all",
    "valkey://h:1/ns?db=0&replicas=0",
  ]) {
    assert.throws(() => parseRedisUrl(url), ConfigError, url);
  }
});

test("credentials are a password", async () => {
  const url = "redis://127.0.0.1:1/ns?db=0&replicas=0";
  const password = new Password({ password: "hunter2", username: "app" });
  assert.ok(!JSON.stringify(password).includes("hunter2"));
  for (const wrong of ["", undefined]) {
    assert.throws(() => new Password({ password: wrong as string }), ConfigError);
  }
  const later = new Date(Date.now() + 3_600_000);
  assert.throws(
    () => new Password({ password: "p", expiresAt: later } as { password: string }),
    (e) => e instanceof ConfigError && /a provider returns/.test(e.message),
  );
  await assert.rejects(
    openStore(url, "environment" as "env"),
    (e) => e instanceof ConfigError && /a Redis store takes/.test(e.message),
  );
  const saved = process.env.REDIS_PASSWORD;
  try {
    delete process.env.REDIS_PASSWORD;
    await assert.rejects(openStore(url, "env"), /REDIS_PASSWORD/);
    process.env.REDIS_PASSWORD = "p";
    await (await openStore(url, "env")).close();
  } finally {
    if (saved === undefined) delete process.env.REDIS_PASSWORD;
    else process.env.REDIS_PASSWORD = saved;
  }
  await (await openStore(url, undefined)).close();
  await assert.rejects(
    openStore("s3+http://127.0.0.1:1/b/ns?region=r", new Password({ password: "p" })),
    ConfigError,
  );
});

/** A client of the standalone server, for what the tests check or change on it. */
async function admin(): Promise<ReturnType<typeof createClient>> {
  const [[host, port]] = parseRedisUrl(redisUrl("redis")).nodes as [[string, number]];
  const client = createClient({ socket: { host, port } });
  await client.connect();
  return client as ReturnType<typeof createClient>;
}

const skip = noRedis("redis");

test("a password reaches the server", { skip }, async () => {
  const server = await admin();
  const user = `substantial-test-${token(4)}`;
  await server.sendCommand(["ACL", "SETUSER", user, "on", ">right", "~*", "&*", "+@all"]);
  const url = redisUrl("redis");
  let calls = 0;
  const provider = () => {
    calls += 1;
    return { password: calls === 1 ? "wrong" : "right", username: user };
  };
  try {
    await assert.rejects(
      Substantial.create(url, { partitions: 1, credentials: provider }),
      (e) => e instanceof StoreError && /WRONGPASS|invalid username-password/.test(e.message),
    );
    const sub = await Substantial.create(url, { partitions: 1, credentials: provider });
    await sub.close();
    assert.equal(calls, 2, "asked again after a failed connection, then once");
    const right = new Password({ password: "right", username: user });
    const explicit = await Substantial.open(url, { credentials: right });
    await explicit.close();
  } finally {
    await server.sendCommand(["ACL", "DELUSER", user]);
    server.destroy();
  }
});

test("a waiting listing returns as a key lands without polling", { skip }, async () => {
  const url = redisUrl("redis");
  const index = `{${parseRedisUrl(url).namespace}}:i`;
  // MONITOR, not the server's command counts: other tests list on the same server meanwhile.
  const server = await admin();
  const monitor = server.duplicate();
  await monitor.connect();
  let listed = 0;
  await monitor.monitor((line: string) => {
    if (/"ZRANGE"/i.test(line) && line.includes(`"${index}"`)) listed += 1;
  });
  const store = await openStore(url, undefined);
  try {
    await store.create("q/a", bytes("a"));
    const first = await store.list("q/", { maxKeys: 10, wait: { seconds: 0 } });
    assert.deepEqual(first.keys, ["q/a"]);
    assert.ok(first.etag);
    const before = listed;
    let began = performance.now();
    const unchanged = await store.list("q/", {
      maxKeys: 10,
      wait: { seconds: 2, etag: first.etag },
    });
    assert.deepEqual(unchanged, first);
    assert.ok(performance.now() - began >= 2000);
    assert.equal(listed - before, 1, "listed once, then waited");
    let done = false;
    const held = store
      .list("q/", { maxKeys: 10, wait: { seconds: 20, etag: first.etag } })
      .finally(() => {
        done = true;
      });
    await new Promise((resolve) => setTimeout(resolve, 500));
    await store.create("other/x", bytes("x"));
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.ok(!done, "a key under another prefix does not end the wait");
    began = performance.now();
    await store.create("q/b", bytes("b"));
    const woken = await held;
    assert.ok(performance.now() - began < 500);
    assert.deepEqual(woken.keys, ["q/a", "q/b"]);
    assert.notEqual(woken.etag, first.etag);
  } finally {
    await store.close();
    monitor.destroy();
    server.destroy();
  }
});

test("a waiting listing on a closed store fails at once", { skip }, async () => {
  const store = await openStore(redisUrl("redis"), undefined);
  await store.list("q/", { maxKeys: 10, wait: { seconds: 1 } });
  await store.close();
  const began = performance.now();
  await assert.rejects(
    store.list("q/", { maxKeys: 10, wait: { seconds: 1 } }),
    (e) => e instanceof StoreFailure && /closed/.test(e.message),
  );
  assert.ok(performance.now() - began < 1000);
});

/** An ACL user of password `right` allowed `commands`, deleted by `drop`. */
async function user(
  server: ReturnType<typeof createClient>,
  ...commands: string[]
): Promise<{ name: string; drop: () => Promise<void> }> {
  const name = `substantial-test-${token(4)}`;
  await server.sendCommand(["ACL", "SETUSER", name, "on", ">right", "~*", "&*", ...commands]);
  return { name, drop: async () => void (await server.sendCommand(["ACL", "DELUSER", name])) };
}

test("a user denied waitaof is refused as such", { skip }, async () => {
  const server = await admin();
  const { name, drop } = await user(server, "+@all", "-waitaof");
  try {
    await assert.rejects(
      Substantial.create(redisUrl("redis"), {
        partitions: 1,
        credentials: new Password({ password: "right", username: name }),
      }),
      (e) => e instanceof StoreError && /no permissions/.test(e.message),
    );
  } finally {
    await drop();
    server.destroy();
  }
});

test("a subscription lost again resubscribes at once", { skip }, async () => {
  const server = await admin();
  const { name, drop } = await user(server, "+@all");
  const url = redisUrl("redis");
  const channel = `{${parseRedisUrl(url).namespace}}:c`;
  const store = await openStore(url, new Password({ password: "right", username: name }));
  try {
    await store.list("q/", { maxKeys: 10, wait: { seconds: 1 } });
    for (let i = 0; i < 6; i++) {
      assert.ok(Number(await server.sendCommand(["CLIENT", "KILL", "USER", name])) >= 1);
      const began = performance.now();
      for (;;) {
        const [, subscribers] = (await server.sendCommand(["PUBSUB", "NUMSUB", channel])) as [
          string,
          number,
        ];
        if (subscribers > 0) break;
        assert.ok(performance.now() - began < 1000, "subscribed again within a second");
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    }
  } finally {
    await store.close();
    await drop();
    server.destroy();
  }
});

test("a start from another client comes through the wait", { skip }, async () => {
  const answer = workflow("answer", async (_c: Context) => 42);
  const url = redisUrl("redis");
  const sub = await Substantial.create(url, { partitions: 1 });
  try {
    await using _ = script(sub, [answer], { maxIdle: minutes(5) });
    await new Promise((resolve) => setTimeout(resolve, 500));
    const other = await Substantial.open(url);
    try {
      const began = performance.now();
      const run = await other.start(answer);
      assert.equal(await run.result({ timeout: LONG }), 42);
      assert.ok(performance.now() - began < 2000);
    } finally {
      await other.close();
    }
  } finally {
    await sub.close();
  }
});

test("a run ends on redis through a script worker", { skip }, async () => {
  const double = workflow("double", async (_c: Context, input: { n: number }) => input.n * 2);
  const sub = await Substantial.create(redisUrl("redis"), { partitions: 2 });
  try {
    await using _ = script(sub, [double]);
    assert.equal(await (await sub.start(double, { n: 21 })).result({ timeout: LONG }), 42);
  } finally {
    await sub.close();
  }
});

test("a primary without a replica online answers nothing", {
  skip: noRedis("redis-sentinel"),
}, async () => {
  const url = redisUrl("redis-sentinel");
  const store = (await openStore(url, undefined)) as RedisStore;
  try {
    await store.open();
    assert.ok(store.current);
    // As just promoted, its replicas still syncing.
    store.current.replicas = 0;
    for (const answer of [
      () => store.create("k", bytes("v")),
      () => store.get("k"),
      () => store.delete("k"),
    ]) {
      await assert.rejects(
        answer(),
        (e) => e instanceof StoreFailure && /no replica online/.test(e.message),
      );
    }
  } finally {
    await store.close();
  }
  const counted = await openStore(url, undefined);
  try {
    assert.equal(await counted.get("k"), undefined, "nothing written");
  } finally {
    await counted.close();
  }
});
