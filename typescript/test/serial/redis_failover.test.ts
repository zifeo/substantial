// Failovers during a stream of creates: the primary holding the namespace is killed, a replica
// takes over, and every create acknowledged before or during it is there after, none acknowledged
// twice. Only with the containers of `dev/redis-sentinel.compose.yaml` and
// `dev/redis-cluster.compose.yaml` and their variables set; each test starts the killed container
// again and waits for it to rejoin as a replica. Run apart from the other tests, which would meet
// the failover.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { type TestContext, test } from "node:test";
import { promisify } from "node:util";
import { createClient } from "@redis/client";
import { openStore } from "../../src/stores/open.ts";
import type { RedisStore } from "../../src/stores/redis_store.ts";
import { StoreFailure } from "../../src/stores/store.ts";
import { noRedis, redisUrl } from "../helpers.ts";

const run = promisify(execFile);
const WRITERS_PER_KEY = 2;
const KEYS_AT_ONCE = 4;
const AFTER_FAILOVER_MS = 3000;
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** The container `name`, or why the test is skipped. */
async function container(name: string): Promise<{ name: string } | { skip: string }> {
  try {
    await run("docker", ["inspect", name]);
    return { name };
  } catch (e) {
    const missing = (e as { code?: unknown }).code === "ENOENT";
    return { skip: missing ? "docker is not on the PATH" : `no container ${name}` };
  }
}

async function until(what: string, check: () => Promise<boolean>, withinMs = 60_000) {
  const deadline = performance.now() + withinMs;
  for (;;) {
    try {
      if (await check()) return;
    } catch {}
    assert.ok(performance.now() < deadline, `timed out waiting for ${what}`);
    await sleep(200);
  }
}

/** A command to the server at `where`, `host:port`, without a password. */
async function ask(where: string, command: string[]): Promise<unknown> {
  const colon = where.lastIndexOf(":");
  const client = createClient({
    socket: { host: where.slice(0, colon), port: Number(where.slice(colon + 1)) },
    RESP: 2,
  });
  client.on("error", () => {});
  await client.connect();
  try {
    return await client.sendCommand(command);
  } finally {
    client.destroy();
  }
}

/** The fields of an `INFO replication` answer. */
async function replication(where: string): Promise<Map<string, string>> {
  const text = String(await ask(where, ["INFO", "replication"]));
  return new Map(
    text
      .split("\r\n")
      .filter((line) => line.includes(":"))
      .map((line) => [line.slice(0, line.indexOf(":")), line.slice(line.indexOf(":") + 1)]),
  );
}

async function role(where: string): Promise<string> {
  const info = await replication(where);
  return `${info.get("role")}:${info.get("master_link_status") ?? ""}`;
}

/** Creates keys, each by two writers at once with their own bytes, while the primary is killed,
 * then checks each answer against the store once the old primary is back. */
async function streamThroughFailover(
  t: TestContext,
  url: string,
  prefix: string,
  failedOver: (old: string) => Promise<boolean>,
): Promise<void> {
  const store = (await openStore(url, undefined)) as RedisStore;
  await store.open();
  const where = (store.current as { where: string }).where;
  const found = await container(`${prefix}-${where.slice(where.lastIndexOf(":") + 1)}`);
  if ("skip" in found) {
    await store.close();
    return t.skip(found.skip);
  }
  const killed = found.name;
  const answers = new Map<string, ["created" | "exists", string][]>();
  let failures = 0;
  let stop = false;

  const writer = async (slot: number, writerId: number) => {
    for (let n = 0; !stop; n += 1) {
      const key = `k/${slot}-${String(n).padStart(6, "0")}`;
      const data = `${key} by ${writerId}`;
      try {
        const outcome = await store.create(key, new TextEncoder().encode(data));
        answers.set(key, [...(answers.get(key) ?? []), [outcome, data]]);
      } catch (e) {
        if (!(e instanceof StoreFailure)) throw e;
        failures += 1;
      }
    }
  };

  const writers = Array.from({ length: KEYS_AT_ONCE * WRITERS_PER_KEY }, (_, i) =>
    writer(Math.floor(i / WRITERS_PER_KEY), i % WRITERS_PER_KEY),
  );
  let promotedIn: number;
  try {
    await sleep(1000);
    await run("docker", ["kill", killed]);
    const killedAt = performance.now();
    await until("a replica promoted", () => failedOver(where));
    promotedIn = performance.now() - killedAt;
    await sleep(AFTER_FAILOVER_MS);
  } finally {
    stop = true;
    await Promise.all(writers);
    await run("docker", ["start", killed]);
  }
  await store.close();
  await until("the old primary back as a replica", async () => (await role(where)) === "slave:up");

  const check = await openStore(url, undefined);
  let acknowledged = 0;
  try {
    for (const [key, outcomes] of answers) {
      const created = outcomes.filter(([outcome]) => outcome === "created").map(([, d]) => d);
      assert.ok(created.length <= 1, `${key} created twice: ${JSON.stringify(outcomes)}`);
      const stored = await check.get(key);
      const text = stored === undefined ? undefined : new TextDecoder().decode(stored);
      if (created.length > 0) {
        acknowledged += 1;
        assert.equal(text, created[0], `${key} acknowledged as ${created[0]}, holds ${text}`);
      } else {
        assert.notEqual(text, undefined, `${key} answered exists, then missing`);
      }
    }
  } finally {
    await check.close();
  }
  assert.ok(acknowledged > 100, `only ${acknowledged} creates acknowledged`);
  console.log(
    `${acknowledged} acknowledged, ${failures} store failures, ` +
      `a replica promoted ${(promotedIn / 1000).toFixed(1)} s after the kill`,
  );
}

test("a sentinel failover keeps every acknowledged create", {
  skip: noRedis("redis-sentinel"),
}, async (t) => {
  const url = redisUrl("redis-sentinel");
  const sentinels = (url.split("://")[1] as string).split("/")[0]?.split(",") as string[];
  const master = new URLSearchParams(url.slice(url.indexOf("?") + 1)).get("master") as string;
  await streamThroughFailover(t, url, "substantial-sentinel", async (old) => {
    const answer = (await ask(sentinels[0] as string, [
      "SENTINEL",
      "GET-MASTER-ADDR-BY-NAME",
      master,
    ])) as [string, string];
    return `${answer[0]}:${Number(answer[1])}` !== old;
  });
});

test("a cluster failover keeps every acknowledged create", {
  skip: noRedis("redis-cluster"),
}, async (t) => {
  const url = redisUrl("redis-cluster");
  const replicas = new Map<string, string>();
  const replicaOf = async (primary: string): Promise<string> => {
    let replica = replicas.get(primary);
    if (replica === undefined) {
      const first = (await replication(primary)).get("slave0") as string;
      const fields = new Map(first.split(",").map((f) => f.split("=", 2) as [string, string]));
      replica = `${fields.get("ip")}:${fields.get("port")}`;
      replicas.set(primary, replica);
    }
    return replica;
  };
  const store = (await openStore(url, undefined)) as RedisStore;
  await store.open();
  await replicaOf((store.current as { where: string }).where);
  await store.close();
  await streamThroughFailover(t, url, "substantial-cluster", async (old) =>
    (await role(await replicaOf(old))).startsWith("master"),
  );
});
