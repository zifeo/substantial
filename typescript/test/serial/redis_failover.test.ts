// Failovers during a stream of creates: the primary holding the namespace is killed, a replica
// takes over, creates are acknowledged again, and every create acknowledged before or during it is
// there after, none acknowledged twice, on the containers of `dev/compose.yaml`, which a test fails
// without; each test starts the killed container again and waits for it to rejoin as a replica. Run
// apart from the other tests, which would meet the failover.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { test } from "node:test";
import { promisify } from "node:util";
import { createClient } from "@redis/client";
import { openStore } from "../../src/stores/open.ts";
import type { RedisStore } from "../../src/stores/redis_store.ts";
import { StoreFailure } from "../../src/stores/store.ts";
import { failoverLock, holdsFailovers, kind, redisUrl, UP } from "../helpers.ts";

holdsFailovers();

const run = promisify(execFile);
const WRITERS_PER_KEY = 2;
const KEYS_AT_ONCE = 4;
// Creates acknowledged before the kill, and again among those sent after the promotion: counts,
// not durations, so that a loaded machine makes the test slower, never failing.
const ACKS = 50;
const WITHIN_MS = 60_000;
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** The container `name`, which the test fails without. */
async function container(name: string): Promise<string> {
  try {
    await run("docker", ["inspect", name]);
    return name;
  } catch (e) {
    const missing = (e as { code?: unknown }).code === "ENOENT";
    throw new Error(missing ? "docker is not on the PATH" : `no container ${name}: run \`${UP}\``);
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
 * until `ACKS` creates sent after the promotion are acknowledged, then checks each answer against
 * the store once the old primary is back. `rejoinFirst` starts the killed container right after
 * the promotion, for a primary with one replica, which acknowledges no create alone (I-26). */
async function streamThroughFailover(
  url: string,
  prefix: string,
  failedOver: (old: string) => Promise<boolean>,
  rejoinFirst: boolean,
): Promise<void> {
  const store = (await openStore(url, undefined)) as RedisStore;
  await store.open();
  const where = (store.current as { where: string }).where;
  const killed = await container(`${prefix}-${where.slice(where.lastIndexOf(":") + 1)}`).catch(
    async (e) => {
      await store.close();
      throw e;
    },
  );
  const answers = new Map<string, ["created" | "exists", string][]>();
  let failures = 0;
  let acked = 0;
  let ackedAfter = 0;
  let promoted = false;
  let stop = false;

  const writer = async (slot: number, writerId: number) => {
    for (let n = 0; !stop; n += 1) {
      const key = `k/${slot}-${String(n).padStart(6, "0")}`;
      const data = `${key} by ${writerId}`;
      const after = promoted;
      try {
        const outcome = kind(await store.create(key, new TextEncoder().encode(data), undefined));
        if (outcome === "created") {
          acked += 1;
          if (after) ackedAfter += 1;
        }
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
  let resumedIn: number;
  try {
    await until("creates acknowledged before the kill", async () => acked >= ACKS, WITHIN_MS);
    await run("docker", ["kill", killed]);
    const killedAt = performance.now();
    await until("a replica promoted", () => failedOver(where), WITHIN_MS);
    promotedIn = performance.now() - killedAt;
    promoted = true;
    if (rejoinFirst) await run("docker", ["start", killed]);
    await until(
      "creates acknowledged after the promotion",
      async () => ackedAfter >= ACKS,
      WITHIN_MS,
    );
    resumedIn = performance.now() - killedAt - promotedIn;
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
      const stored = (await check.get(key))?.data;
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
  console.log(
    `${acknowledged} acknowledged, ${failures} store failures, ` +
      `a replica promoted ${(promotedIn / 1000).toFixed(1)} s after the kill, ` +
      `${ACKS} creates sent after it acknowledged ${(resumedIn / 1000).toFixed(1)} s later`,
  );
}

/** Runs `fn` with the failover lock held alone. */
async function alone(fn: () => Promise<void>): Promise<void> {
  const release = await failoverLock(true);
  try {
    await fn();
  } finally {
    await release();
  }
}

test("a sentinel failover keeps every acknowledged create", () =>
  alone(async () => {
    const url = await redisUrl("redis-sentinel");
    const sentinels = (url.split("://")[1] as string).split("/")[0]?.split(",") as string[];
    const master = new URLSearchParams(url.slice(url.indexOf("?") + 1)).get("master") as string;
    await streamThroughFailover(
      url,
      "substantial-sentinel",
      async (old) => {
        const answer = (await ask(sentinels[0] as string, [
          "SENTINEL",
          "GET-MASTER-ADDR-BY-NAME",
          master,
        ])) as [string, string];
        return `${answer[0]}:${Number(answer[1])}` !== old;
      },
      false,
    );
  }));

test("a cluster failover keeps every acknowledged create", () =>
  alone(async () => {
    const url = await redisUrl("redis-cluster");
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
    await streamThroughFailover(
      url,
      "substantial-cluster",
      async (old) => (await role(await replicaOf(old))).startsWith("master"),
      true,
    );
  }));
