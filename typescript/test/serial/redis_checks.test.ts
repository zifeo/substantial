// The checks at open of the Redis adapter, against a server whose configuration each test
// changes for a moment: run apart from the other tests, which would meet it, on the standalone
// server `SUBSTANTIAL_REDIS_URL` names (see `dev/compose.yaml`).
import assert from "node:assert/strict";
import { test } from "node:test";
import { createClient } from "@redis/client";
import { ConfigError, Substantial } from "../../src/mod.ts";
import { parseRedisUrl } from "../../src/stores/redis.ts";
import { failoverLock, holdsFailovers, redisUrl } from "../helpers.ts";

holdsFailovers();

/** Runs `fn` with the server's `name` set to `value`, then sets it back, the failover lock held
 * alone. */
async function withConfig(name: string, value: string, fn: () => Promise<void>): Promise<void> {
  const release = await failoverLock(true);
  try {
    await configured(name, value, fn);
  } finally {
    await release();
  }
}

async function configured(name: string, value: string, fn: () => Promise<void>): Promise<void> {
  const [[host, port]] = parseRedisUrl(await redisUrl("redis")).nodes as [[string, number]];
  const admin = createClient({ socket: { host, port } });
  await admin.connect();
  const before = (await admin.configGet(name))[name] as string;
  await admin.configSet(name, value);
  try {
    await fn();
  } finally {
    await admin.configSet(name, before);
    admin.destroy();
  }
}

test("a server without aof is refused", () =>
  withConfig("appendonly", "no", async () => {
    await assert.rejects(
      Substantial.create(await redisUrl("redis")),
      (e) => e instanceof ConfigError && /no AOF/.test(e.message),
    );
  }));

test("a server that evicts is refused", () =>
  withConfig("maxmemory-policy", "allkeys-lru", async () => {
    await assert.rejects(
      Substantial.create(await redisUrl("redis")),
      (e) => e instanceof ConfigError && /allkeys-lru/.test(e.message),
    );
  }));
