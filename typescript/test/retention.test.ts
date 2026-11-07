// Retention on the test environment: ended runs collected, their ids taken for 7 days, and the
// order of one client's messages, ported from the Python tests under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import { createOn } from "../src/client.ts";
import * as s from "../src/mod.ts";
import { refollow } from "../src/run.ts";
import { MemoryStore } from "../src/stores/memory.ts";
import { type Env, worldOf } from "../src/testing/env.ts";
import { withEnv } from "./testing_helpers.ts";

const quick = s.workflow("quick", async () => 1);
const kept = s.workflow("kept", async () => 2, { retention: s.days(90) });
const forever = s.workflow("forever", async () => 3, { retention: s.FOREVER });

/** Continues as new every hour, `left` times, then ends with the count. */
const counter = s.workflow("counter", async (c, input: { left: number; done: number }) => {
  if (input.left === 0) return input.done;
  await c.sleep(s.hours(1));
  return await c.continueAsNew({ left: input.left - 1, done: input.done + 1 });
});

/** Collects the values of its messages until it has `n`. */
const inbox = s.workflow("inbox", async (c, input: { n: number }) => {
  const names: string[] = [];
  c.handle<string>("m", (name) => {
    names.push(name);
  });
  await c.waitUntil(() => names.length >= input.n);
  return names;
});

/** Sends to `to` and returns the error the send raised. */
const sender = s.workflow("sender", async (c, input: { to: string }) => {
  try {
    await c.send(input.to, "m", 1);
  } catch (e) {
    if (e instanceof s.RunNotFound) return `RunNotFound ${e.collected} ${e.until?.toISOString()}`;
    throw e;
  }
  throw new Error("sent");
});

const script = { preset: "script" } as const;

function keys(env: Env, prefix: string): string[] {
  return worldOf(env).keys(prefix);
}

function runs(env: Env): Set<string> {
  return new Set(keys(env, "runs/").map((k) => k.split("/")[1] ?? k));
}

async function notFound(promise: Promise<unknown>): Promise<s.RunNotFound> {
  try {
    await promise;
  } catch (e) {
    if (e instanceof s.RunNotFound) return e;
    throw e;
  }
  assert.fail("no RunNotFound");
}

test(
  "a run is collected 30 days after its end",
  withEnv(async (env) => {
    await using _ = env.worker([quick], script);
    const run = await env.start(quick, {}, { id: "q" });
    assert.equal(await run.result(), 1);
    await env.advance(s.milliseconds(s.days(30).ms - s.minutes(1).ms));
    assert.equal(await run.status(), "completed");
    await env.advance(s.minutes(2));
    const e = await notFound(run.status());
    assert.ok(e.collected);
    const until = e.until?.getTime() ?? 0;
    const week = env.now.getTime() + s.days(7).ms;
    assert.ok(week - s.minutes(2).ms < until && until <= week);
    assert.ok((await notFound(run.result())).collected);
    assert.equal(await run.read("any"), undefined);
    assert.deepEqual(keys(env, "runs/q/"), ["runs/q/gone"]);
  }),
);

test(
  "a start during the tombstone raises run exists",
  withEnv(async (env) => {
    await using _ = env.worker([quick], script);
    const run = await env.start(quick, {}, { id: "q" });
    assert.equal(await run.result(), 1);
    await env.advance(s.days(31));
    await assert.rejects(env.start(quick, {}, { id: "q" }), (e) => {
      assert.ok(e instanceof s.RunExists);
      assert.ok(e.collected);
      assert.ok((e.until?.getTime() ?? 0) > env.now.getTime() + s.days(5).ms);
      assert.match(e.message, /collected/);
      return true;
    });
  }),
);

test(
  "the id starts a new run after the tombstone",
  withEnv(async (env) => {
    await using _ = env.worker([quick], script);
    const run = await env.start(quick, {}, { id: "q" });
    assert.equal(await run.result(), 1);
    await env.advance(s.milliseconds(s.days(37).ms + s.minutes(2).ms));
    const e = await notFound(run.status());
    assert.equal(e.collected, false);
    assert.equal(e.until, undefined);
    assert.deepEqual(keys(env, "runs/q/"), []);
    const again = await env.start(quick, {}, { id: "q" });
    assert.equal(await again.result(), 1);
  }),
);

test(
  "a workflow retention overrides the workers",
  withEnv(async (env) => {
    await using _ = env.worker([quick, kept], { ...script, retention: s.days(2) });
    const q = await env.start(quick, {}, { id: "q" });
    const k = await env.start(kept, {}, { id: "k" });
    assert.equal(await q.result(), 1);
    assert.equal(await k.result(), 2);
    await env.advance(s.days(3));
    assert.ok((await notFound(q.status())).collected);
    assert.equal(await k.status(), "completed");
    await env.advance(s.days(88));
    assert.ok((await notFound(k.status())).collected);
  }),
);

test(
  "forever keeps a run",
  withEnv(async (env) => {
    await using _ = env.worker([quick, forever], { ...script, retention: s.FOREVER });
    const q = await env.start(quick, {}, { id: "q" });
    const f = await env.start(forever, {}, { id: "f" });
    assert.equal(await q.result(), 1);
    assert.equal(await f.result(), 3);
    await env.advance(s.days(400));
    assert.equal(await q.status(), "completed");
    assert.equal(await f.result(), 3);
  }),
);

test(
  "a long lived chain keeps a bounded number of runs",
  withEnv(async (env) => {
    await using _ = env.worker([counter], { ...script, retention: s.hours(1) });
    const run = await env.start(counter, { left: 48, done: 0 }, { id: "c" });
    let most = 0;
    for (let i = 0; i < 48; i++) {
      await env.advance(s.hours(1));
      most = Math.max(most, runs(env).size);
    }
    assert.equal(await run.result(), 48);
    assert.ok(most <= 4, `${most} runs kept at once`);
    await env.advance(s.hours(3));
    assert.deepEqual(keys(env, "runs/"), ["runs/c/gone"]);
    assert.deepEqual(keys(env, "index/"), []);
  }),
);

test(
  "messages of one client in one millisecond are consumed in send order",
  withEnv(async (env) => {
    await using _ = env.worker([inbox], script);
    const run = await env.start(inbox, { n: 4 }, { id: "i" });
    await env.settle();
    for (const id of ["d", "b", "c", "a"]) await run.send("m", id, { id });
    assert.deepEqual(await run.result(), ["d", "b", "c", "a"]);
  }),
);

test(
  "a result waiter ends with run not found when its run is collected",
  withEnv(async (env) => {
    await using _ = env.worker([quick], script);
    const run = await env.start(quick, {}, { id: "q" });
    await env.settle();
    await env.advance(s.days(31));
    assert.ok((await notFound(run.result())).collected);
  }),
);

test(
  "a retention under an hour is a config error",
  withEnv(async (env) => {
    assert.throws(
      () => env.worker([quick], { ...script, retention: s.minutes(59) }),
      s.ConfigError,
    );
    const short = s.workflow("short", async () => 1, { retention: s.minutes(59) });
    assert.throws(() => env.worker([short], script), s.ConfigError);
  }),
);

test(
  "a retention over 100 years is a config error",
  withEnv(async (env) => {
    assert.throws(
      () => env.worker([quick], { ...script, retention: s.days(36_501) }),
      (e) => e instanceof s.ConfigError && /at most 3153600000000 ms or forever/.test(e.message),
    );
    const long = s.workflow("long", async () => 1, { retention: s.days(36_501) });
    assert.throws(() => env.worker([long], script), s.ConfigError);
    const longest = s.workflow("longest", async () => 1, { retention: s.days(36_500) });
    await env.worker([longest], { ...script, retention: s.days(36_500) }).shutdown();
  }),
);

test(
  "a send to a collected chain raises run not found",
  withEnv(async (env) => {
    await using _ = env.worker([quick, sender], script);
    const run = await env.start(quick, {}, { id: "q" });
    assert.equal(await run.result(), 1);
    await env.advance(s.days(31));
    const sent = await notFound(run.send("m", 1));
    assert.ok(sent.collected);
    assert.ok((await notFound(run.cancel("late"))).collected);
    const fromWorkflow = await env.start(sender, { to: "q" });
    assert.equal(await fromWorkflow.result(), `RunNotFound true ${sent.until?.toISOString()}`);
  }),
);

test("a result wait follows its chain again and sees it collected", async () => {
  const store = new MemoryStore("retention-refollow");
  const sub = await createOn(store, { partitions: 1 });
  const run = await sub.start(quick, {}, { id: "q" });
  refollow(run, 50);
  const wait = run.result({ timeout: s.seconds(10) });
  await new Promise((resolve) => setTimeout(resolve, 20));
  for (const key of (await store.list("runs/q/", { maxKeys: 1000 })).keys) await store.delete(key);
  const e = await notFound(wait);
  assert.equal(e.collected, false);
});
