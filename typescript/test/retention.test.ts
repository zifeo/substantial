// Retention on the test environment: ended runs removed by their expiry, their ids taken while
// they are kept, and the order of one client's messages. The ports of `rust/tests/retention.rs`,
// under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import { createOn } from "../src/client.ts";
import * as s from "../src/mod.ts";
import { MemoryStore } from "../src/stores/memory.ts";
import { type Env, worldOf } from "../src/testing/env.ts";
import { eventually, LONG, script as scripted } from "./helpers.ts";
import { withEnv } from "./testing_helpers.ts";

const KEPT_DAYS = 90;

const quick = s.workflow("quick", async () => 1);
const kept = s.workflow("kept", async () => 2, { retention: KEPT_DAYS });

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
    if (e instanceof s.RunNotFound) return `RunNotFound ${e.message}`;
    throw e;
  }
  throw new Error("sent");
});

const script = { preset: "script" } as const;

function keys(env: Env, prefix: string): string[] {
  return worldOf(env).keys(prefix);
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
  "a run is removed 30 days after its end",
  withEnv(async (env) => {
    await using _ = env.worker([quick], script);
    const run = await env.start(quick, {}, { id: "q" });
    assert.equal(await run.result(), 1);
    await env.advance(s.milliseconds(s.days(30).ms - s.minutes(1).ms));
    assert.equal(await run.status(), "completed");
    await env.advance(s.minutes(2));
    assert.equal((await notFound(run.status())).message, "no chain q");
    await notFound(run.result());
    assert.equal(await run.read("any"), undefined);
    assert.deepEqual(keys(env, "runs/"), []);
  }),
);

test(
  "a start while the run is kept raises run exists",
  withEnv(async (env) => {
    await using _ = env.worker([quick], script);
    const run = await env.start(quick, {}, { id: "q" });
    assert.equal(await run.result(), 1);
    await env.advance(s.milliseconds(s.days(30).ms - s.minutes(1).ms));
    await assert.rejects(env.start(quick, { other: true } as never, { id: "q" }), (e) => {
      assert.ok(e instanceof s.RunExists);
      assert.equal(e.message, "id q is taken by another workflow, version or input");
      return true;
    });
  }),
);

test(
  "the id starts a new run once its retention passed",
  withEnv(async (env) => {
    await using _ = env.worker([quick], script);
    const run = await env.start(quick, {}, { id: "q" });
    assert.equal(await run.result(), 1);
    await env.advance(s.milliseconds(s.days(30).ms + s.minutes(1).ms));
    await notFound(run.status());
    const again = await env.start(quick, { other: true } as never, { id: "q" });
    assert.equal(await again.result(), 1);
  }),
);

test(
  "a workflow retention overrides the workers",
  withEnv(async (env) => {
    await using _ = env.worker([quick, kept], { ...script, retention: 2 });
    const q = await env.start(quick, {}, { id: "q" });
    const k = await env.start(kept, {}, { id: "k" });
    assert.equal(await q.result(), 1);
    assert.equal(await k.result(), 2);
    await env.advance(s.days(3));
    await notFound(q.status());
    assert.equal(await k.status(), "completed");
    await env.advance(s.days(88));
    await notFound(k.status());
  }),
);

test(
  "a long lived chain keeps a bounded number of runs",
  withEnv(async (env) => {
    await using _ = env.worker([counter], { ...script, retention: 1 });
    const run = await env.start(counter, { left: 72, done: 0 }, { id: "c" });
    let most = 0;
    for (let i = 0; i < 72; i++) {
      await env.advance(s.hours(1));
      most = Math.max(most, keys(env, "runs/").length + keys(env, "archives/").length);
    }
    assert.equal(await run.result(), 72);
    // A day of hourly runs, each kept by its archive and its id, and the chain's head.
    assert.ok(most <= 2 * 25 + 1, `${most} objects kept at once`);
    await env.advance(s.milliseconds(s.days(1).ms + s.minutes(1).ms));
    assert.deepEqual(keys(env, "runs/"), []);
    assert.deepEqual(keys(env, "archives/"), []);
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

/** Sends `m` to `to` under the ids `e` to `a`, in that order, the first three at once. */
const ordered = s.workflow("ordered", async (c, input: { to: string }) => {
  const ids = ["e", "d", "c", "b", "a"];
  await Promise.all(ids.slice(0, 3).map((id) => c.send(input.to, "m", id, { id })));
  for (const id of ids.slice(3)) await c.send(input.to, "m", id, { id });
});

test(
  "messages of one run in one millisecond are consumed in call order",
  withEnv(async (env) => {
    await using _sending = env.worker([ordered], script);
    const run = await env.start(inbox, { n: 5 }, { id: "i" });
    const sending = await env.start(ordered, { to: "i" });
    await sending.result();
    // Served once every message waits: its first replay consumes them in key order.
    await using _receiving = env.worker([inbox], script);
    assert.deepEqual(await run.result(), ["e", "d", "c", "b", "a"]);
  }),
);

test(
  "a result waiter ends with run not found when its run is removed",
  withEnv(async (env) => {
    await using _ = env.worker([quick], script);
    const run = await env.start(quick, {}, { id: "q" });
    await env.settle();
    await env.advance(s.days(31));
    // § one hnjtggsx7.a@5xa45
    await notFound(run.result());
  }),
);

/** Starts a child kept 90 days and returns its value. */
const adopting = s.workflow("adopting", async (c) => {
  const child = await c.start(kept, {}, { id: "child" });
  return await child.result();
});

test(
  "a parent is removed after its own retention whatever its child",
  withEnv(async (env) => {
    await using _ = env.worker([adopting, kept], script);
    const run = await env.start(adopting, {}, { id: "p" });
    assert.equal(await run.result(), 2);
    const child = env.run(kept, "child");
    // § a1ys7at7p@yw0bg
    // The parent's own retention is 30 days, its child's 90.
    await env.advance(s.milliseconds(s.days(30).ms + s.minutes(1).ms));
    await notFound(run.status());
    assert.equal(await child.status(), "completed");
    await env.advance(s.days(60));
    await notFound(child.status());
    // § end a1ys7at7p
  }),
);

/** Waits for `go`. */
const lingering = s.workflow("lingering", async (c) => await c.receive<number>("go"));

/** Starts `lingering` as `kid`, abandoned, then continues once and ends. */
const continuing = s.workflow("continuing", async (c, input: { again?: boolean }) => {
  if (input.again === true) return 1;
  await c.start(lingering, undefined, { id: "kid", abandon: true });
  return await c.continueAsNew({ again: true });
});

test(
  "a continued run expires while its abandoned child lives",
  withEnv(async (env) => {
    // A day advanced stops at every idle scan: a long `maxIdle` keeps them few.
    await using _ = env.worker([continuing, lingering], {
      ...script,
      retention: 1,
      maxIdle: s.hours(1),
    });
    const run = await env.start(continuing, {}, { id: "p" });
    assert.equal(await run.result(), 1);
    // § a1ys7at7p.c@x7cjd
    await env.advance(s.milliseconds(s.days(1).ms + s.minutes(1).ms));
    assert.deepEqual(keys(env, "archives/"), [], "the continued run");
    await notFound(run.status());
    const kid = env.run(lingering, "kid");
    assert.equal(await kid.status(), "suspended");
    // § end a1ys7at7p.c
    // § n0ge6zsws.a@8ab8g
    // Its end tells a parent its retention removed: the notice is deleted unconsumed.
    await kid.send("go", 2);
    assert.equal(await kid.result(), 2);
    await env.settle();
    assert.deepEqual(keys(env, "queues/"), [], JSON.stringify(keys(env, "")));
    await notFound(run.status());
    // § end n0ge6zsws.a
  }),
);

/** Starts `lingering` as `kid`, abandoned, and ends. */
const leaving = s.workflow("leaving", async (c) => {
  await c.start(lingering, undefined, { id: "kid", abandon: true });
  return 0;
});

test(
  "a child's end reaching a run that took its parent's id is ignored",
  withEnv(async (env) => {
    // A day advanced stops at every idle scan: a long `maxIdle` keeps them few.
    await using _ = env.worker([leaving, lingering], {
      ...script,
      retention: 1,
      maxIdle: s.hours(1),
    });
    const parent = await env.start(leaving, {}, { id: "p" });
    assert.equal(await parent.result(), 0);
    await env.advance(s.milliseconds(s.days(1).ms + s.minutes(1).ms));
    await notFound(parent.status());
    // § n0ge6zsws@agxeb
    const stranger = await env.start(lingering, undefined, { id: "p" });
    await env.settle();
    const kid = env.run(lingering, "kid");
    await kid.send("go", 2);
    assert.equal(await kid.result(), 2);
    await env.settle();
    assert.equal(await stranger.status(), "suspended");
    const history = await stranger.history();
    assert.ok(
      history.every((e) => e.data.name !== "substantial.ended"),
      JSON.stringify(history),
    );
    await stranger.send("go", 3);
    assert.equal(await stranger.result(), 3);
    // § end n0ge6zsws
  }),
);

/** Sleeps 2 days, past its parent's retention, then ends with 4. */
const late = s.workflow("late", async (c) => {
  await c.sleep(s.days(2));
  return 4;
});

/** Starts `late` as `k4`, abandoned, and ends at once, or, `awaiting`, also returns its result: the
 * same workflow deployed again with the same child start. */
function returning(awaiting: boolean) {
  return s.workflow(
    "returning",
    async (c) => {
      const child = await c.start(late, undefined, { id: "k4", abandon: true });
      if (awaiting) return await child.result();
      return 0;
    },
    { retention: 1 },
  );
}

test(
  "a run that took its parent's id adopting its ended child gets its end",
  withEnv(async (env) => {
    const options = { ...script, maxIdle: s.hours(1) };
    const first = env.worker([returning(false), late], options);
    const parent = await env.start(returning(false), undefined, { id: "p4" });
    assert.equal(await parent.result(), 0);
    await env.advance(s.milliseconds(s.days(2).ms + s.minutes(1).ms));
    await notFound(parent.status());
    assert.equal(await env.run(late, "k4").result(), 4);
    await first.shutdown();
    await using _ = env.worker([returning(true), late], options);
    // § svx76xx41@6p07z
    const again = await env.start(returning(true), undefined, { id: "p4" });
    assert.equal(await again.result(), 4);
    // § end svx76xx41
  }),
);

/** A workflow no worker of these tests serves. */
const unserved = s.workflow("unserved", async () => 9);

/** Starts `unserved` as `kid`, then waits for `go` and ends, which cancels its child. */
const owing = s.workflow("owing", async (c) => {
  await c.start(unserved, undefined, { id: "kid" });
  return await c.receive<number>("go");
});

/** A parent whose child never ran, served by no worker; then the child's head and due entry go,
 * as its retention's expiry removes a chain. */
async function childGone(name: string) {
  const store = new MemoryStore(name);
  const sub = await createOn(store);
  const worker = scripted(sub, [owing, lingering]);
  await sub.start(owing, undefined, { id: "p" });
  const run = sub.run(owing, "p");
  await eventually("the wait", async () => (await run.status()) === "suspended");
  await store.delete("runs/kid");
  for (const key of (await store.list("queues/unserved/", { maxKeys: 100 })).keys) {
    await store.delete(key);
  }
  return { store, sub, worker, run };
}

test("a cancel reaching a child past its retention is dropped", async () => {
  const { store, worker, run } = await childGone("retention-cancel-gone");
  // § n0ge6zsws.b@krgd5
  await run.send("go", 1);
  assert.equal(await run.result({ timeout: LONG }), 1);
  await worker.shutdown();
  const left = (await store.list("", { maxKeys: 100 })).keys;
  assert.deepEqual(left, ["runs/p", "substantial.json"]);
  // § end n0ge6zsws.b
});

test("a cancel never reaches a run that took its child's id", async () => {
  const { sub, worker, run } = await childGone("retention-cancel-retaken");
  // § n0ge6zsws.b@jh3pe
  const stranger = await sub.start(lingering, undefined, { id: "kid" });
  await run.send("go", 1);
  assert.equal(await run.result({ timeout: LONG }), 1);
  await stranger.send("go", 3);
  assert.equal(await stranger.result({ timeout: LONG }), 3);
  // § end n0ge6zsws.b
  await worker.shutdown();
});

test(
  "a retention change reaches only the runs that end after it",
  withEnv(async (env) => {
    const short = env.worker([quick], { ...script, retention: 2 });
    const before = await env.start(quick, {}, { id: "before" });
    assert.equal(await before.result(), 1);
    await short.shutdown();
    await using _ = env.worker([quick], script);
    const after = await env.start(quick, {}, { id: "after" });
    assert.equal(await after.result(), 1);
    // § a1ys7at7p.b@tmpvz
    await env.advance(s.days(3));
    await notFound(before.status());
    assert.equal(await after.status(), "completed");
    // § end a1ys7at7p.b
  }),
);

const failing = s.workflow("failing", async () => {
  throw new s.Fail("Refused", "not today");
});

test(
  "a retry of a removed chain raises run not found",
  withEnv(async (env) => {
    await using _ = env.worker([failing], script);
    const run = await env.start(failing, {}, { id: "f" });
    await assert.rejects(run.result(), s.RunFailed);
    await env.advance(s.days(31));
    assert.equal((await notFound(run.retry())).message, "no chain f");
  }),
);

/** Version 1 counts the runs of its step, then fails while `refuse.on`; version 2 ends. */
function retried(version: number, counted: { steps: number }, refuse: { on: boolean }) {
  return s.workflow(
    "retried",
    async (c) => {
      if (version === 2) return 0;
      const n = await c.step("count", () => {
        counted.steps += 1;
        return counted.steps;
      });
      if (refuse.on) throw new s.Fail("Refused", "not today");
      return n;
    },
    { version, retention: 1 },
  );
}

test(
  "a failed run is kept until its retry holds its history",
  withEnv(async (env) => {
    const counted = { steps: 0 };
    const refuse = { on: true };
    // The retry's entry, due while no worker takes it, keeps the environment from skipping
    // scans: the service preset and a long `maxIdle` keep them few.
    const worker = (version: number) =>
      env.worker([retried(version, counted, refuse)], {
        preset: "service",
        maxIdle: s.hours(1),
      });
    const first = worker(1);
    const run = await env.start(retried(1, counted, refuse), {}, { id: "f" });
    await assert.rejects(run.result(), s.RunFailed);
    await first.shutdown();
    refuse.on = false;
    await run.retry();
    // Serves the name, but not the retry's version, which waits.
    await using _other = worker(2);
    // The failed run's retention passed: its archive expired, its history is in the retry's head.
    await env.advance(s.days(2));
    assert.deepEqual(keys(env, "archives/"), []);
    await using _replaying = worker(1);
    assert.equal(await run.result(), 1, "the recorded step, from the retry's head");
    assert.equal(counted.steps, 1);
  }),
);

test(
  "a retention under a day is a config error",
  withEnv(async (env) => {
    for (const retention of [0, 0.5, s.hours(23)]) {
      assert.throws(
        () => env.worker([quick], { ...script, retention: retention as number }),
        s.ConfigError,
        String(retention),
      );
    }
    const short = s.workflow("short", async () => 1, { retention: 0 });
    assert.throws(() => env.worker([short], script), s.ConfigError);
  }),
);

test(
  "a retention over 100 years is a config error",
  withEnv(async (env) => {
    for (const retention of [36_501, 2 ** 32]) {
      assert.throws(
        () => env.worker([quick], { ...script, retention }),
        (e) => e instanceof s.ConfigError && /whole days from 1 to 36500, /.test(e.message),
        String(retention),
      );
      const long = s.workflow("long", async () => 1, { retention });
      assert.throws(
        () => env.worker([long], script),
        (e) =>
          e instanceof s.ConfigError &&
          e.message ===
            `the retention of workflow "long" must be whole days from 1 to 36500, not ${retention}`,
        String(retention),
      );
    }
    const longest = s.workflow("longest", async () => 1, { retention: 36_500 });
    await env.worker([longest], { ...script, retention: 36_500 }).shutdown();
  }),
);

test(
  "a send to a removed chain raises run not found",
  withEnv(async (env) => {
    await using _ = env.worker([quick, sender], script);
    const run = await env.start(quick, {}, { id: "q" });
    assert.equal(await run.result(), 1);
    await env.advance(s.days(31));
    assert.equal((await notFound(run.send("m", 1))).message, "no chain q");
    await notFound(run.cancel("late"));
    const fromWorkflow = await env.start(sender, { to: "q" });
    assert.equal(await fromWorkflow.result(), "RunNotFound no chain q");
  }),
);

test("a result wait sees its chain removed", async () => {
  const store = new MemoryStore("retention-removed");
  const sub = await createOn(store);
  const run = await sub.start(quick, {}, { id: "q" });
  const wait = run.result({ timeout: s.seconds(10) });
  await new Promise((resolve) => setTimeout(resolve, 20));
  await store.delete("runs/q");
  // § one hnjtggsx7.a@vr3c6
  await notFound(wait);
});
