// The test environment of docs/testing.md: failures and faults, what never hangs, what is cheap.
import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";
import * as s from "../src/mod.ts";
import { worldOf } from "../src/testing/env.ts";
import { limits } from "../src/testing/time.ts";
import type { WorkerClock } from "../src/testing/workers.ts";
import { T0 } from "./testing_helpers.ts";

const pushes: [Date, number[]][] = [];
const defaults = { ...limits };

beforeEach(() => {
  pushes.length = 0;
});

afterEach(() => {
  Object.assign(limits, defaults);
});

const service = { preset: "service" } as const;

function at(ms: number): Date {
  return new Date(T0.getTime() + ms);
}

function stuck(pattern: RegExp): (e: unknown) => boolean {
  return (e) => e instanceof s.testing.Stuck && pattern.test(e.message);
}

const debounce = s.workflow("debounce", async (c, _input: { key: string }) => {
  const events: number[] = [];
  c.handle<number>("event", (e) => {
    events.push(e);
  });
  let seen = 0;
  for (;;) {
    const n = seen;
    if (!(await c.waitUntil(() => events.length > n, { timeout: s.minutes(2) }))) break;
    seen = events.length;
  }
  const now = (await c.now()).toISOString();
  await c.step("push", () => {
    pushes.push([new Date(now), [...events]]);
  });
  return events;
});

const failing = s.workflow("failing", async (c) => {
  await c.step("fail", () => {
    throw new s.Permanent("unknown invoice");
  });
});

const failingParent = s.workflow("failing_parent", async (c) => {
  await c.start(failing, {}, { id: "its-child" });
});

const napping = s.workflow("napping", async (c) => {
  await c.sleep(s.days(30));
});

const echo = s.workflow("echo", async (c) => {
  const pings: number[] = [];
  c.handle<number>("ping", (p) => {
    pings.push(p);
  });
  for (;;) {
    const seen = pings.length;
    await c.waitUntil(() => pings.length > seen);
    await c.send(c.chain, "ping", seen);
  }
});

test("an outage inside the debounce window", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  await using _worker = env.worker([debounce], service);
  const run = await env.start(debounce, { key: "c1" }, { id: "contact-c1" });
  await run.send("event", 1, { id: "crm-1" });
  await env.advance(s.minutes(1));
  env.outage(s.minutes(10));
  await assert.rejects(run.send("event", 2, { id: "crm-2" }), s.StoreError);
  assert.deepEqual(env.now, at(s.minutes(1).ms));
  await env.advance(s.minutes(12));
  assert.deepEqual(await run.result(), [1]);
  // The timer came due in the outage: the worker retried, and pushed once the store was back.
  assert.equal(pushes.length, 1);
  const [[when, events]] = pushes as [[Date, number[]]];
  assert.ok(when >= at(s.minutes(11).ms) && when <= at(s.minutes(12).ms), String(when));
  assert.deepEqual(events, [1]);
  await env.check();
});

test("a thousand events in one instant", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  await using _worker = env.worker([debounce], service);
  const run = await env.start(debounce, { key: "c1" });
  const all = Array.from({ length: 1000 }, (_, i) => i);
  for (const i of all) await run.send("event", i, { id: `crm-${String(i).padStart(4, "0")}` });
  await env.advance(s.minutes(2));
  assert.deepEqual(await run.result(), all);
  assert.deepEqual(pushes, [[at(s.minutes(2).ms), all]]);
});

test("a run failing in the background fails the test", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  await using _worker = env.worker([failing, failingParent], service);
  await using app = await env.client();
  const read = await env.start(failing, {}, { id: "read" });
  const looked = await env.start(failing, {}, { id: "looked" });
  await app.start(failing, {}, { id: "started-by-the-app" });
  await env.start(failingParent, {}, { id: "parent" });
  const cancelled = await env.start(napping, {}, { id: "cancelled" });
  await cancelled.cancel("not needed");
  await env.advance(s.minutes(1));
  await assert.rejects(read.result(), s.RunFailed);
  assert.equal(await looked.status(), "failed");
  assert.equal(await app.run(failing, "started-by-the-app").status(), "failed");
  const unchecked = await env.check().catch((e: unknown) => e);
  assert.ok(unchecked instanceof s.testing.Unchecked, String(unchecked));
  const message = unchecked.message;
  assert.match(message, /unknown invoice/);
  for (const chain of ["started-by-the-app", "its-child"])
    assert.ok(message.includes(`${chain} (`));
  for (const chain of ["read", "looked", "cancelled", "parent"]) {
    assert.ok(!message.includes(` ${chain} (`), message);
  }
  await assert.rejects(env.run(failing, "started-by-the-app").result(), s.RunFailed);
  await assert.rejects(env.run(failing, "its-child").result(), s.RunFailed);
  await env.check();
});

const handlingParent = s.workflow("handling_parent", async (c) => {
  const child = await c.start(failing, {}, { id: "handled-child" });
  try {
    await child.result();
  } catch (e) {
    if (e instanceof s.RunFailed) return "shipped without it";
    throw e;
  }
  return "shipped";
});

test("a child failure its parent read is read", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  await using _worker = env.worker([failing, handlingParent], service);
  const run = await env.start(handlingParent, {}, { id: "order" });
  await env.advance(s.minutes(1));
  assert.equal(await run.result(), "shipped without it");
  await env.check();
});

const failingBrieflyKept = s.workflow(
  "failing_briefly_kept",
  async (c) => {
    await c.step("fail", () => {
      throw new s.Permanent("card declined");
    });
  },
  { retention: s.hours(1) },
);

test("a failed run collected before check is reported", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  await using _worker = env.worker([failingBrieflyKept], service);
  const run = await env.start(failingBrieflyKept, {}, { id: "nightly" });
  await env.advance(s.days(1));
  await assert.rejects(run.status(), s.RunNotFound);
  await assert.rejects(
    env.check(),
    (e) =>
      e instanceof s.testing.Unchecked && /nightly \(StepFailed: .*card declined\)/.test(e.message),
  );
});

test("a hung step is stuck and interrupted when its worker crashes", async () => {
  limits.stepMs = 50;
  const answered = Promise.withResolvers<void>();
  const ended: string[] = [];
  const hanging = s.workflow("hanging", async (c) => {
    await c.step("charge", async () => {
      const signal = s.stepSignal();
      await new Promise<void>((resolve, reject) => {
        answered.promise.then(resolve);
        signal.addEventListener("abort", () => reject(signal.reason), { once: true });
      });
      ended.push("charged");
    });
  });
  {
    await using env = await s.testing.Env.open({ now: T0 });
    {
      await using worker = env.worker([hanging], service);
      await env.start(hanging, {}, { id: "hung" });
      await assert.rejects(env.settle(), stuck(/hung\/charge/));
      worker.crash();
    }
    answered.resolve();
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.deepEqual(ended, []);
});

const quote = s.workflow("quote", async (_c, _input: { sku: string }) => 42);

const engines: s.Substantial[] = [];

const priced = s.workflow("priced", async (c, { sku }: { sku: string }) => {
  return await c.step("ask_quote", async () => {
    const run = await (engines[0] as s.Substantial).start(quote, { sku }, { id: `quote-${sku}` });
    return await run.result({ timeout: s.seconds(30) });
  });
});

test("a step waiting on the environment fails at once", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  await using _worker = env.worker([priced, quote], service);
  await using engine = await env.client();
  engines.splice(0, engines.length, engine);
  const run = await env.start(priced, { sku: "a" }, { id: "p1" });
  const started = performance.now();
  await assert.rejects(
    run.result(),
    (e) =>
      e instanceof s.RunFailed &&
      /a step cannot wait for the environment to settle/.test(e.message),
  );
  assert.ok(performance.now() - started < 1000);
  await env.check();
});

test("a hung step fails the test instead of hanging it", async () => {
  limits.stepMs = 200;
  const answered = Promise.withResolvers<void>();
  const hanging = s.workflow("hanging", async (c) => {
    await c.step("charge", () => answered.promise);
  });
  const scenario = async () => {
    await using env = await s.testing.Env.open({ now: T0 });
    const worker = env.worker([hanging], service);
    await env.start(hanging, {}, { id: "hung" });
    try {
      await env.settle();
    } finally {
      worker.crash();
    }
  };
  let timer: ReturnType<typeof setTimeout> | undefined;
  const late = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error("leaving the environment hung")), 3000);
  });
  try {
    await assert.rejects(Promise.race([scenario(), late]), stuck(/hung\/charge/));
  } finally {
    clearTimeout(timer);
    answered.resolve();
  }
});

test("runs waking each other without the clock moving are stuck", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  await using worker = env.worker([echo], service);
  const run = await env.start(echo, {}, { id: "echo" });
  await run.send("ping", 0);
  await assert.rejects(
    env.advance(s.minutes(1)),
    stuck(/run echo was replayed more than 100 times/),
  );
  assert.deepEqual(env.now, T0);
  worker.crash();
});

const stamp = s.workflow("stamp", async (c) => (await c.now()).toISOString());

test("an outage moves the scans after it", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  await using _worker = env.worker([stamp], service);
  await using otherPod = await env.client();
  await env.advance(s.seconds(5));
  env.outage(s.seconds(40));
  await env.advance(s.seconds(57));
  const run = await otherPod.start(stamp, {}, { id: "late" });
  await env.advance(s.minutes(1));
  // The scan due at 30 s failed and was retried once the store was back, which moved the scans
  // after it: the run is found at 69 s, not at 90 s.
  assert.equal(await run.result(), at(s.seconds(69).ms).toISOString());
});

test("chaos faults the scans of an idle worker", async () => {
  await using env = await s.testing.Env.open({
    now: T0,
    chaos: new s.testing.Chaos({ seed: 1, rate: 0.5 }),
  });
  await using _worker = env.worker([stamp], service);
  await env.advance(s.hours(1));
  // About 120 scans, half of them failed at least once.
  assert.ok(env.faults.failed > 60, String(env.faults.failed));
});

const counter = s.workflow("counter", async (c) => {
  const events: number[] = [];
  c.handle<number>("event", (e) => {
    events.push(e);
  });
  await c.waitUntil(() => events.length >= 150);
  return events.length;
});

test("many settles at one instant are not a livelock", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  await using _worker = env.worker([counter], service);
  const run = await env.start(counter, {}, { id: "counter" });
  for (let i = 0; i < 150; i++) {
    await run.send("event", i);
    await env.settle();
  }
  assert.equal(await run.result(), 150);
});

const order = s.workflow("order", async (c, { order }: { order: string }) => {
  const stock = await c.step("reserve", async () => `stock-${order}`);
  const paid = await c.receive("paid");
  const captureId = await c.step("capture", async () => `cap-${order}`);
  const receipt = await (await c.start(receiptOf, { capture: captureId })).result();
  return { stock, paid, receipt };
});

const receiptOf = s.workflow("receipt_of", async (c, { capture }: { capture: string }) => {
  await c.sleep(s.seconds(10));
  return await c.step("mail", async () => `mailed ${capture}`);
});

async function chaotic(seed: number, rate: number, workers: number): Promise<unknown[]> {
  const chaos = new s.testing.Chaos({ seed, rate });
  await using env = await s.testing.Env.open({ now: T0, chaos });
  const runs: s.Run[] = [];
  const results: unknown[] = [];
  {
    await using stack = new AsyncDisposableStack();
    for (let i = 0; i < workers; i++) stack.use(env.worker([order, receiptOf], service));
    for (let i = 0; i < 3; i++) runs.push(await env.start(order, { order: `o-${i}` }));
    for (const run of runs) await run.send("paid", 42);
    await env.advance(s.minutes(30));
    for (const run of runs) results.push(await run.result());
  }
  await env.check();
  // A worker Chaos replaced has an id of its own.
  const ids = worldOf(env).pods.map((p) => [p.place, p.replaced, p.clock?.id]);
  assert.equal(new Set(ids.map(([, , id]) => id)).size, ids.length);
  assert.equal(
    ids.every(([, replaced]) => replaced === 0),
    env.faults.crashed === 0,
  );
  return [env.faults, runs.map((r) => r.chain), results, ids];
}

for (let seed = 0; seed < 50; seed++) {
  test(`an order survives any fault [${seed}]`, async () => {
    const [faults, , results] = (await chaotic(seed, 0.05, 1)) as [
      s.testing.Faults,
      unknown,
      unknown,
    ];
    assert.deepEqual(
      results,
      [0, 1, 2].map((i) => ({ stock: `stock-o-${i}`, paid: 42, receipt: `mailed cap-o-${i}` })),
    );
    assert.ok(faults.failed + faults.ambiguous + faults.crashed > 0);
  });
}

test("chaos counts a create that found its key as failed", async () => {
  await using env = await s.testing.Env.open({
    now: T0,
    chaos: new s.testing.Chaos({ seed: 1, rate: 1 }),
  });
  const world = worldOf(env);
  await world.memory.create("taken", new Uint8Array([1]));
  const clock = { id: "w", paused: false, dead: false, tries: new Map(), now: () => world.time.ms };
  const store = world.store(clock as unknown as WorkerClock);
  world.time.open = true;
  // Each create fails its first try, then cannot wait its backoff outside a task of a worker.
  for (let i = 0; i < 20; i++) await assert.rejects(store.create("taken", new Uint8Array([1])));
  world.time.open = false;
  assert.deepEqual([env.faults.failed, env.faults.ambiguous], [20, 0]);
});

test("the same seed replays the same faults and ids", async () => {
  const first = await chaotic(11, 0.2, 2);
  const faults = first[0] as s.testing.Faults;
  assert.ok(faults.failed && faults.ambiguous && faults.crashed, JSON.stringify(faults));
  assert.deepEqual(await chaotic(11, 0.2, 2), first);
  assert.notDeepEqual((await chaotic(12, 0.2, 2)).slice(0, 2), first.slice(0, 2));
});

const manySteps = s.workflow("many_steps", async (c, { n }: { n: number }) => {
  for (let i = 0; i < n; i++) await c.step("step", () => i);
  return n;
});

test("chaos crashes a worker after about rate of the step attempts", async () => {
  let crashed = 0;
  for (let seed = 0; seed < 4; seed++) {
    const chaos = new s.testing.Chaos({ seed, rate: 0.05 });
    await using env = await s.testing.Env.open({ now: T0, chaos });
    await using _worker = env.worker([manySteps], { ...service, continueAsNewAfter: 0 });
    const run = await env.start(manySteps, { n: 100 });
    await env.advance(s.hours(1));
    assert.equal(await run.result(), 100);
    crashed += env.faults.crashed;
  }
  // 400 steps, each attempt that returns drawing a crash with probability 0.05.
  assert.ok(crashed >= 10 && crashed <= 35, String(crashed));
});

test("chaos faults the workers not the clients", async () => {
  const chaos = new s.testing.Chaos({ seed: 1, rate: 1 });
  await using env = await s.testing.Env.open({ now: T0, chaos });
  const run = await env.start(napping);
  await run.send("note", 1);
  assert.equal(await run.status(), "pending");
  assert.deepEqual(env.faults, { failed: 0, ambiguous: 0, crashed: 0 });
  await using worker = env.worker([napping], service);
  await env.advance(s.seconds(1));
  assert.ok(env.faults.failed > 0);
  worker.crash();
});

const ticking = s.workflow("ticking", async (c) => {
  const instants: string[] = [];
  for (let i = 0; i < 6; i++) {
    await c.sleep(s.hours(4));
    await c.step("tick", () => null);
    instants.push((await c.now()).toISOString());
  }
  return instants;
});

async function aChaoticDay(skipping: boolean) {
  await using env = await s.testing.Env.open({
    now: T0,
    chaos: new s.testing.Chaos({ seed: 11, rate: 0.05 }),
  });
  const world = worldOf(env);
  world.skipping = skipping;
  let results: unknown[];
  {
    await using _worker = env.worker([ticking], service);
    await using otherPod = await env.client();
    const run = await env.start(ticking, {}, { id: "early" });
    await env.advance(s.minutes(7 * 60 + 13));
    const late = await otherPod.start(ticking, {}, { id: "late" });
    await env.advance(s.hours(26));
    results = [await run.result(), await late.result()];
  }
  const same = { results, faults: env.faults, objects: new Map(world.objects), now: env.now };
  return { same, skipped: world.skipped };
}

test("chaos skips only the scans whose draws succeed", async () => {
  const skipped = await aChaoticDay(true);
  const ran = await aChaoticDay(false);
  // The same faults, the same histories, every event at the same instant: the store's keys and
  // bytes, the segments and their times included.
  assert.deepEqual(skipped.same, ran.same);
  const { faults } = skipped.same;
  assert.ok(faults.failed > 0 && faults.crashed > 0, JSON.stringify(faults));
  assert.ok(skipped.skipped > 2000 && ran.skipped === 0, `${skipped.skipped} ${ran.skipped}`);
});

for (const preset of ["service", "script"] as const) {
  test(`a month of virtual time is cheap [${preset}]`, async () => {
    await using env = await s.testing.Env.open({ now: T0 });
    await using _worker = env.worker([napping], { preset });
    await using otherPod = await env.client();
    const run = await env.start(napping);
    const started = performance.now();
    await env.advance(s.days(31));
    assert.ok(performance.now() - started < 500);
    assert.equal(await run.status(), "completed");
    // The scans skipped kept their instants: one started 10 s after a scan is found at the next.
    await env.advance(s.seconds(10));
    const late = await otherPod.start(napping, {}, { id: "late" });
    await env.advance(s.seconds(preset === "service" ? 19 : 0));
    assert.equal(await late.status(), "pending");
    await env.advance(s.seconds(1));
    assert.equal(await late.status(), "suspended");
  });
}
