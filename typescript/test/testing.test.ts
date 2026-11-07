// The test environment of docs/testing.md: the environment, its ids and its time, ported from the
// Python tests under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { objectsOf } from "../src/stores/memory.ts";
import { worldOf } from "../src/testing/env.ts";
import { idOf, iso, T0, withEnv } from "./testing_helpers.ts";

const debounce = s.workflow("debounce", async (c, _input: { key: string }) => {
  const events: number[] = [];
  c.handle<number>("event", (e) => {
    events.push(e);
  });
  let seen = 0;
  for (;;) {
    const n = seen;
    if (!(await c.waitUntil(() => events.length > n, { timeout: s.minutes(5) }))) break;
    seen = events.length;
  }
  return events;
});

/** Publishes how many events it has seen, until two. */
const seen = s.workflow("seen", async (c) => {
  const events: number[] = [];
  c.handle<number>("event", (e) => {
    events.push(e);
  });
  while (events.length < 2) {
    const n = events.length;
    c.publish("seen", n);
    await c.waitUntil(() => events.length > n);
  }
  return events.length;
});

const clocks = s.workflow("clocks", async (c) => [
  (await c.now()).toISOString(),
  await c.step("wall", () => new Date().toISOString()),
]);

const napping = s.workflow("napping", async (c, input: { days: number }) => {
  await c.sleep(s.days(input.days));
  return (await c.now()).toISOString();
});

const randoms = s.workflow("randoms", async (c) => {
  await c.step("nothing", async () => null);
  return [await c.random(), await c.uuid()];
});

const parent = s.workflow("parent", async (c) => {
  const child = await c.start(napping, { days: 0 }, { id: "the-child" });
  return await child.result();
});

const service = { preset: "service" } as const;

test("the debounce of the docs", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  await using _worker = env.worker([debounce], service);
  const run = await env.start(debounce, { key: "k" }, { id: "k" });
  await run.send("event", 1);
  await env.advance(s.minutes(4));
  await run.send("event", 2);
  await env.advance(s.minutes(4));
  assert.equal(await run.status(), "suspended");
  await env.advance(s.minutes(1));
  assert.deepEqual(await run.result(), [1, 2]);
  await env.check();
});

test("an environment opens at now and drops its namespace when left", async () => {
  let ns: string;
  {
    await using env = await s.testing.Env.open({ now: T0 });
    assert.ok(env instanceof s.Substantial);
    assert.deepEqual(env.now, T0);
    ns = worldOf(env).ns;
    assert.ok(objectsOf(ns));
  }
  assert.equal(objectsOf(ns), undefined);
  await assert.rejects(s.testing.Env.open({ now: new Date("not a date") }), s.InvalidArgument);
});

test("an environment's clock counts whole milliseconds", async () => {
  // A Date holds whole milliseconds: a finer instant cannot reach the environment.
  const now = new Date(T0.getTime() + 123.456);
  await using env = await s.testing.Env.open({ now });
  assert.equal(env.now.getTime(), T0.getTime() + 123);
});

test(
  "nothing runs between two calls of the test",
  withEnv(async (env) => {
    await using _worker = env.worker([napping], service);
    const run = await env.start(napping, { days: 1 });
    assert.equal(await run.status(), "pending");
    await env.settle();
    assert.equal(await run.status(), "suspended");
    assert.deepEqual(env.now, T0);
  }),
);

test(
  "a run that only waits reads suspended once a worker took it",
  withEnv(async (env) => {
    const idle = s.workflow("idle", (c) => c.receive<number>("go"));
    await using _worker = env.worker([idle], service);
    const run = await env.start(idle);
    assert.equal(await run.status(), "pending");
    await env.settle();
    assert.equal(await run.status(), "suspended");
    await run.send("go", 1);
    assert.equal(await run.result(), 1);
  }),
);

test(
  "two sends in a row reach the run together",
  withEnv(async (env) => {
    await using _worker = env.worker([seen], service);
    const together = await env.start(seen, {}, { id: "together" });
    await together.send("event", 1);
    await together.send("event", 2);
    const apart = await env.start(seen, {}, { id: "apart" });
    await apart.send("event", 1);
    await env.settle();
    assert.equal(await apart.read("seen"), 1);
    await apart.send("event", 2);
    assert.equal(await together.result(), 2);
    assert.equal(await apart.result(), 2);
    // One replay took both messages, in one segment; the other run woke for each.
    assert.equal(worldOf(env).keys("runs/together/log/").length, 1);
    assert.equal(worldOf(env).keys("runs/apart/log/").length, 2);
  }),
);

test(
  "a client shares the namespace and the clock",
  withEnv(async (env) => {
    {
      await using client = await env.client();
      await using _worker = env.worker([napping], service);
      assert.ok(client instanceof s.Substantial);
      await env.advance(s.seconds(10));
      const run = await client.start(napping, { days: 0 }, { id: "from-a-client" });
      // No worker of the client: the environment's finds the run at its next scan, at 30 s.
      await env.advance(s.seconds(19));
      assert.equal(await run.status(), "pending");
      await env.advance(s.seconds(1));
      const result = await env.run(napping, "from-a-client").result();
      assert.equal(result, iso(30_000));
      assert.equal(await run.status(), "completed");
    }
    const other = await env.client();
    assert.equal(await other.run(napping, "from-a-client").status(), "completed");
    await other.close();
  }),
);

test(
  "a step reads the wall clock and the run the virtual one",
  withEnv(async (env) => {
    await using _worker = env.worker([clocks], { preset: "script" });
    const [virtual, wall] = (await (await env.start(clocks)).result()) as string[];
    assert.equal(virtual, T0.toISOString());
    assert.ok(Math.abs(new Date(wall as string).getTime() - Date.now()) < 60_000);
  }),
);

test(
  "a timer fires at its due instant",
  withEnv(async (env) => {
    await using _worker = env.worker([napping], service);
    const run = await env.start(napping, { days: 30 });
    await env.advance(s.milliseconds(s.days(30).ms - 1000));
    assert.equal(await run.status(), "suspended");
    await env.advance(s.seconds(1));
    assert.equal(await run.result(), iso(s.days(30).ms));
  }),
);

test(
  "a child is found at once by the worker that started it",
  withEnv(async (env) => {
    await using _worker = env.worker([parent, napping], service);
    const run = await env.start(parent);
    assert.equal(await run.result(), T0.toISOString());
  }),
);

test(
  "result settles and never moves the clock",
  withEnv(async (env) => {
    await using _worker = env.worker([napping], service);
    const run = await env.start(napping, { days: 2 });
    await assert.rejects(run.result(), stuck(/timer due at 2030-01-09T09:00:00\.000Z/));
    await assert.rejects(run.result({ timeout: s.days(3) }), s.WaitTimeout);
    assert.deepEqual(env.now, T0);
    await env.advance(s.days(2));
    assert.equal(await run.result({ timeout: s.seconds(1) }), iso(s.days(2).ms));
  }),
);

function stuck(pattern: RegExp): (e: unknown) => boolean {
  return (e) => e instanceof s.testing.Stuck && pattern.test(e.message);
}

test(
  "stuck names a message and a workflow no worker serves",
  withEnv(async (env) => {
    await using _worker = env.worker([seen, parent], service);
    const waiting = await env.start(seen, {}, { id: "waiting" });
    await assert.rejects(waiting.result(), stuck(/a message/));
    const orphan = await env.start(napping, { days: 0 }, { id: "orphan" });
    await assert.rejects(orphan.result(), stuck(/napping, which no worker serves/));
    const aParent = await env.start(parent, {}, { id: "a-parent" });
    await assert.rejects(aParent.result(), stuck(/the-child/));
  }),
);

/** The ids and randoms of one run, by one worker or by a second one after the first crashed. */
async function ids(chaos: s.testing.Chaos | undefined, takeover: boolean): Promise<unknown[]> {
  await using env = await s.testing.Env.open(
    chaos === undefined ? { now: T0 } : { now: T0, chaos },
  );
  await using first = env.worker([randoms], service);
  await using second = env.worker([randoms], service);
  const hold = takeover ? env.hold("nothing") : undefined;
  const run = await env.start(randoms);
  await env.settle();
  if (hold !== undefined) {
    hold.crash();
    await env.advance(s.minutes(1));
  }
  return [run.chain, await run.result(), idOf(env, first), idOf(env, second)];
}

test("ids and randoms are drawn from the seed and what they name", async () => {
  const plain = await ids(undefined, false);
  assert.deepEqual(await ids(undefined, false), plain);
  assert.deepEqual(await ids(new s.testing.Chaos({ seed: 0, rate: 0 }), true), plain);
  const seeded = await ids(new s.testing.Chaos({ seed: 7, rate: 0 }), false);
  plain.forEach((a, i) => {
    assert.notDeepEqual(a, seeded[i]);
  });
});
