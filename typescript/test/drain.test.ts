// `worker.drain({ within })`: a worker woken on a schedule serves what is due by its horizon, then
// stops; ported from the Python tests under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { token } from "./helpers.ts";
import { T0, withEnv } from "./testing_helpers.ts";

const script = { preset: "script" } as const;

const ship = s.step("ship", (order: string) => `shipped ${order}`);

const order = s.workflow("order", async (c, input: { order: string; wait: number }) => {
  await c.sleep(s.seconds(input.wait));
  return await ship(input.order);
});

function after(ms: number): Date {
  return new Date(T0.getTime() + ms);
}

test(
  "drain serves a run due now then stops",
  withEnv(async (env) => {
    const run = await env.start(order, { order: "o1", wait: 0 });
    const worker = env.worker([order], script);
    await worker.drain({ within: s.minutes(1) });
    assert.ok(!worker.alive);
    assert.equal(worker.error, undefined);
    assert.equal(await run.result(), "shipped o1");
    assert.deepEqual(env.now, T0);
  }),
);

test(
  "drain serves a run due within its horizon",
  withEnv(async (env) => {
    const run = await env.start(order, { order: "o1", wait: 30 });
    await env.worker([order], script).drain({ within: s.minutes(1) });
    assert.equal(await run.result(), "shipped o1");
    assert.deepEqual(env.now, after(30_000));
  }),
);

test(
  "drain leaves a run due after its horizon for later",
  withEnv(async (env) => {
    const run = await env.start(order, { order: "o1", wait: 120 });
    await env.worker([order], script).drain({ within: s.minutes(1) });
    assert.deepEqual(env.now, T0);
    assert.equal(await run.status(), "suspended");
    await env.advance(s.minutes(2));
    await env.worker([order], script).drain({ within: s.minutes(1) });
    assert.equal(await run.result(), "shipped o1");
  }),
);

test(
  "drain waits for a crashed workers claim expiring within its horizon",
  withEnv(async (env) => {
    const held = env.hold("ship");
    let run: s.Run;
    {
      await using crashed = env.worker([order], script);
      run = await env.start(order, { order: "o1", wait: 0 });
      await env.settle();
      assert.equal(held.held, 1);
      crashed.crash();
    }
    await env.worker([order], script).drain({ within: s.minutes(1) });
    assert.equal(await run.result(), "shipped o1");
    // Past claimTtl plus claimSkew of the crashed worker's claim.
    assert.deepEqual(env.now, after(35_001));
  }),
);

class ObserverDown extends Error {}

test(
  "drain raises the workers error",
  withEnv(async () => {
    const observe = (event: s.Observed) => {
      if (event instanceof s.Attempt) throw new ObserverDown("observer down");
    };
    await using failing = await s.testing.Env.open({ now: T0, observe });
    await failing.start(order, { order: "o1", wait: 0 });
    const worker = failing.worker([order], script);
    const error = console.error;
    console.error = () => {};
    try {
      await assert.rejects(
        worker.drain({ within: s.minutes(1) }),
        (e) => e instanceof ObserverDown && e.message === "observer down",
      );
    } finally {
      console.error = error;
    }
    assert.ok(worker.error instanceof ObserverDown);
    await failing.settle();
  }),
);

test(
  "drain checks within",
  withEnv(async (env) => {
    const worker = env.worker([order], script);
    await assert.rejects(
      worker.drain({ within: -1000 as unknown as s.Duration }),
      (e) => e instanceof s.InvalidArgument && /within/.test(e.message),
    );
  }),
);

test("drain on the real clock", async () => {
  await using sub = await s.Substantial.create(`memory://drain-${token(4)}`);
  const runs = [];
  for (let i = 0; i < 3; i++) runs.push(await sub.start(order, { order: `o${i}`, wait: 0 }));
  const late = await sub.start(order, { order: "late", wait: 3600 });
  const worker = sub.worker([order], script);
  await worker.drain({ within: s.seconds(1) });
  const results = [];
  for (const r of runs) results.push(await r.result());
  assert.deepEqual(results, ["shipped o0", "shipped o1", "shipped o2"]);
  assert.equal(await late.status(), "suspended");
  assert.ok(!worker.alive);
  assert.equal(worker.error, undefined);
});
