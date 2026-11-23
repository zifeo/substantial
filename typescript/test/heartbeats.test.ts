// Heartbeats of step attempts (I-6): an attempt silent past its heartbeat timeout fails with a
// retryable `StepTimeout`, one that beats goes on. The ports of `rust/tests/heartbeats.rs`.
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import * as s from "../src/mod.ts";
import {
  type Context,
  type Duration,
  heartbeat,
  milliseconds,
  NotInWorkflow,
  StepFailed,
  Substantial,
  seconds,
  UndoFailed,
  workflow,
} from "../src/mod.ts";
import { LONG, script, token } from "./helpers.ts";
import { T0 } from "./testing_helpers.ts";

const BEAT = milliseconds(150);

/** A step that runs 500 ms, beating every 50 ms when `beats`: "ok", or its cause's type and
 * message. */
async function working(c: Context, name: string, beats: boolean, own?: Duration): Promise<string> {
  const body = async () => {
    for (let i = 0; i < 10; i++) {
      await sleep(50);
      if (beats) heartbeat();
    }
    return null;
  };
  const options = own === undefined ? {} : { heartbeatTimeout: own };
  try {
    await c.step(name, body, { timeout: seconds(5), ...options });
    return "ok";
  } catch (e) {
    if (e instanceof StepFailed) return `${e.cause.type}: ${e.cause.message}`;
    throw e;
  }
}

test("a silent attempt fails and a beating one goes on", async () => {
  const sub = await Substantial.create(`memory://heartbeats-${token()}`);
  const own = workflow("own", async (c: Context) => [
    await working(c, "silent", false, BEAT),
    await working(c, "beating", true, BEAT),
    await working(c, "unset", false),
  ]);
  const inherited = workflow(
    "inherited",
    async (c: Context) => [
      await working(c, "silent", false),
      await working(c, "patient", false, seconds(2)),
    ],
    { heartbeatTimeout: BEAT },
  );
  await using _ = script(sub, [own, inherited]);
  const silent = "StepTimeout: no heartbeat within 150 ms";
  // § vvp37rp40@tvfb8
  assert.deepEqual(await (await sub.start(own)).result({ timeout: LONG }), [silent, "ok", "ok"]);
  assert.deepEqual(await (await sub.start(inherited)).result({ timeout: LONG }), [silent, "ok"]);
});

test("a silent attempt is retried and an undo has its step's heartbeat", async () => {
  const sub = await Substantial.create(`memory://heartbeats-retried-${token()}`);
  let attempts = 0;
  const retried = workflow("retried", async (c: Context) =>
    c.step(
      "flaky",
      async () => {
        const n = attempts++;
        for (let i = 0; i < 10; i++) {
          await sleep(50);
          if (n > 0) heartbeat();
        }
        return n;
      },
      {
        retry: { maxAttempts: 2, initial: milliseconds(10), max: milliseconds(10), multiplier: 1 },
        heartbeatTimeout: BEAT,
      },
    ),
  );
  const undone = workflow("undone", async (c: Context) => {
    await c.step("charge", () => 1, {
      heartbeatTimeout: BEAT,
      undo: () => sleep(500),
    });
    try {
      await c.rollback("refused");
    } catch (e) {
      if (e instanceof UndoFailed) return `${e.step}: ${e.failure.message}`;
      throw e;
    }
    return "not undone";
  });
  await using _ = script(sub, [retried, undone]);
  assert.equal(await (await sub.start(retried)).result({ timeout: LONG }), 1, "the second beat");
  assert.equal(attempts, 2);
  const ended = (await (await sub.start(undone)).result({ timeout: LONG })) as string;
  // § vvp37rp40.b@abpc4
  assert.ok(ended.startsWith("charge.undo: "), ended);
  assert.ok(ended.includes("no heartbeat within 150 ms"), ended);
});

test("a heartbeat outside a step throws", () => {
  // § vvp37rp40.a@qsn3m
  let thrown: unknown;
  try {
    heartbeat();
  } catch (e) {
    thrown = e;
  }
  assert.ok(thrown instanceof NotInWorkflow, String(thrown));
});

test("a held attempt is silent past its heartbeat timeout", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  const held = workflow("held", async (c: Context) => {
    try {
      await c.step("export", () => null, {
        timeout: s.minutes(10),
        heartbeatTimeout: s.minutes(1),
      });
      return "ok";
    } catch (e) {
      if (e instanceof StepFailed) return e.cause.type;
      throw e;
    }
  });
  await using _ = env.worker([held], { preset: "service" });
  const hold = env.hold("export");
  const run = await env.start(held);
  await env.advance(milliseconds(s.minutes(1).ms - 1));
  assert.equal(hold.held, 1, "still within its heartbeat timeout");
  await env.advance(milliseconds(1));
  assert.equal(hold.held, 0);
  assert.equal(await run.result(), "StepTimeout");
  await env.check();
});
