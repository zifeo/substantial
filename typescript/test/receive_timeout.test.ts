// `c.receive(name, { timeout })`: the next message of a name, or `WaitTimeout` once the timeout
// passed with none, ported from the Python tests under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { iso, withEnv } from "./testing_helpers.ts";

const service = { preset: "service" } as const;

const receiver = s.workflow("receiver", async (c) => {
  try {
    return { value: await c.receive("note", { timeout: s.minutes(5) }) };
  } catch (e) {
    if (!(e instanceof s.WaitTimeout)) throw e;
    return { timeout: e.message, at: (await c.now()).toISOString() };
  }
});

test(
  "a message within the timeout is returned",
  withEnv(async (env) => {
    await using _worker = env.worker([receiver], service);
    const run = await env.start(receiver);
    await env.advance(s.minutes(4));
    await run.send("note", "hello");
    assert.deepEqual(await run.result(), { value: "hello" });
  }),
);

test(
  "a null value is returned as such",
  withEnv(async (env) => {
    await using _worker = env.worker([receiver], service);
    const run = await env.start(receiver);
    await run.send("note", null);
    assert.deepEqual(await run.result(), { value: null });
  }),
);

test(
  "none within the timeout raises WaitTimeout at its instant",
  withEnv(async (env) => {
    await using _worker = env.worker([receiver], service);
    const run = await env.start(receiver);
    await env.advance(s.minutes(5));
    assert.deepEqual(await run.result(), {
      timeout: 'no message "note" within 300000ms',
      at: iso(5 * 60_000),
    });
  }),
);

const interleaved = s.workflow("interleaved", async (c) => {
  const seen: number[] = [];
  c.handle("n", (n: number) => seen.push(n));
  const first = await c.receive("n");
  const second = await c.receive("n", { timeout: s.minutes(5) });
  let timedOut = false;
  try {
    await c.receive("n", { timeout: s.minutes(5) });
  } catch (e) {
    if (!(e instanceof s.WaitTimeout)) throw e;
    timedOut = true;
  }
  const third = await c.receive("n");
  const fourth = await c.receive("n", { timeout: s.minutes(1) });
  return [first, second, timedOut, third, fourth, seen];
});

test(
  "receives with and without timeout interleave in order",
  withEnv(async (env) => {
    await using _worker = env.worker([interleaved], service);
    const run = await env.start(interleaved);
    await run.send("n", 1);
    await run.send("n", 2);
    await env.advance(s.minutes(6));
    await run.send("n", 3);
    await run.send("n", 4);
    assert.deepEqual(await run.result(), [1, 2, true, 3, 4, [1, 2, 3, 4]]);
  }),
);

const pair = s.workflow("pair", async (c) => {
  const waits = [0, 1].map(() => c.receive("n", { timeout: s.minutes(5) }));
  const results = await Promise.allSettled(waits);
  return results.map((r) => (r.status === "fulfilled" ? r.value : (r.reason as Error).name));
});

test(
  "two receives waiting together take one message each",
  withEnv(async (env) => {
    await using _worker = env.worker([pair], service);
    const run = await env.start(pair);
    await env.advance(s.minutes(1));
    await run.send("n", 1);
    await env.advance(s.minutes(5));
    assert.deepEqual(await run.result(), [1, "WaitTimeout"]);
  }),
);

const mixed = s.workflow("mixed", async (c, { timedFirst }: { timedFirst: boolean }) => {
  try {
    if (timedFirst) {
      c.receive("n", { timeout: s.minutes(5) }).catch(() => {});
      c.receive("n");
    } else {
      c.receive("n");
      await c.receive("n", { timeout: s.minutes(5) });
    }
  } catch (e) {
    if (!(e instanceof s.InvalidArgument)) throw e;
    return e.message;
  }
  return "accepted";
});

for (const timedFirst of [true, false]) {
  test(
    `a receive with and one without timeout pending at once are refused [${timedFirst}]`,
    withEnv(async (env) => {
      await using _worker = env.worker([mixed], service);
      const run = await env.start(mixed, { timedFirst });
      const result = (await run.result()) as string;
      assert.match(result, /pending at once/);
    }),
  );
}
