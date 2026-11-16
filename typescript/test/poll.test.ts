// `c.poll`: a step tried again after a sleep until it returns a value or its timeout passes,
// ported from the Python tests under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { withEnv } from "./testing_helpers.ts";

let pokes = 0;
const service = { preset: "service" } as const;

/** Clears the pokes, then runs `fn` in a fresh environment at `T0`. */
function polled(fn: (env: s.testing.Env) => Promise<void>): () => Promise<void> {
  return withEnv(async (env) => {
    pokes = 0;
    await fn(env);
  });
}

const poller = s.workflow(
  "poller",
  async (c, input: { n: number; every: number; timeout: number }) => {
    const check = () => {
      pokes += 1;
      return pokes >= input.n ? "ready" : null;
    };
    const every = s.minutes(input.every);
    return await c.poll("check", check, { every, timeout: s.minutes(input.timeout) });
  },
);

test(
  "poll returns the first value that is not null",
  polled(async (env) => {
    await using _worker = env.worker([poller], service);
    const run = await env.start(poller, { n: 3, every: 1, timeout: 10 });
    await env.advance(s.minutes(2));
    assert.equal(await run.result(), "ready");
    assert.equal(pokes, 3);
    const events = (await run.history()).filter((e) => e.data.name === "check");
    assert.deepEqual(
      events.map((e) => [e.data.name, e.data.occurrence]),
      [
        ["check", 0],
        ["check", 1],
        ["check", 2],
      ],
    );
  }),
);

test(
  "poll returns undefined once its timeout passed",
  polled(async (env) => {
    await using _worker = env.worker([poller], service);
    const run = await env.start(poller, { n: 100, every: 3, timeout: 10 });
    await env.advance(s.minutes(9));
    assert.equal(pokes, 4);
    await env.advance(s.minutes(1));
    // A workflow's `undefined` is recorded as JSON's `null`.
    assert.equal(await run.result(), null);
    // At 0, 3, 6 and 9 minutes, and at 10, the sleep bounded by the time left.
    assert.equal(pokes, 5);
  }),
);

test(
  "a recorded poke never runs again",
  polled(async (env) => {
    await using _worker = env.worker([poller], { ...service, continueAsNewAfter: 0 });
    const run = await env.start(poller, { n: 2, every: 1, timeout: 10 });
    await env.advance(s.minutes(1));
    assert.equal(await run.result(), "ready");
    assert.equal(pokes, 2);
  }),
);

const flaky = s.workflow("flaky", async (c) => {
  const check = () => {
    pokes += 1;
    if (pokes === 1) throw new Error("provider down");
    return pokes;
  };
  const retry = { maxAttempts: 2, initial: s.seconds(1), max: s.seconds(1), multiplier: 1 };
  return await c.poll("check", check, { every: s.minutes(1), timeout: s.minutes(5), retry });
});

test(
  "each poke retries as its step says",
  polled(async (env) => {
    await using _worker = env.worker([flaky], service);
    const run = await env.start(flaky);
    await env.advance(s.seconds(1));
    assert.equal(await run.result(), 2);
  }),
);

for (const [every, timeout, message] of [
  [0, 10, /every is a positive Duration/],
  // A Duration is never negative: building one fails before `poll` sees it.
  [1, -1, /not negative/],
] as const) {
  test(
    `poll checks every and timeout [${every} ${timeout}]`,
    polled(async (env) => {
      await using _worker = env.worker([poller], service);
      const run = await env.start(poller, { n: 1, every, timeout });
      await assert.rejects(
        run.result(),
        (e) => e instanceof s.RunFailed && message.test(e.message),
      );
    }),
  );
}
