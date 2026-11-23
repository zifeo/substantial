// An error of the code that is no end blocks its run: nothing is recorded, the run reads blocked
// with its error, and it goes on once code that runs it is deployed (I-20). The ports of
// `python/tests/test_blocking.py` under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { T0, withEnv } from "./testing_helpers.ts";

const script = { preset: "script" } as const;
const doubled: number[] = [];

class Broken extends Error {
  override name = "Broken";
}

function doubling(fixed: boolean) {
  return s.workflow("doubling", async (c, { n }: { n: number }) => {
    const x = await c.step("double", () => {
      doubled.push(n);
      return n * 2;
    });
    if (!fixed) throw new Broken(`bug at ${x}`);
    return x;
  });
}

/** `fn` with every warning it logs, a line each. */
async function warned(fn: () => Promise<void>): Promise<string[]> {
  const warnings: string[] = [];
  const warn = console.warn;
  console.warn = (...data: unknown[]) => warnings.push(data.join(" "));
  try {
    await fn();
  } finally {
    console.warn = warn;
  }
  return warnings;
}

test("an error of the code blocks the run until a fix", async () => {
  doubled.length = 0;
  const told: s.Observed[] = [];
  await using env = await s.testing.Env.open({ now: T0, observe: (e) => told.push(e) });
  const broken = { type: "Broken", message: "bug at 4", retryable: false };
  let run: s.Run<number> | undefined;
  const warnings = await warned(async () => {
    await using _worker = env.worker([doubling(false)], script);
    run = await env.start(doubling(false), { n: 2 }, { id: "d" });
    await env.settle();
    // § hzew2nw4v@4bn6z
    assert.equal(await run.status(), "blocked");
    const [info] = await Array.fromAsync(env.runs(doubling(false)));
    assert.deepEqual([info?.waitingOn, info?.error], ["blocked", broken]);
    assert.equal(info?.dueAt?.getTime(), env.now.getTime() + 1000);
    // § end hzew2nw4v
    assert.deepEqual(await run.history(), [], "nothing recorded, the step since the last commit");
    const blocked = told.filter((e) => e instanceof s.Blocked);
    assert.deepEqual(blocked, [new s.Blocked("doubling", "d", broken)]);
    await assert.rejects(run.result(), /blocked by Broken: bug at 4/);
    await env.advance(s.seconds(1));
    const [again] = await Array.fromAsync(env.runs(doubling(false)));
    assert.equal(again?.dueAt?.getTime(), env.now.getTime() + 2000, "the backoff doubles");
  });
  assert.ok(warnings.includes("run d blocked: Broken: bug at 4"), String(warnings));
  assert.ok(run !== undefined);
  {
    await using _worker = env.worker([doubling(true)], script);
    await env.advance(s.seconds(2));
    assert.equal(await run.result(), 4);
  }
  assert.deepEqual(doubled, [2, 2, 2], "a step the replay did not commit runs again");
  assert.deepEqual(
    (await run.history()).map((e) => e.kind),
    ["step", "completed"],
  );
  assert.equal(await run.status(), "completed");
  await env.check();
});

/** Steps then waits for `go`, or, changed, waits for `go` then steps: the same name and no
 * version. */
function waiting(first: boolean) {
  return s.workflow("waiting", async (c) => {
    if (first) {
      await c.step("one", () => 1);
      await c.receive("go");
    } else {
      await c.receive("go");
      await c.step("one", () => 1);
    }
    return 1;
  });
}

test(
  "a divergence blocks the run until the code is rolled back",
  withEnv(async (env) => {
    let run: s.Run<number>;
    {
      await using _worker = env.worker([waiting(true)], script);
      run = await env.start(waiting(true));
      await env.settle();
    }
    await run.send("go");
    {
      await using _worker = env.worker([waiting(false)], script);
      await env.settle();
      assert.equal(await run.status(), "blocked");
      const [info] = await Array.fromAsync(env.runs(waiting(false)));
      assert.equal(info?.error?.type, "NonDeterminism", JSON.stringify(info));
    }
    await using _worker = env.worker([waiting(true)], script);
    await env.advance(s.seconds(1));
    assert.equal(await run.result(), 1);
  }),
);

/** `waiting`, whose code catches what its body raises and fails on purpose, as code that wraps a
 * whole workflow in a handler does. */
function catching(first: boolean) {
  return s.workflow("waiting", async (c) => {
    try {
      if (first) {
        await c.step("one", () => 1);
        await c.receive("go");
      } else {
        await c.receive("go");
        await c.step("one", () => 1);
      }
      return 1;
    } catch (e) {
      if (e instanceof s.NonDeterminism) throw new s.Fail("Caught", e.message);
      throw e;
    }
  });
}

test(
  "a divergence the code catches and fails on still blocks the run",
  withEnv(async (env) => {
    let run: s.Run<number>;
    {
      await using _worker = env.worker([catching(true)], script);
      run = await env.start(catching(true));
      await env.settle();
    }
    await run.send("go");
    {
      await using _worker = env.worker([catching(false)], script);
      await env.settle();
      // § rbwstdetg.e@z1w7v
      assert.equal(await run.status(), "blocked");
      const [info] = await Array.fromAsync(env.runs(catching(false)));
      assert.equal(info?.error?.type, "NonDeterminism", JSON.stringify(info));
      // § end rbwstdetg.e
    }
    await using _worker = env.worker([catching(true)], script);
    await env.advance(s.seconds(1));
    assert.equal(await run.result(), 1);
  }),
);

const refusing = s.workflow("refusing", async () => {
  throw new s.Fail("Refused", "not today");
});

test(
  "fail ends the run failed",
  withEnv(async (env) => {
    await using _worker = env.worker([refusing], script);
    const run = await env.start(refusing);
    // § dxgjekysz.d@141aj
    const ended = await run.result().then(
      () => undefined,
      (e: unknown) => e,
    );
    assert.ok(ended instanceof s.RunFailed, String(ended));
    assert.deepEqual(ended.error, { type: "Refused", message: "not today", retryable: false });
    assert.equal(await run.status(), "failed");
    // § end dxgjekysz.d
  }),
);

test("fail takes a type and a message", () => {
  for (const [type, message] of [
    [1, "m"],
    ["T", undefined],
  ]) {
    assert.throws(() => new s.Fail(type as string, message as string), s.InvalidArgument);
  }
  assert.equal(new s.Fail("T", "m").message, "T: m");
});

const timestamped = s.workflow("timestamped", async (c) => await c.now());

test(
  "a result JSON cannot hold blocks the run",
  withEnv(async (env) => {
    await using _worker = env.worker([timestamped], script);
    const run = await env.start(timestamped);
    await assert.rejects(
      run.result(),
      (e) =>
        e instanceof s.testing.Stuck && /blocked by Json: the value of timestamped/.test(e.message),
    );
  }),
);
