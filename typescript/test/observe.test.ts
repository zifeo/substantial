// What a `Substantial` tells its observer, in the test environment and on the real clock.
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import * as s from "../src/mod.ts";
import { script, token } from "./helpers.ts";
import { T0 } from "./testing_helpers.ts";

const S3 = "aws-s3-standard-us-east-1-2026-11-09";
const service = { preset: "service" } as const;

class Recorder {
  readonly seen: s.Observed[] = [];
  readonly observe = (event: s.Observed): void => {
    this.seen.push(event);
  };

  of<T extends s.Observed>(kind: abstract new (...args: never[]) => T): T[] {
    return this.seen.filter((e): e is T => e instanceof kind);
  }
}

/** A step that fails once, retried, one that fails at once, then a sleep. */
function flaky() {
  let tries = 0;
  return s.workflow("flaky", async (c) => {
    const retry = { maxAttempts: 2, initial: s.seconds(1), max: s.seconds(1), multiplier: 1 };
    const value = await c.step(
      "once",
      () => {
        tries += 1;
        if (tries === 1) throw new Error("once");
        return tries;
      },
      { retry },
    );
    await assert.rejects(
      c.step("no", () => {
        throw new Error("no");
      }),
      s.StepFailed,
    );
    await c.sleep(s.minutes(1));
    return value;
  });
}

test("every kind of event with its fields", async () => {
  const seen = new Recorder();
  const wf = flaky();
  {
    await using env = await s.testing.Env.open({
      now: T0,
      prices: new s.Prices(S3),
      observe: seen.observe,
    });
    await using _worker = env.worker([wf], service);
    const run = await env.start(wf);
    await env.advance(s.minutes(2));
    assert.equal(await run.result(), 2);
  }

  const ops = seen.of(s.StoreOp);
  const per = { create: 5000n, list: 5000n, get: 400n, delete: 0n };
  const outcomes = {
    create: ["created", "exists"],
    get: ["found", "missing"],
    list: ["listed"],
    delete: ["deleted"],
  };
  for (const op of ops) {
    assert.deepEqual(op.cost, new s.Amount("USD", per[op.op] * BigInt(op.attempts)), `${op.op}`);
    assert.ok(op.attempts >= 1 && op.seconds >= 0, `${op.op}`);
    assert.ok(outcomes[op.op].includes(op.outcome), `${op.op} ${op.outcome}`);
  }
  assert.deepEqual(new Set(ops.map((op) => op.op)), new Set(Object.keys(per)));

  const tried = seen.of(s.Attempt).map((a) => {
    assert.ok(a.workflow === "flaky" && a.seconds >= 0);
    return [a.step, a.attempt, a.outcome];
  });
  assert.deepEqual(tried, [
    ["once", 1, "retry"],
    ["once", 2, "ok"],
    ["no", 1, "failed"],
  ]);

  const replays = seen.of(s.Replay);
  assert.ok(replays.every((r) => r.workflow === "flaky"));
  const ends = replays.map((r) => r.end);
  assert.equal(ends.at(-1), "ended");
  assert.ok(ends.includes("suspended"), `${ends}`);
  assert.ok(
    replays.every((r) => r.delay !== undefined && r.delay >= 0),
    "each serves a due entry",
  );

  const scans = seen.of(s.Scan);
  assert.ok(scans.every((scan) => scan.workflow === "flaky"));
  assert.ok(
    scans.some((scan) => scan.due > 0),
    JSON.stringify(scans),
  );
});

test("the events are classes of their fields", () => {
  const kind = (event: s.Observed): string => {
    if (event instanceof s.Attempt && event.outcome === "failed") return `failed ${event.step}`;
    if (event instanceof s.Replay && event.delay !== undefined) return "late";
    if (event instanceof s.StoreOp && event.cost !== undefined) return `${event.cost.nano} nano`;
    return "other";
  };
  assert.equal(kind(new s.Attempt("w", "pay", 1, 0.5, "failed")), "failed pay");
  assert.equal(kind(new s.Replay("w", 1.5, 0.1, "idle")), "late");
  assert.equal(kind(new s.Replay("w", undefined, 0.1, "idle")), "other");
  assert.equal(kind(new s.StoreOp("get", 0.1, 2, "found", new s.Amount("USD", 800n))), "800 nano");
  assert.equal(kind(new s.StoreOp("get", 0.1, 2, "found", undefined)), "other");
  assert.equal(kind(new s.Scan("w", 3)), "other");
});

test("attempts that time out or are left behind", async () => {
  // On the real clock: an attempt past its timeout, then one left behind by a shutdown.
  const seen = new Recorder();
  const leaving = s.workflow("leaving", async (c) => {
    await assert.rejects(
      c.step("late", () => sleep(5_000, 0, { signal: s.stepSignal() }), {
        timeout: s.milliseconds(100),
      }),
      s.StepFailed,
    );
    return await c.step("slow", () => sleep(3_600_000, 0, { signal: s.stepSignal() }));
  });
  {
    await using sub = await s.Substantial.create(`memory://observe-${token(4)}`, {
      observe: seen.observe,
    });
    await using _worker = script(sub, [leaving], { shutdownGrace: s.milliseconds(100) });
    await sub.start(leaving);
    const deadline = performance.now() + 5_000;
    while (seen.of(s.Attempt).length === 0) {
      assert.ok(performance.now() < deadline, "no attempt ended");
      await sleep(20);
    }
    // The second step runs once the timeout is recorded.
    await sleep(300);
  }
  assert.deepEqual(
    seen.of(s.Attempt).map((a) => [a.step, a.outcome]),
    [
      ["late", "timeout"],
      ["slow", "abandoned"],
    ],
  );
  assert.equal(seen.of(s.Replay).at(-1)?.end, "dropped");
});

test("a store operation retried reports its attempts", async () => {
  const seen = new Recorder();
  const steps = s.workflow("steps", async (c) => {
    for (let i = 0; i < 5; i++) await c.step("s", () => i);
    return 0;
  });
  const chaos = new s.testing.Chaos({ seed: 7, rate: 0.3 });
  await using env = await s.testing.Env.open({ now: T0, chaos, observe: seen.observe });
  await using _worker = env.worker([steps], service);
  const run = await env.start(steps);
  await env.advance(s.minutes(5));
  assert.equal(await run.result(), 0);
  assert.ok(env.faults.failed > 0);
  const ops = seen.of(s.StoreOp);
  assert.ok(ops.some((op) => op.attempts > 1 && op.outcome !== "failed"));
  assert.ok(
    ops.every((op) => op.cost === undefined),
    "no prices, no cost",
  );
});

class ObserverDown extends Error {}

test("an observer that throws stops the worker or fails the client call", async () => {
  const w = s.workflow("w", async () => 0);
  {
    const onScans = (event: s.Observed) => {
      if (event instanceof s.Scan) throw new ObserverDown("observer down");
    };
    await using env = await s.testing.Env.open({ now: T0, observe: onScans });
    const worker = env.worker([w], service);
    await assert.rejects(env.settle(), ObserverDown);
    assert.ok(!worker.alive && worker.error instanceof ObserverDown);
  }
  const onOps = (event: s.Observed) => {
    if (event instanceof s.StoreOp) throw new ObserverDown("observer down");
  };
  await assert.rejects(s.testing.Env.open({ now: T0, observe: onOps }), ObserverDown);
  await assert.rejects(
    s.Substantial.create(`memory://observe-${token(4)}`, { observe: onOps }),
    ObserverDown,
  );
});

test("a store operation of a worker whose observer throws stops it", async () => {
  const onDeletes = (event: s.Observed) => {
    if (event instanceof s.StoreOp && event.op === "delete") throw new ObserverDown("down");
  };
  const w = s.workflow("w", async (c) => {
    await c.sleep(s.minutes(1));
    return 0;
  });
  await using env = await s.testing.Env.open({ now: T0, observe: onDeletes });
  const worker = env.worker([w], service);
  await env.start(w);
  await assert.rejects(env.advance(s.minutes(2)), ObserverDown);
  assert.ok(!worker.alive && worker.error instanceof ObserverDown);
});

test("a client call whose observer throws fails with its error", async () => {
  let down = false;
  const onOps = (event: s.Observed) => {
    if (down && event instanceof s.StoreOp) throw new ObserverDown("down");
  };
  const w = s.workflow("w", async () => 0);
  await using env = await s.testing.Env.open({ now: T0, observe: onOps });
  const run = await env.start(w);
  down = true;
  await assert.rejects(run.status(), ObserverDown);
  await assert.rejects(env.start(w), ObserverDown);
});
