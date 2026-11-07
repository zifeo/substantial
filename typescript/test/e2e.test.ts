// Clients and workers end to end, on every adapter of this host.
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { createOn } from "../src/client.ts";
import {
  type Context,
  InvalidArgument,
  milliseconds,
  minutes,
  RunEnded,
  RunExists,
  RunFailed,
  RunNotFound,
  seconds,
  step,
  WaitTimeout,
  workflow,
} from "../src/mod.ts";
import { eachAdapter, eventually, LONG, script } from "./helpers.ts";

const roulette = step("roulette", () => 100, {
  retry: { maxAttempts: 6, initial: seconds(1), max: minutes(1), multiplier: 2 },
});
const multiply = step("multiply", (a: number, b: number) => a * b);

/** The workflow of I-3, its sleep shortened. */
const example = workflow("example", async (c: Context) => {
  let res = await roulette();
  await c.sleep(milliseconds(200));
  res = await multiply(res, 0.5);
  const n = await c.receive<number>("by");
  res = await multiply(res, n);
  let stopped = false;
  c.handle("stop", () => {
    stopped = true;
  });
  await c.waitUntil(() => stopped);
  return res;
});

eachAdapter("a first workflow", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [example]);
  const run = await sub.start(example);
  await run.send("by", 2);
  await run.send("stop");
  assert.equal(await run.result({ timeout: LONG }), 100);
  assert.equal(await run.status(), "completed");
});

const first = step("first", async () => null);

/** Records a step, so that the run has a segment, then waits for a note. */
const waiting = workflow("waiting", async (c: Context) => {
  await first();
  return await c.receive<string>("note");
});

eachAdapter("status moves from pending to the end", async (store) => {
  const sub = await createOn(store, { partitions: 2 });
  const run = await sub.start(waiting, {}, { id: "status-1" });
  assert.equal(await run.status(), "pending");
  await using _ = script(sub, [waiting]);
  await eventually("a suspended run", async () => (await run.status()) === "suspended");
  await assert.rejects(run.result({ timeout: milliseconds(50) }), WaitTimeout);
  await run.send("note", "hi", { id: "note-1" });
  assert.equal(await run.result({ timeout: LONG }), "hi");
  assert.equal(await run.status(), "completed");
  await assert.rejects(run.send("note", "again"), RunEnded);
});

eachAdapter("a waiting run is cancelled", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [waiting]);
  const run = await sub.start(waiting);
  await eventually("a suspended run", async () => (await run.status()) === "suspended");
  await run.cancel("not needed");
  const failed = await run.result({ timeout: LONG }).catch((e) => e);
  assert.ok(failed instanceof RunFailed);
  assert.equal(failed.status, "cancelled");
  assert.equal(failed.error.message, "not needed");
  assert.equal(await run.status(), "cancelled");
});

eachAdapter("a cancel reaches a run looping on steps", async (store) => {
  let ticks = 0;
  const looping = workflow("looping", async (c: Context) => {
    for (;;) {
      await c.step("tick", async () => {
        ticks += 1;
        await sleep(20);
      });
    }
  });
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [looping]);
  const run = await sub.start(looping);
  await eventually("a few steps", async () => ticks > 2);
  await run.cancel("stop looping");
  const failed = await run.result({ timeout: LONG }).catch((e) => e);
  assert.ok(failed instanceof RunFailed, String(failed));
  assert.equal(failed.status, "cancelled");
});

eachAdapter("unknown chains are not found", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  const run = sub.run(waiting, "nobody");
  const calls = [
    () => run.send("by", 1),
    () => run.cancel("x"),
    () => run.status(),
    () => run.result({ timeout: LONG }),
  ];
  for (const call of calls) await assert.rejects(call, RunNotFound);
  assert.equal(await run.read("progress"), undefined);
});

eachAdapter("starts are idempotent by id", async (store) => {
  let runs = 0;
  const count = step("count", () => ++runs);
  const counted = workflow("counted", async (_c: Context, input: { x: unknown }) => {
    await count();
    return input;
  });
  const sub = await createOn(store, { partitions: 1 });
  const once = await sub.start(counted, { x: 1 }, { id: "once" });
  const again = await sub.start(counted, { x: 1 }, { id: "once" });
  assert.equal(once.chain, again.chain);
  await assert.rejects(sub.start(counted, { x: 2 }, { id: "once" }), RunExists);
  await assert.rejects(sub.start(counted, 3 as never), InvalidArgument);
  await assert.rejects(sub.start(counted, [] as never), InvalidArgument);
  await assert.rejects(sub.start(counted, { x: new Map() }), InvalidArgument);
  await assert.rejects(sub.start(counted, { x: Number.NaN }), InvalidArgument);
  {
    await using _ = script(sub, [counted]);
    assert.deepEqual(await once.result({ timeout: LONG }), { x: 1 });
  }
  assert.equal(runs, 1, "one run");
});

eachAdapter("names outside the rules are invalid arguments", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  const long = "a".repeat(101);
  const keyRule = (what: string) => (e: unknown) =>
    e instanceof InvalidArgument &&
    e.message.startsWith(what) &&
    e.message.includes("must match [a-z0-9._-]{1,100}");
  for (const bad of ["Upper", ".", "..", "a/b", long]) {
    await assert.rejects(sub.start(waiting, {}, { id: bad }), keyRule("run id"));
    const run = sub.run(waiting, bad);
    const calls = [
      () => run.send("note", "x"),
      () => run.status(),
      () => run.cancel("x"),
      () => run.result({ timeout: LONG }),
    ];
    for (const call of calls) await assert.rejects(call, InvalidArgument);
  }
  const run = await sub.start(waiting, {}, { id: "fine" });
  for (const bad of ["Upper", "..", long]) {
    await assert.rejects(run.send("note", "x", { id: bad }), keyRule("message id"));
  }
  await assert.rejects(run.send("a note", "x"), /must match \[A-Za-z0-9._-\]\{1,128\}/);
  assert.throws(() => workflow("Upper", async () => null), keyRule("workflow name"));
});

eachAdapter(
  "ids of a hundred bytes work end to end",
  async (store) => {
    const sub = await createOn(store, { partitions: 1 });
    await using _ = script(sub, [waiting]);
    const run = await sub.start(waiting, {}, { id: "r".repeat(100) });
    await run.send("note", "hi", { id: "m".repeat(100) });
    assert.equal(await run.result({ timeout: LONG }), "hi");
  },
  ["file", "s3"],
);

eachAdapter("a step name outside the rules fails the run", async (store) => {
  const bad = workflow("bad-step", async (c: Context) => await c.step("a step", () => 1));
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [bad]);
  const run = await sub.start(bad);
  const failed = await run.result({ timeout: LONG }).catch((e) => e);
  assert.ok(failed instanceof RunFailed);
  assert.equal(failed.status, "failed");
  assert.equal(failed.error.type, "InvalidArgument");
});

eachAdapter("a message its decode refuses fails the run", async (store) => {
  const asNumber = (value: unknown): number => {
    if (typeof value !== "number") throw new TypeError(`${JSON.stringify(value)} is not a number`);
    return value;
  };
  const typed = workflow("typed", async (c: Context) => {
    const a = await c.receive("n", { decode: asNumber });
    const b = await c.receive("n", { decode: asNumber });
    return a + b;
  });
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [typed]);
  const run = await sub.start(typed);
  await run.send("n", 1);
  await run.send("n", "not a number");
  const failed = await run.result({ timeout: LONG }).catch((e) => e);
  assert.ok(failed instanceof RunFailed, String(failed));
  assert.equal(failed.error.type, "TypeError");
});
