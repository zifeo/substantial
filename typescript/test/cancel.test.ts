// Cancellation (I-13), waits replayed from the history, and messages across a compression.
import assert from "node:assert/strict";
import { createOn } from "../src/client.ts";
import {
  Cancelled,
  type Context,
  isControl,
  milliseconds,
  RunFailed,
  workflow,
} from "../src/mod.ts";
import { eachAdapter, eventually, LONG, script } from "./helpers.ts";

/** Whether a "finish" message came, kept up to date by a handler. */
function finished(c: Context): { done: boolean } {
  const s = { done: false };
  c.handle("finish", () => {
    s.done = true;
  });
  return s;
}

eachAdapter("a cancelled run may roll back and still ends cancelled", async (store) => {
  const undone: string[] = [];
  const careful = workflow("careful", async (c: Context) => {
    await c.step("charge", () => "payment", { undo: (v) => undone.push(v) });
    const s = finished(c);
    try {
      await c.waitUntil(() => s.done);
    } catch (e) {
      if (!(e instanceof Cancelled)) throw e;
      await c.rollback("cancelled");
    }
  });
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [careful]);
  const run = await sub.start(careful);
  await eventually("the run suspended", async () => (await run.status()) === "suspended");
  await run.cancel("not needed");
  const failed = await run.result({ timeout: LONG }).catch((e) => e);
  assert.ok(failed instanceof RunFailed, String(failed));
  assert.equal(failed.status, "cancelled");
  assert.equal(failed.error.message, "not needed");
  assert.deepEqual(undone, ["payment"]);
});

eachAdapter(
  "a saga that sees Cancelled undoes its steps",
  async (store) => {
    const undone: string[] = [];
    const holder = workflow("holder", async (c: Context) => {
      await c.saga(async () => {
        await c.step("reserve", () => "seat", { undo: (v) => undone.push(v) });
        await c.receive("never");
      });
    });
    const sub = await createOn(store, { partitions: 1 });
    await using _ = script(sub, [holder]);
    const run = await sub.start(holder);
    await eventually("the run suspended", async () => (await run.status()) === "suspended");
    await run.cancel("stop");
    const failed = await run.result({ timeout: LONG }).catch((e) => e);
    assert.ok(failed instanceof RunFailed && failed.status === "cancelled", String(failed));
    assert.deepEqual(undone, ["seat"]);
  },
  ["memory", "file"],
);

eachAdapter("a wait already satisfied replays from the history", async (store) => {
  const replays: boolean[] = [];
  const twice = workflow("twice", async (c: Context) => {
    const s = finished(c);
    const first = await c.waitUntil(() => s.done, { timeout: milliseconds(5000) });
    replays.push(first);
    await c.sleep(milliseconds(100));
    const late = await c.waitUntil(() => false, { timeout: milliseconds(100) });
    return [first, late];
  });
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [twice]);
  const run = await sub.start(twice);
  await run.send("finish");
  assert.deepEqual(await run.result({ timeout: LONG }), [true, false]);
  assert.ok(replays.length >= 2, `replayed: ${replays}`);
  assert.ok(replays.every((r) => r));
});

eachAdapter("messages not consumed yet cross a compression", async (store) => {
  const looping = workflow("looping", async (c: Context) => {
    const s = { seen: [] as string[] };
    for await (const _ of c.iterate(s)) {
      const note = await c.receive<string>("note");
      s.seen.push(note);
      if (s.seen.length === 3) break;
    }
    return s.seen;
  });
  const sub = await createOn(store, { partitions: 1 });
  const run = await sub.start(looping);
  for (const note of ["a", "b", "c"]) await run.send("note", note);
  await using _ = script(sub, [looping], { continueAsNewAfter: 0 });
  assert.deepEqual(await run.result({ timeout: LONG }), ["a", "b", "c"]);
});

eachAdapter(
  "control flow is told apart",
  async (store) => {
    const quitting = workflow("quitting", async (c: Context) => {
      try {
        await c.cancel("bye");
      } catch (e) {
        if (isControl(e)) throw e;
      }
      return "swallowed";
    });
    const sub = await createOn(store, { partitions: 1 });
    await using _ = script(sub, [quitting]);
    const failed = await (await sub.start(quitting)).result({ timeout: LONG }).catch((e) => e);
    assert.ok(failed instanceof RunFailed && failed.status === "cancelled", String(failed));
  },
  ["memory"],
);
