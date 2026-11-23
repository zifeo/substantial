// Retention on every store, on the real clock: an ended run's head is written with its retention
// as its expiry, which the store then removes it by. The port of `rust/tests/store_retention.rs`,
// under the same names.
import assert from "node:assert/strict";
import { createOn } from "../src/client.ts";
import { type Context, RunExists, workflow } from "../src/mod.ts";
import type { Store } from "../src/stores/store.ts";
import { wake } from "../src/worker.ts";
import { eachAdapter, eventually, LONG, Recording, script } from "./helpers.ts";

const quick = workflow("quick", async (_c: Context) => 1, { retention: 1 });

async function keys(store: Store, prefix: string): Promise<string[]> {
  return (await store.list(prefix, { maxKeys: 1000 })).keys;
}

eachAdapter("an ended run is written to expire after its retention", async (store) => {
  const recording = new Recording(store);
  const sub = await createOn(recording);
  const worker = script(sub, [quick]);
  const run = await sub.start(quick, undefined, { id: "q" });
  assert.equal(await run.result({ timeout: LONG }), 1);
  await worker.shutdown();
  const expiries = recording.writes.filter(([k]) => k === "runs/q").map(([, expiry]) => expiry);
  assert.equal(expiries.at(-1), 1, JSON.stringify(expiries));
  assert.ok(
    expiries.slice(0, -1).every((e) => e === undefined),
    JSON.stringify(expiries),
  );
  assert.equal(await run.status(), "completed");
  assert.equal(await run.result(), 1);
  const other = workflow("other", async () => 2);
  await assert.rejects(
    sub.start(other, undefined, { id: "q" }),
    (e) => e instanceof RunExists && e.message.startsWith("id q "),
  );
});

/** A wakeup of `q` due now, as a late sender or a crashed worker leaves one. */
function lateWakeup(name: string): string {
  const at = new Date().toISOString().replace(/[-:.]/g, "").toLowerCase();
  return `queues/${name}/due/${at}~q~s0000000001`;
}

const publishing = workflow(
  "publishing",
  async (c: Context) => {
    c.publish("p", 1);
    return await c.step("s", () => 1);
  },
  { retention: 1 },
);

eachAdapter("the keys of a removed run never come back", async (store) => {
  const sub = await createOn(store);
  await using worker = script(sub, [publishing]);
  const run = await sub.start(publishing, undefined, { id: "q" });
  assert.equal(await run.result({ timeout: LONG }), 1);
  assert.equal(await run.read("p"), 1);
  // As its expiry removes it.
  await store.delete("runs/q");
  const late = lateWakeup("publishing");
  await store.create(late, new Uint8Array(), undefined);
  wake(worker);
  await eventually(
    "the late wakeup served",
    async () => !(await keys(store, "queues/publishing/due/")).includes(late),
    10_000,
  );
  assert.equal(await run.read("p"), undefined);
  assert.deepEqual(await keys(store, "runs/q"), []);
  assert.deepEqual(await keys(store, "archives/"), []);
});
