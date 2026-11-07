// Retention on every store, on the real clock: the collection's deletes and the tombstone, with
// retention and tombstone shortened through the settings a worker does not take publicly.
import assert from "node:assert/strict";
import { createOn } from "../src/client.ts";
import {
  type Context,
  milliseconds,
  RunExists,
  RunNotFound,
  seconds,
  workflow,
} from "../src/mod.ts";
import type { Store } from "../src/stores/store.ts";
import { eachAdapter, eventually, LONG, script } from "./helpers.ts";

const SHORT = {
  retention: milliseconds(300),
  retentionMin: milliseconds(300),
  tombstone: seconds(2),
  recheck: milliseconds(200),
};

const quick = workflow("quick", async (_c: Context) => 1);

async function keys(store: Store, prefix: string): Promise<string[]> {
  return (await store.list(prefix, { maxKeys: 1000 })).keys;
}

eachAdapter("a run is collected and its id taken until the tombstone ends", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [quick], SHORT);
  const run = await sub.start(quick, undefined, { id: "q" });
  assert.equal(await run.result({ timeout: LONG }), 1);
  const collected = async () =>
    JSON.stringify(await keys(store, "runs/q/")) === JSON.stringify(["runs/q/gone"]);
  await eventually("the collection", collected, 10_000);
  await assert.rejects(run.status(), (e) => e instanceof RunNotFound && e.collected);
  await assert.rejects(
    sub.start(quick, undefined, { id: "q" }),
    (e) => e instanceof RunExists && e.collected,
  );
  const ended = async () => (await keys(store, "runs/q/")).length === 0;
  await eventually("the tombstone's end", ended, 10_000);
  const again = await sub.start(quick, undefined, { id: "q" });
  assert.equal(await again.result({ timeout: LONG }), 1);
});
