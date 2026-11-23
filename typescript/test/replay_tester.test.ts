// The replay tester (I-19): runs recorded on a store, replayed by new code in an environment. The
// ports of `rust/tests/replay_tester.rs`.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import {
  type Context,
  hours,
  InvalidArgument,
  LayoutError,
  NonDeterminism,
  RunNotFound,
  Substantial,
  workflow,
} from "../src/mod.ts";
import { openStore } from "../src/stores/open.ts";
import type { Store } from "../src/stores/store.ts";
import { eventually, script, token } from "./helpers.ts";

/** `charge`, a sleep of an hour, `ship`; `reserve` first when `reserving`. Counts the attempts
 * of its steps in `ran`. */
function order(reserving: boolean, ran: { n: number }) {
  return workflow("order", async (c: Context) => {
    const step = (name: string) =>
      c.step(name, async () => {
        ran.n += 1;
        return null;
      });
    if (reserving) await step("reserve");
    await step("charge");
    await c.sleep(hours(1));
    await step("ship");
    return "shipped";
  });
}

const actor = () => workflow("actor", async (c: Context) => await c.receive<number>("go"));
const quick = () => workflow("quick", async () => 1);

async function keys(store: Store): Promise<[string, string][]> {
  const all: [string, string][] = [];
  for (const key of (await store.list("", { maxKeys: 10_000 })).keys) {
    const bytes = (await store.get(key))?.data ?? new Uint8Array();
    all.push([key, Buffer.from(bytes).toString("hex")]);
  }
  return all;
}

test("new code replays recorded runs and writes nothing to them", async () => {
  const url = `memory://replay-${token()}`;
  const sub = await Substantial.create(url);
  const ran = { n: 0 };
  const worker = script(sub, [order(false, ran), actor(), quick()]);
  const suspended = await sub.start(order(false, ran), undefined, { id: "a" });
  const waiting = await sub.start(actor(), undefined, { id: "b" });
  const ended = await sub.start(quick(), undefined, { id: "c" });
  assert.equal(await ended.result(), 1);
  await eventually("a suspended order", async () => (await suspended.status()) === "suspended");
  await eventually("a waiting actor", async () => (await waiting.status()) === "suspended");
  await worker.shutdown();
  const source = await openStore(url, undefined);
  const before = await keys(source);

  const replayedRan = { n: 0 };
  const same = [order(false, replayedRan), actor(), quick()];
  const replayed = await s.testing.replay(url, same, { runs: ["a", "b", "c"] });
  // § awhhtbr5n@pyk4s
  const outcomes = replayed.map((r) => [r.run, r.outcome]);
  assert.deepEqual(outcomes, [
    ["a", "matched"],
    ["b", "matched"],
    ["c", "ended"],
  ]);
  // § end awhhtbr5n
  assert.equal(replayedRan.n, 0, "no step ran");

  const changed = [order(true, replayedRan)];
  const [diverged, ...rest] = await s.testing.replay(url, changed, { runs: ["a"] });
  assert.equal(rest.length, 0);
  assert.equal(diverged?.run, "a");
  assert.equal(diverged?.outcome, "diverged");
  assert.ok(diverged?.error instanceof NonDeterminism, String(diverged?.error));
  assert.match(diverged.error.message, /charge/);
  // § awhhtbr5n.b@dejms
  const attempts = replayedRan.n;
  assert.equal(attempts, 0, "no step ran");
  // § end awhhtbr5n.b
  // § awhhtbr5n.d@k1w14
  assert.deepEqual(await keys(source), before, "the source is as it was");
  await sub.close();
});

test("a replay names runs its workflows serve", async () => {
  const url = `memory://replay-${token()}`;
  const sub = await Substantial.create(url);
  await sub.start(quick(), undefined, { id: "c" });
  // § awhhtbr5n.c@evvbh
  const refused = async (runs: string[], code: s.Workflow[], url_ = url) => {
    try {
      await s.testing.replay(url_, code, { runs });
    } catch (e) {
      return e;
    }
    return undefined;
  };
  assert.ok((await refused([], [quick()])) instanceof InvalidArgument);
  assert.ok((await refused(["nope"], [quick()])) instanceof RunNotFound);
  assert.ok((await refused(["c"], [actor()])) instanceof InvalidArgument);
  const v2 = workflow("quick", async () => 1, { version: 2 });
  assert.ok((await refused(["c"], [v2])) instanceof InvalidArgument);
  const missing = `memory://replay-${token()}`;
  assert.ok((await refused(["c"], [quick()], missing)) instanceof LayoutError);
  const replayed = await s.testing.replay(url, [quick()], { runs: ["c"] });
  assert.equal(replayed[0]?.outcome, "matched");
  await sub.close();
});
