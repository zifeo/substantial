// Children (I-11), loops and continue-as-new (I-12), end to end.
import assert from "node:assert/strict";
import { createOn } from "../src/client.ts";
import {
  Child,
  type Context,
  InvalidArgument,
  Permanent,
  RunFailed,
  stepKey,
  workflow,
} from "../src/mod.ts";
import { current } from "../src/scheduler.ts";
import { eachAdapter, LONG, script } from "./helpers.ts";

const double = workflow("double", async (c: Context, input: { n: number }) => {
  if (input.n === 0) {
    await c.step("refuse", () => {
      throw new Permanent("zero");
    });
  }
  return input.n * 2;
});

const listener = workflow("listener", async (c: Context) => c.receive<string>("hurry"));

const parent = workflow("parent", async (c: Context) => {
  const ok = await c.start(double, { n: 21 }, { id: "kid-ok" });
  const failing = await c.start(double, { n: 0 });
  const listening = await c.start(listener);
  await listening.send("hurry", "now");
  const heard = await listening.result();
  await c.waitUntil(() => ok.done && failing.done);
  const value = await ok.result();
  const failed = await failing.result().catch((e) => e);
  if (!(failed instanceof RunFailed)) throw new Error(`the failing child did not fail: ${failed}`);
  return [value, failed.error.type, ok.chain, heard];
});

eachAdapter("a parent awaits its children", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [parent, double, listener]);
  const run = await sub.start(parent);
  assert.deepEqual(await run.result({ timeout: LONG }), [42, "StepFailed", "kid-ok", "now"]);
  assert.equal(await sub.run(double, "kid-ok").result({ timeout: LONG }), 42);
});

const adopter = workflow("adopter", async (c: Context) => {
  const kid = await c.start(double, { n: 21 }, { id: "taken" });
  const failed = await kid.result().catch((e) => e);
  if (!(failed instanceof RunFailed))
    throw new Error(`a child of a taken id did not fail: ${failed}`);
  assert.equal(failed.status, "failed");
  return failed.error.type;
});

eachAdapter("a child id taken by another run fails the child", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [adopter, double]);
  const taken = await sub.start(double, { n: 1 }, { id: "taken" });
  assert.equal(await taken.result({ timeout: LONG }), 2);
  assert.equal(await (await sub.start(adopter)).result({ timeout: LONG }), "RunExists");
});

eachAdapter(
  "the result of a run not a child is invalid",
  async (store) => {
    const stranger = workflow("stranger", async () => {
      const shared = current.getStore();
      if (shared === undefined) throw new Error("no replay");
      try {
        await new Child(shared, "someone").result();
      } catch (e) {
        if (e instanceof InvalidArgument) return "refused";
        throw e;
      }
      return "accepted";
    });
    const sub = await createOn(store, { partitions: 1 });
    await using _ = script(sub, [stranger]);
    assert.equal(await (await sub.start(stranger)).result({ timeout: LONG }), "refused");
  },
  ["memory", "file"],
);

class Count {
  i = 0;
  get next(): number {
    return this.i + 1;
  }
}

eachAdapter("a loop compresses at every boundary", async (store) => {
  const keys: string[] = [];
  const counter = workflow("counter", async (c: Context) => {
    const s = new Count();
    for await (const same of c.iterate(s)) {
      assert.equal(same, s, "iterate yields the state given");
      keys.push(await c.step("tick", () => stepKey()));
      if (s.i === 3) break;
      s.i = s.next;
    }
    return s.i;
  });
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [counter], { continueAsNewAfter: 0 });
  const run = await sub.start(counter, {}, { id: "counter-1" });
  assert.equal(await run.result({ timeout: LONG }), 3);
  assert.equal(await run.status(), "completed");
  assert.equal(keys.length, 4, `one tick per iteration: ${keys}`);
  assert.equal(keys[0], "counter-1/tick/0");
  assert.equal(new Set(keys.map((k) => k.split("/")[0])).size, 4, `a run per iteration: ${keys}`);
  assert.ok(
    keys.every((k) => k.endsWith("/tick/0")),
    String(keys),
  );
});

eachAdapter(
  "a state that is not JSON fails a compression",
  async (store) => {
    const opaque = workflow("opaque", async (c: Context) => {
      const s = { i: 0, at: undefined as unknown };
      for await (const _ of c.iterate(s)) {
        s.i += 1;
        s.at = new Map();
      }
    });
    const notObject = workflow("not-object", async (c: Context) => {
      try {
        await c.iterate([1] as never).next();
      } catch (e) {
        if (e instanceof InvalidArgument) return "refused";
        throw e;
      }
      return "accepted";
    });
    const sub = await createOn(store, { partitions: 1 });
    await using _ = script(sub, [opaque, notObject], { continueAsNewAfter: 0 });
    const failed = await (await sub.start(opaque)).result({ timeout: LONG }).catch((e) => e);
    assert.ok(failed instanceof RunFailed, String(failed));
    assert.equal(failed.error.type, "NotCompressible", failed.error.message);
    assert.equal(await (await sub.start(notObject)).result({ timeout: LONG }), "refused");
  },
  ["memory"],
);

const countdown = workflow("countdown", async (c: Context, input: { n: number }) => {
  if (input.n > 0) await c.continueAsNew({ n: input.n - 1 });
  return "liftoff";
});

const landing = workflow(
  "landing",
  async (_c: Context, input: { n: number }) => `landed v2 after ${input.n}`,
  { version: 2 },
);

const flight = workflow("flight", async (c: Context, input: { n: number }) => {
  await c.continueAsNew({ n: input.n }, { workflow: landing });
});

eachAdapter("continue as new by hand", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [countdown, flight, landing]);
  const run = await sub.start(countdown, { n: 3 });
  assert.equal(await run.result({ timeout: LONG }), "liftoff");
  assert.equal(await run.status(), "completed");
  const other = await sub.start(flight, { n: 5 });
  assert.equal(await other.result({ timeout: LONG }), "landed v2 after 5");
});

const named = workflow("named", async (c: Context, input: { n: number }) => {
  if (input.n > 0) await c.continueAsNew({ n: input.n - 1 });
  return c.chain;
});

const naming = workflow("naming", async (c: Context) => {
  const kid = await c.start(named, { n: 1 });
  return { own: c.chain, kid: kid.chain, said: await kid.result() };
});

eachAdapter(
  "a run knows its chain, through a continuation too",
  async (store) => {
    const sub = await createOn(store, { partitions: 1 });
    await using _ = script(sub, [named, naming]);
    const run = await sub.start(naming, {}, { id: "naming-1" });
    const { own, kid, said } = (await run.result({ timeout: LONG })) as Record<string, string>;
    assert.equal(own, "naming-1");
    assert.equal(said, kid, "the child's c.chain is its handle's chain");
    assert.equal(await sub.run(named, kid as string).result({ timeout: LONG }), kid);
    const ulid = await sub.start(named, { n: 2 });
    assert.equal(await ulid.result({ timeout: LONG }), ulid.chain);
  },
  ["memory"],
);
