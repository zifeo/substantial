// Time, randomness, step keys, names, cancellation from inside, publishing, versions and logs,
// end to end.
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { createOn } from "../src/client.ts";
import {
  ConfigError,
  type Context,
  InvalidArgument,
  milliseconds,
  RunFailed,
  step,
  stepKey,
  workflow,
} from "../src/mod.ts";
import { eachAdapter, eventually, LONG, script } from "./helpers.ts";

eachAdapter("now, sleepUntil and step keys", async (store) => {
  const timed = workflow("timed", async (c: Context) => {
    const start = await c.now();
    await c.sleepUntil(new Date(start.getTime() + 300));
    const end = await c.now();
    const keys = [];
    for (let i = 0; i < 2; i++) keys.push(await c.step("keyed", () => stepKey()));
    return { slept: end.getTime() - start.getTime(), keys };
  });
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [timed]);
  const run = await sub.start(timed, {}, { id: "timed-1" });
  const { slept, keys } = (await run.result({ timeout: LONG })) as {
    slept: number;
    keys: string[];
  };
  assert.ok(slept >= 300, `slept ${slept} ms`);
  assert.deepEqual(keys, ["timed-1/keyed/0", "timed-1/keyed/1"]);
});

eachAdapter("random and uuid are recorded", async (store) => {
  const seen: string[] = [];
  const rolled = workflow("rolled", async (c: Context) => {
    const x = await c.random();
    const id = await c.uuid();
    seen.push(`${x} ${id}`);
    c.publish("rolled", { x, id });
    await c.receive("again");
    return { x, id };
  });
  const sub = await createOn(store, { partitions: 1 });
  const run = await sub.start(rolled);
  {
    // A commit lists the inbox after it writes the segment, so a message sent once the roll is
    // published could still reach the same replay: only a new worker surely replays.
    await using _ = script(sub, [rolled]);
    await eventually("the roll published", async () => (await run.read("rolled")) !== undefined);
  }
  await run.send("again");
  await using _ = script(sub, [rolled]);
  const { x, id } = (await run.result({ timeout: LONG })) as { x: number; id: string };
  assert.ok(x >= 0 && x < 1);
  assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  assert.ok(seen.length >= 2, "replayed after the message");
  assert.deepEqual(new Set(seen), new Set([`${x} ${id}`]), "every replay saw the same values");
});

eachAdapter(
  "reserved and invalid names are refused in a workflow",
  async (store) => {
    const names = workflow("names", async (c: Context) => {
      const refused = [];
      for (const bad of ["substantial.ended", "a note", ""]) {
        for (const call of [() => c.receive(bad), () => c.handle(bad, () => {})]) {
          try {
            call();
          } catch (e) {
            if (e instanceof InvalidArgument) refused.push(bad);
          }
        }
      }
      return refused;
    });
    const sub = await createOn(store, { partitions: 1 });
    await using _ = script(sub, [names]);
    const run = await sub.start(names);
    const twice = (name: string) => [name, name];
    assert.deepEqual(await run.result({ timeout: LONG }), [
      ...twice("substantial.ended"),
      ...twice("a note"),
      ...twice(""),
    ]);
  },
  ["memory"],
);

eachAdapter("a run cancels itself", async (store) => {
  const quitter = workflow("quitter", async (c: Context) => {
    await c.step("first", () => null);
    await c.cancel("enough");
  });
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [quitter]);
  const failed = await (await sub.start(quitter)).result({ timeout: LONG }).catch((e) => e);
  assert.ok(failed instanceof RunFailed, String(failed));
  assert.equal(failed.status, "cancelled");
  assert.equal(failed.error.message, "enough");
});

eachAdapter("published values are read by clients", async (store) => {
  const publisher = workflow("publisher", async (c: Context) => {
    c.publish("progress", { done: 1 });
    await c.step("work", () => null);
    c.publish("progress", { done: 2 });
    await c.receive("go");
    try {
      c.publish("Not A Key", 0);
    } catch (e) {
      if (e instanceof InvalidArgument) return true;
    }
    return false;
  });
  const sub = await createOn(store, { partitions: 1 });
  const run = await sub.start(publisher, {}, { id: "pub-1" });
  assert.equal(await run.read("progress"), undefined);
  {
    await using _ = script(sub, [publisher]);
    await eventually("the last value published", async () => {
      const read = await run.read<{ done: number }>("progress");
      return read?.done === 2;
    });
    await run.send("go");
    assert.equal(await run.result({ timeout: LONG }), true);
  }
  assert.deepEqual(await run.read("progress"), { done: 2 });
  assert.equal(await run.read("other"), undefined);
  await assert.rejects(run.read("Not A Key"), InvalidArgument);
});

function versioned(version?: number) {
  const options = version === undefined ? {} : { version };
  return workflow(
    "versioned",
    async () => (version === undefined ? "none" : `v${version}`),
    options,
  );
}

eachAdapter("runs are replayed by the code of their version", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  assert.throws(() => script(sub, [versioned(), versioned(2), versioned(2)]), ConfigError);
  const none = await sub.start(versioned());
  {
    await using _ = script(sub, [versioned(1)]);
    const one = await sub.start(versioned(1));
    assert.equal(await one.result({ timeout: LONG }), "v1");
    await sleep(300);
    assert.equal(await none.status(), "pending", "a version-only worker leaves it");
  }
  await using _ = script(sub, [versioned(), versioned(1)]);
  const two = await sub.start(versioned(2));
  assert.equal(await none.result({ timeout: LONG }), "none");
  await sleep(300);
  assert.equal(await two.status(), "pending");
  await using _all = script(sub, [versioned(), versioned(1), versioned(2)]);
  assert.equal(await two.result({ timeout: LONG }), "v2");
});

/** Version 1 steps then sleeps, version 2 sleeps then steps: the same name, no version. */
function changed(first: boolean) {
  return workflow("changed", async (c: Context) => {
    if (first) {
      await c.step("one", () => 1);
      await c.sleep(milliseconds(300));
    } else {
      await c.sleep(milliseconds(300));
      await c.step("one", () => 1);
    }
    return 1;
  });
}

eachAdapter("an incompatible change fails the run with NonDeterminism", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  const run = await sub.start(changed(true));
  {
    await using _ = script(sub, [changed(true)]);
    await eventually("the sleep", async () => (await run.status()) === "suspended");
  }
  await using _ = script(sub, [changed(false)]);
  await assert.rejects(run.result({ timeout: LONG }), (e) => {
    assert.ok(e instanceof RunFailed, String(e));
    assert.equal(e.status, "failed");
    assert.equal(e.error.type, "NonDeterminism");
    assert.ok(
      e.message.endsWith("; an incompatible change needs a new version (version=)"),
      e.message,
    );
    return true;
  });
});

const work = step("work", async (n: number) => n);

const logging = workflow("logging", async (c: Context) => {
  c.log.info("before");
  const a = await work(1);
  await c.sleep(milliseconds(100));
  const b = await work(2);
  c.log.warn(`after ${a + b}`);
  return a + b;
});

eachAdapter(
  "logs are muted while a replay walks the history",
  async (store, adapter) => {
    const lines: string[] = [];
    const info = console.info;
    const warn = console.warn;
    console.info = (...data: unknown[]) => lines.push(`info ${data.join(" ")}`);
    console.warn = (...data: unknown[]) => lines.push(`warn ${data.join(" ")}`);
    try {
      const sub = await createOn(store, { partitions: 1 });
      await using _ = script(sub, [logging]);
      assert.equal(await (await sub.start(logging)).result({ timeout: LONG }), 3);
    } finally {
      console.info = info;
      console.warn = warn;
    }
    assert.deepEqual(lines, ["info before", "warn after 3"], adapter);
  },
  ["memory", "file"],
);

test("a step's options are checked when it is declared", () => {
  assert.throws(() => step("s", () => 1, { retries: 3 } as never), InvalidArgument);
  assert.throws(() => step("s", () => 1, { timeout: 5 } as never), InvalidArgument);
});
