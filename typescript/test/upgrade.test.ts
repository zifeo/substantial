// A loop that upgrades continues into the highest version of its workflow its worker serves, and
// `sub.schedule` with a newer version moves a running schedule to it. The ports of
// `python/tests/test_upgrade.py` under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { T0, withEnv } from "./testing_helpers.ts";

const script = { preset: "script" } as const;
const service = { preset: "service" } as const;

/** Each iteration notes its version, then waits for `next`, three times. */
function looping(version: number, upgrade: boolean) {
  return s.workflow(
    "looping",
    async (c) => {
      let last: { versions: number[] } = { versions: [] };
      for await (const st of c.iterate({ versions: [] as number[] }, { upgrade })) {
        st.versions.push(version);
        last = st;
        if (st.versions.length === 3) break;
        await c.receive("next");
      }
      return last;
    },
    { version },
  );
}

async function versionsOf(upgrade: boolean): Promise<number[]> {
  await using env = await s.testing.Env.open({ now: T0 });
  let run: s.Run<{ versions: number[] }>;
  {
    await using _worker = env.worker([looping(1, upgrade)], script);
    run = await env.start(looping(1, upgrade));
    await env.settle();
  }
  await using _worker = env.worker([looping(1, upgrade), looping(2, upgrade)], script);
  await run.send("next");
  await env.settle();
  const [info] = await Array.fromAsync(env.runs(looping(1, upgrade)));
  await run.send("next");
  const result = await run.result();
  await env.check();
  return [info?.version as number, ...result.versions];
}

test("a loop that upgrades continues into the highest version served", async () => {
  // § zsyv7772c@e6nmf
  assert.deepEqual(await versionsOf(true), [2, 1, 2, 2]);
  // § end zsyv7772c
  assert.deepEqual(await versionsOf(false), [1, 1, 1, 1]);
});

function ticking(version: number) {
  return s.workflow(
    "ticking",
    async (c) => {
      for await (const _ of c.schedule({ upgrade: true })) {
        await c.step("tick", () => version);
      }
    },
    { version },
  );
}

test(
  "a schedule of a newer version moves the running one",
  withEnv(async (env) => {
    const hourly = new s.Cron("0 * * * *", "UTC");
    let run: s.Run<void>;
    {
      await using _worker = env.worker([ticking(1)], service);
      run = await env.schedule(ticking(1), "tick", hourly, { revision: 1 });
      await env.settle();
    }
    await using _worker = env.worker([ticking(1), ticking(2)], service);
    // § sfzkmddwj@fszcz
    const moved = await env.schedule(ticking(2), "tick", hourly, { revision: 2 });
    assert.equal(moved.chain, run.chain);
    await env.advance(s.hours(1));
    const [info] = await Array.fromAsync(env.runs(ticking(2)));
    assert.equal(info?.version, 2);
    // § end sfzkmddwj
    assert.equal(((await run.read("schedule")) as { revision: number }).revision, 2);
  }),
);
