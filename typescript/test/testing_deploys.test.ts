// The test environment of docs/testing.md: deploys, cancels and messages landing with a timer.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { withEnv } from "./testing_helpers.ts";

const service = { preset: "service" } as const;

function reminder(suffix: string) {
  return async (c: s.Context, { account }: { account: string }) => {
    await c.sleep(s.days(30));
    return `reminded ${account}${suffix}`;
  };
}

const remind = s.workflow("remind", reminder(""));
const remindV2 = s.workflow("remind", reminder(", v2"), { version: 2 });

test(
  "a deploy mid wait keeps old runs on old code",
  withEnv(async (env) => {
    let old: s.Run<string>;
    {
      await using _v1 = env.worker([remind], service);
      old = await env.start(remind, { account: "acc-1" });
      await env.advance(s.days(10));
    }
    await using _v2 = env.worker([remind, remindV2], service);
    const fresh = await env.start(remindV2, { account: "acc-2" });
    await env.advance(s.days(30));
    assert.equal(await old.result(), "reminded acc-1");
    assert.equal(await fresh.result(), "reminded acc-2, v2");
  }),
);

test(
  "a cancel landing with the timer",
  withEnv(async (env) => {
    await using _worker = env.worker([remind], service);
    await using client = await env.client();
    const own = await env.start(remind, { account: "acc-1" });
    const other = await env.start(remind, { account: "acc-2" });
    await env.advance(s.milliseconds(s.days(30).ms - 1));
    await own.cancel("account deleted");
    await client.run(remind, other.chain).cancel("account deleted");
    await env.advance(s.minutes(1));
    assert.equal(await own.status(), "cancelled");
    // The cancel from another pod reaches the store first; the replay at the timer sees it.
    assert.equal(await other.status(), "cancelled");
  }),
);

test(
  "a message to a run that just ended",
  withEnv(async (env) => {
    await using _worker = env.worker([remind], service);
    const run = await env.start(remind, { account: "acc-1" });
    await env.advance(s.days(30));
    await assert.rejects(run.send("upgraded", null, { id: "upgrade-acc-1" }), s.RunEnded);
    assert.equal(await run.result(), "reminded acc-1");
  }),
);
