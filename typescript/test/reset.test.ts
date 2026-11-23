// `run.retry({ keep })`: a reset of an ended run, which replays its first events and runs the
// rest again. The ports of `python/tests/test_reset.py` under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { withEnv } from "./testing_helpers.ts";

const script = { preset: "script" } as const;
const ran: string[] = [];

function note(name: string): string {
  ran.push(name);
  return name;
}

const ordered = s.workflow("ordered", async (c) => {
  const a = await c.step("a", () => note("a"));
  const go = await c.receive<string>("go");
  const b = await c.step("b", () => note("b"));
  return [a, go, b];
});

test(
  "a reset replays the first events of an ended run",
  withEnv(async (env) => {
    ran.length = 0;
    await using _worker = env.worker([ordered], script);
    const run = await env.start(ordered, undefined, { id: "o" });
    await env.settle();
    await run.send("go", "now");
    assert.deepEqual(await run.result(), ["a", "now", "b"]);
    const kinds = (await run.history()).map((e) => e.kind);
    assert.deepEqual(kinds, ["step", "message", "step", "completed"]);
    // § grzez328c@51gfk
    await run.retry({ keep: 1 });
    assert.deepEqual(
      await run.result(),
      ["a", "now", "b"],
      "the message after the point given back",
    );
    assert.deepEqual(ran, ["a", "b", "b"], "the kept step not again");
    // § end grzez328c
  }),
);

test(
  "a reset refuses a point past the end and a live run",
  withEnv(async (env) => {
    await using _worker = env.worker([ordered], script);
    const run = await env.start(ordered);
    await env.settle();
    // § grzez328c.a@c96tz
    const live = run.retry({ keep: 0 });
    await assert.rejects(live, s.InvalidArgument);
    await run.send("go", "now");
    await run.result();
    for (const keep of [99, -1, true, 1.5]) {
      await assert.rejects(run.retry({ keep: keep as number }), s.InvalidArgument, String(keep));
    }
    // § end grzez328c.a
  }),
);
