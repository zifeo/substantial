// `c.patched(id)`: new code branches on a patch without a new version, its answer recorded. The
// ports of `python/tests/test_patched.py` under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { withEnv } from "./testing_helpers.ts";

const script = { preset: "script" } as const;

/** Waits for `a`, steps, waits for `b`; patched, it asks for a patch after each wait. */
function patching(patched: boolean) {
  return s.workflow("patching", async (c) => {
    const seen: boolean[] = [];
    await c.receive("a");
    if (patched) seen.push(c.patched("fix-1"));
    await c.step("x", () => 1);
    await c.receive("b");
    if (patched) seen.push(c.patched("fix-2"));
    return seen;
  });
}

test(
  "a patch is true live and on replay and false in an older history",
  withEnv(async (env) => {
    let older: s.Run<boolean[]>;
    {
      await using _worker = env.worker([patching(false)], script);
      older = await env.start(patching(false), undefined, { id: "older" });
      await older.send("a");
      await env.settle();
    }
    await using _worker = env.worker([patching(true)], script);
    const newer = await env.start(patching(true), undefined, { id: "newer" });
    await newer.send("a");
    await env.settle();
    for (const run of [older, newer]) await run.send("b");
    // § hbsrjm0w2@sj504
    assert.deepEqual(await older.result(), [false, true], "past its point, then live");
    assert.deepEqual(await newer.result(), [true, true], "recorded, then live");
    const patches = (await newer.history()).filter((e) => e.kind === "patched").map((e) => e.data);
    assert.deepEqual(patches, [{ id: "fix-1" }, { id: "fix-2" }]);
    // § end hbsrjm0w2
  }),
);

const misnamed = s.workflow("misnamed", async (c) => {
  try {
    c.patched("Not a label");
  } catch (e) {
    if (e instanceof s.InvalidArgument) return e.message;
    throw e;
  }
  return "accepted";
});

test(
  "a patch id outside the rules is refused",
  withEnv(async (env) => {
    await using _worker = env.worker([misnamed], script);
    const run = await env.start(misnamed);
    // § hbsrjm0w2.a@rtskn
    const refused = await run.result();
    assert.match(refused, /patch id "Not a label" is invalid/);
    // § end hbsrjm0w2.a
  }),
);
