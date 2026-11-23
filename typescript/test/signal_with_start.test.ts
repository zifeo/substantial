// `sub.start(..., { message })`: a start that sends its run a message once it is created or
// found. The ports of `python/tests/test_signal_with_start.py` under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { withEnv } from "./testing_helpers.ts";

const collecting = s.workflow("collecting", async (c, { n }: { n: number }) => {
  const got: string[] = [];
  for (let i = 0; i < n; i++) got.push(await c.receive<string>("add"));
  return got;
});

test(
  "a start with a message sends it once the run is created or found",
  withEnv(async (env) => {
    await using _worker = env.worker([collecting], { preset: "script" });
    // § fqv5ts8vg@xv1ft
    const first: s.Message = { name: "add", value: "a", id: "m-a" };
    const run = await env.start(collecting, { n: 2 }, { id: "cart", message: first });
    await env.start(collecting, { n: 2 }, { id: "cart", message: first });
    await env.start(collecting, { n: 2 }, { id: "cart", message: { name: "add", value: "b" } });
    assert.deepEqual(await run.result(), ["a", "b"], "the same id delivered once");
    // § end fqv5ts8vg
    // § fqv5ts8vg.a@8b0h4
    const late = env.start(
      collecting,
      { n: 2 },
      { id: "cart", message: { name: "add", value: "c" } },
    );
    await assert.rejects(late, s.RunEnded);
    // § end fqv5ts8vg.a
  }),
);

test(
  "a start refuses a message outside the rules",
  withEnv(async (env) => {
    // § fqv5ts8vg.a@nzfw7
    for (const message of [{ name: "substantial.x" }, { name: "add", id: "Upper" }, "add"]) {
      await assert.rejects(
        env.start(collecting, { n: 1 }, { message: message as s.Message }),
        s.InvalidArgument,
        JSON.stringify(message),
      );
    }
    // § end fqv5ts8vg.a
    await assert.rejects(
      env.start(collecting, { n: 1 }, { message: { name: "add", value: new Map() } }),
      (e) => e instanceof s.InvalidArgument && /JSON/.test(e.message),
    );
  }),
);
