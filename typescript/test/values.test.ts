// The cap on a value's JSON (D1): refused before it reaches the core or the store.
import assert from "node:assert/strict";
import { test } from "node:test";
import { createOn } from "../src/client.ts";
import {
  type Context,
  InvalidArgument,
  RunFailed,
  StepFailed,
  Substantial,
  ValueTooLarge,
  workflow,
} from "../src/mod.ts";
import { MAX_VALUE } from "../src/proto.ts";
import { MemoryStore } from "../src/stores/memory.ts";
import { LONG, Recording, script, token } from "./helpers.ts";

// Its JSON is the string and its two quotes: one byte over the cap.
const over = () => "x".repeat(MAX_VALUE - 1);
const at = () => "x".repeat(MAX_VALUE - 2);

const LIMIT = new RegExp(`is \\d+ bytes of JSON, over the limit of ${MAX_VALUE} bytes$`);

function tooLarge(e: unknown): boolean {
  return e instanceof ValueTooLarge && LIMIT.test(e.message);
}

test("a client refuses a value over the cap before any store operation", async () => {
  const recording = new Recording(new MemoryStore(`values-${token()}`));
  const sub = await createOn(recording);
  const echo = workflow("echo", async (_c: Context, input: { s: string }) => input.s.length);
  const [created, listed] = [recording.created.length, recording.listed.length];
  await assert.rejects(sub.start(echo, { s: over() }), tooLarge);
  const run = sub.run(echo, "r");
  await assert.rejects(run.send("note", over()), tooLarge);
  assert.equal(recording.created.length, created, "nothing was written");
  assert.equal(recording.listed.length, listed, "nothing was listed");
});

test("workflow values over the cap fail, the core never sees them", async () => {
  const sub = await Substantial.create(`memory://values-${token()}`);
  const big = workflow("big-step", async (c: Context) => {
    try {
      await c.step("big", () => over());
    } catch (e) {
      if (e instanceof StepFailed) return [e.cause.type, e.message];
      throw e;
    }
    return "accepted";
  });
  const largest = workflow("largest-step", async (c: Context) => {
    return (await c.step("largest", () => at())).length;
  });
  const result = workflow("big-result", async () => over());
  const published = workflow("big-publish", async (c: Context) => {
    try {
      c.publish("big", over());
    } catch (e) {
      if (tooLarge(e)) return "refused";
      throw e;
    }
    return "accepted";
  });
  const looping = workflow("big-state", async (c: Context) => {
    const s = { big: "" };
    for await (const _ of c.iterate(s)) {
      if (s.big !== "") return "continued";
      s.big = over();
    }
  });
  const served = [big, largest, result, published, looping];
  await using _ = script(sub, served, { continueAsNewAfter: 0 });
  const [cause, message] = (await (await sub.start(big)).result({ timeout: LONG })) as string[];
  assert.equal(cause, "ValueTooLarge");
  assert.match(String(message), /^step big failed after 1 attempt: the step's value is /);
  assert.match(String(message), LIMIT);
  assert.equal(await (await sub.start(largest)).result({ timeout: LONG }), MAX_VALUE - 2);
  const failed = await (await sub.start(result)).result({ timeout: LONG }).catch((e) => e);
  assert.ok(failed instanceof RunFailed, String(failed));
  assert.equal(failed.error.type, "ValueTooLarge");
  assert.equal(await (await sub.start(published)).result({ timeout: LONG }), "refused");
  const state = await (await sub.start(looping)).result({ timeout: LONG }).catch((e) => e);
  assert.ok(state instanceof RunFailed, String(state));
  assert.equal(state.error.type, "ValueTooLarge");
});

// What JSON.stringify would write as another value: a Date through its toJSON, a Map as {},
// undefined dropped from an object or written null in an array, a hole of a sparse array as
// null, a NaN as null.
const NOT_JSON = () => [
  new Date(0),
  { at: new Date(0) },
  new Map([["a", 1]]),
  { x: undefined },
  [1, undefined],
  new Array(2),
  { list: Object.assign([], { 1: "b" }) },
  { x: Number.NaN },
  { n: 2n ** 53n },
];

test("a value JSON cannot hold is refused, never written as another", async () => {
  const sub = await Substantial.create(`memory://values-${token()}`);
  const holding = workflow("holding", async (c: Context, input: { i: number }) => {
    const bad = NOT_JSON()[input.i];
    assert.throws(() => c.publish("k", bad), InvalidArgument);
    const stepped = await c.step("value", () => bad).catch((e) => e);
    assert.ok(stepped instanceof StepFailed, String(stepped));
    assert.equal(stepped.cause.type, "Json");
    return bad;
  });
  const run = await sub.start(holding, { i: 0 });
  for (const bad of NOT_JSON()) {
    await assert.rejects(run.send("note", bad), (e) => e instanceof InvalidArgument);
  }
  await using _ = script(sub, [holding]);
  for (let i = 0; i < NOT_JSON().length; i++) {
    const failed = await (await sub.start(holding, { i }))
      .result({ timeout: LONG })
      .catch((e) => e);
    assert.ok(failed instanceof RunFailed, String(failed));
    assert.equal(failed.error.type, "Json", failed.error.message);
  }
});
