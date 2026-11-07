// A step's timeout, from the step, its workflow or its worker (I-18), and the cause a StepFailed
// holds (I-20).
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { create } from "@bufbuild/protobuf";
import { ProtocolError } from "../src/errors.ts";
import {
  type Context,
  InvalidArgument,
  milliseconds,
  Permanent,
  StepFailed,
  Substantial,
  seconds,
  workflow,
} from "../src/mod.ts";
import { jsonValue, layout, MAX_VALUE } from "../src/proto.ts";
import { errorFromFailure } from "../src/scheduler.ts";
import { LONG, script, token } from "./helpers.ts";

class CardDeclined extends Permanent {}

/** Runs a step of 300 ms with `timeout`: "ok", or its StepFailed cause as `type retryable`. */
async function slow(c: Context, timeout?: number): Promise<string> {
  try {
    const options = timeout === undefined ? {} : { timeout: milliseconds(timeout) };
    return await c.step("slow", () => sleep(300).then(() => "ok"), options);
  } catch (e) {
    if (e instanceof StepFailed) return `${e.cause.type} ${e.cause.retryable}`;
    throw e;
  }
}

test("a step's own timeout wins, then its workflow's, then its worker's", async () => {
  const sub = await Substantial.create(`memory://steps-${token()}`, { partitions: 1 });
  const fromWorker = workflow("from-worker", async (c: Context) => slow(c));
  const fromWorkflow = workflow("from-workflow", async (c: Context) => slow(c), {
    stepTimeout: seconds(5),
  });
  const ownLarger = workflow("own-larger", async (c: Context) => slow(c, 5000), {
    stepTimeout: milliseconds(50),
  });
  const ownSmaller = workflow("own-smaller", async (c: Context) => slow(c, 50), {
    stepTimeout: seconds(5),
  });
  const served = [fromWorker, fromWorkflow, ownLarger, ownSmaller];
  await using _ = script(sub, served, { stepTimeout: milliseconds(50) });
  const results = await Promise.all(
    served.map(async (w) => (await sub.start(w)).result({ timeout: LONG })),
  );
  assert.deepEqual(results, ["StepTimeout true", "ok", "ok", "StepTimeout true"]);
});

test("a workflow's stepTimeout is a positive Duration", () => {
  for (const stepTimeout of [milliseconds(0), 5]) {
    assert.throws(
      () => workflow("w", async () => 1, { stepTimeout } as never),
      InvalidArgument,
      String(stepTimeout),
    );
  }
});

test("StepFailed holds the failure of the step's last attempt", async () => {
  const sub = await Substantial.create(`memory://steps-${token()}`, { partitions: 1 });
  const causes = workflow("causes", async (c: Context) => {
    const seen: unknown[] = [];
    const steps: [string, () => unknown, { timeout?: ReturnType<typeof milliseconds> }][] = [
      ["big", () => "x".repeat(MAX_VALUE - 1), {}],
      ["late", () => sleep(300), { timeout: milliseconds(20) }],
      [
        "declined",
        () => {
          throw new CardDeclined("card declined");
        },
        {},
      ],
    ];
    for (const [name, fn, options] of steps) {
      try {
        await c.step(name, fn, options);
      } catch (e) {
        if (!(e instanceof StepFailed)) throw e;
        seen.push([e.failure.type, e.cause.type, e.cause.retryable]);
      }
    }
    return seen;
  });
  await using _ = script(sub, [causes]);
  assert.deepEqual(await (await sub.start(causes)).result({ timeout: LONG }), [
    ["StepFailed", "ValueTooLarge", false],
    ["StepFailed", "StepTimeout", true],
    ["StepFailed", "CardDeclined", false],
  ]);
});

test("a StepFailed without the failure it wraps is the core's bug", () => {
  const failure = (details?: unknown) =>
    create(layout.FailureSchema, {
      type: "StepFailed",
      message: "step s failed after 1 attempt: no",
      ...(details === undefined
        ? {}
        : { details: jsonValue(new TextEncoder().encode(JSON.stringify(details))) }),
    });
  assert.ok(errorFromFailure(failure()) instanceof ProtocolError);
  assert.ok(errorFromFailure(failure({ step: "s", attempts: 1 })) instanceof ProtocolError);
  const e = errorFromFailure(
    failure({ step: "s", attempts: 1, error: { type: "E", message: "no", retryable: true } }),
  );
  assert.ok(e instanceof StepFailed);
  assert.deepEqual(e.cause, { type: "E", message: "no", retryable: true });
});
