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
  RunFailed,
  StepFailed,
  Substantial,
  seconds,
  stepSignal,
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

test("a failed run holds the cause of the step that failed it", async () => {
  const sub = await Substantial.create(`memory://causes-${token()}`, { partitions: 1 });
  const paying = workflow("paying", async (c: Context) => {
    await c.step("charge", () => {
      throw new CardDeclined("card declined");
    });
  });
  const refusing = workflow("refusing", async (_c: Context) => {
    throw new CardDeclined("not today");
  });
  const ordering = workflow("ordering", async (c: Context) => {
    const child = await c.start(paying);
    const e = await child.result().catch((e) => e);
    if (!(e instanceof RunFailed) || e.cause === undefined) throw new Error(`${e}`);
    return [e.error.type, e.cause.type, e.cause.message, e.cause.retryable];
  });
  await using _ = script(sub, [paying, refusing, ordering]);
  const paid = await (await sub.start(paying)).result({ timeout: LONG }).catch((e) => e);
  assert.ok(paid instanceof RunFailed, String(paid));
  assert.equal(paid.error.type, "StepFailed");
  assert.deepEqual(paid.cause, {
    type: "CardDeclined",
    message: "card declined",
    retryable: false,
  });
  const refused = await (await sub.start(refusing)).result({ timeout: LONG }).catch((e) => e);
  assert.ok(refused instanceof RunFailed, String(refused));
  assert.equal(refused.error.type, "CardDeclined");
  assert.equal(refused.cause, undefined);
  assert.deepEqual(await (await sub.start(ordering)).result({ timeout: LONG }), [
    "StepFailed",
    "CardDeclined",
    "card declined",
    false,
  ]);
  await sub.close();
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

test("a timed-out async step is interrupted", async () => {
  const reached: string[] = [];
  const sub = await Substantial.create(`memory://hang-${token()}`, { partitions: 1 });
  const hanging = workflow("hanging", async (c: Context) => {
    try {
      await c.step(
        "hang",
        async () => {
          try {
            await sleep(500, undefined, { signal: stepSignal() });
          } catch (e) {
            reached.push("interrupted");
            throw e;
          }
          reached.push("after the await");
        },
        { timeout: milliseconds(200) },
      );
    } catch (e) {
      if (e instanceof StepFailed) return e.cause.type;
      throw e;
    }
    return "returned";
  });
  {
    await using _ = script(sub, [hanging]);
    assert.equal(await (await sub.start(hanging)).result({ timeout: LONG }), "StepTimeout");
  }
  await sleep(600);
  assert.deepEqual(reached, ["interrupted"]);
  await sub.close();
});

test("an uninterruptible step runs to its end with its outcome dropped", async () => {
  const finished: string[] = [];
  const sub = await Substantial.create(`memory://block-${token()}`, { partitions: 1 });
  const blocking = workflow("blocking", async (c: Context) => {
    return await c.step(
      "block",
      async () => {
        await sleep(400);
        finished.push("ended");
        return "late";
      },
      { timeout: milliseconds(200) },
    );
  });
  let failed: unknown;
  {
    await using _ = script(sub, [blocking]);
    failed = await (await sub.start(blocking)).result({ timeout: LONG }).catch((e) => e);
  }
  assert.ok(failed instanceof RunFailed, String(failed));
  assert.equal(failed.cause?.type, "StepTimeout");
  await sleep(600);
  assert.deepEqual(finished, ["ended"]);
  await sub.close();
});
