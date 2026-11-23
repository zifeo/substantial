// `Retry.deadline`: no attempt of a step starts past its deadline, counted from its first
// attempt's start. The ports of `python/tests/test_step_deadline.py` under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { withEnv } from "./testing_helpers.ts";

function retry(deadline: unknown): s.Retry {
  const one = s.seconds(1);
  return {
    maxAttempts: 10,
    initial: one,
    max: one,
    multiplier: 1,
    deadline: deadline as s.Duration,
  };
}

const flaky = s.step(
  "flaky",
  async () => {
    throw new Error("down");
  },
  { retry: retry(s.milliseconds(2500)) },
);

const calling = s.workflow("calling", async () => {
  try {
    await flaky();
  } catch (e) {
    if (e instanceof s.StepFailed) return e.message;
    throw e;
  }
  return "ok";
});

test(
  "no attempt starts past the deadline",
  withEnv(async (env) => {
    await using _worker = env.worker([calling], { preset: "script" });
    const run = await env.start(calling);
    await env.advance(s.seconds(10));
    // § s0a5x1qed@naqxp
    assert.match(await run.result(), /after 3 attempts/);
    const retries = (await run.history()).filter((e) => e.kind === "step_retry").map((e) => e.data);
    assert.equal(retries.length, 2, JSON.stringify(retries));
    assert.ok(
      retries.every((r) => "deadline" in r),
      JSON.stringify(retries),
    );
    // § end s0a5x1qed
  }),
);

test("a deadline is a positive duration", () => {
  // § s0a5x1qed.a@d6mdg
  for (const deadline of [s.milliseconds(0), 5, "1s"]) {
    assert.throws(
      () => s.step("refused", () => null, { retry: retry(deadline) }),
      s.InvalidArgument,
      String(deadline),
    );
  }
  // § end s0a5x1qed.a
});
