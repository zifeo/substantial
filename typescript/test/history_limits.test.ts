// A run's history limits: passing one ends the run failed with `HistoryTooLarge`, and a commit
// past their warning share says so once. The ports of `python/tests/test_history_limits.py`
// under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { LONG, script, token } from "./helpers.ts";

const clocking = s.workflow("clocking", async (c) => {
  try {
    for (let i = 0; i < 100; i++) await c.now();
  } catch (e) {
    if (e instanceof s.HistoryTooLarge) return "caught";
    throw e;
  }
  return "done";
});

test("a run past a limit of its history ends failed", async () => {
  const told: s.Observed[] = [];
  const warnings: string[] = [];
  const warn = console.warn;
  console.warn = (...data: unknown[]) => warnings.push(data.join(" "));
  try {
    const sub = await s.Substantial.create(`memory://limits-${token()}`, {
      observe: (e) => told.push(e),
    });
    await using _ = script(sub, [clocking], { maxHistoryEvents: 10, historyWarnPercent: 50 });
    const run = await sub.start(clocking, undefined, { id: "clock" });
    const ended = await run.result({ timeout: LONG }).then(
      () => undefined,
      (e: unknown) => e,
    );
    // § vkpvtd54g@1vk7z
    assert.ok(ended instanceof s.RunFailed, String(ended));
    const error = ended.error;
    assert.equal(error.type, "HistoryTooLarge", JSON.stringify(error));
    // § end vkpvtd54g
    // § vkpvtd54g.b@48fmp
    const details = error.details;
    assert.deepEqual(details, { limit: "history_events", used: "11", max: "10" });
    assert.equal(error.retryable, false);
    // § end vkpvtd54g.b
  } finally {
    console.warn = warn;
  }
  // Told once, by the commit that took the run past half of it, its end's here.
  const near = told.filter((e) => e instanceof s.NearLimit);
  assert.deepEqual(near, [new s.NearLimit("clocking", "clock", "history_events", 12, 10)]);
  assert.ok(
    warnings.includes("run clock near its history_events limit: 12 of 10"),
    String(warnings),
  );
});

const taking = s.workflow("taking", async (c) => {
  const got: number[] = [];
  c.handle("n", (n: number) => got.push(n));
  await c.waitUntil(() => got.length >= 5);
  return got.length;
});

test("a replay past its inbox limit ends failed", async () => {
  const sub = await s.Substantial.create(`memory://inbox-${token()}`);
  const run = await sub.start(taking);
  for (let n = 0; n < 5; n++) await run.send("n", n);
  await using _ = script(sub, [taking], { maxReplayInbox: 3 });
  await assert.rejects(run.result({ timeout: LONG }), (e) => {
    assert.ok(e instanceof s.RunFailed, String(e));
    assert.equal(e.error.details?.limit, "replay_inbox", JSON.stringify(e.error));
    return true;
  });
});
