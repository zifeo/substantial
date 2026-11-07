// Workers under pressure: concurrency, shutdown, timers during steps, and two workers racing.
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { createOn, openOn } from "../src/client.ts";
import {
  type Context,
  milliseconds,
  step,
  stepSignal,
  type Workflow,
  workflow,
} from "../src/mod.ts";
import { eachAdapter, eventually, LONG, Recording, script } from "./helpers.ts";

const FAST = { claimTtl: milliseconds(200), claimSkew: milliseconds(0) };

/** A step sleeping `ms(attempt)` that counts its attempts and the most in flight at once. */
class Probe {
  attempts = 0;
  inFlight = 0;
  most = 0;
  readonly workflow: Workflow<Record<string, never>, number>;
  readonly #abort = new AbortController();

  constructor(ms: (attempt: number) => number) {
    const probe = step("probe", async () => {
      const attempt = ++this.attempts;
      this.inFlight += 1;
      this.most = Math.max(this.most, this.inFlight);
      try {
        await sleep(ms(attempt), undefined, { signal: this.#abort.signal }).catch(() => {});
      } finally {
        this.inFlight -= 1;
      }
      return attempt;
    });
    this.workflow = workflow("probed", async () => await probe());
  }

  /** Ends the attempts still sleeping, so that the test process exits. */
  [Symbol.dispose](): void {
    this.#abort.abort();
  }
}

eachAdapter("concurrency bounds the replays at once", async (store, adapter) => {
  const concurrency = adapter === "memory" ? 1 : 2;
  using probe = new Probe(() => 100);
  const sub = await createOn(store, { partitions: 2 });
  const runs = [];
  for (let i = 0; i < 4; i++) runs.push(await sub.start(probe.workflow));
  {
    await using _ = script(sub, [probe.workflow], { concurrency });
    for (const run of runs) await run.result({ timeout: LONG });
  }
  assert.equal(probe.most, concurrency);
  assert.equal(probe.attempts, 4);
});

eachAdapter("shutdown lets a step end within the grace", async (store) => {
  using probe = new Probe(() => 300);
  const sub = await createOn(store, { partitions: 1 });
  let run: Awaited<ReturnType<typeof sub.start<Record<string, never>, number>>>;
  {
    await using _ = script(sub, [probe.workflow]);
    run = await sub.start(probe.workflow);
    await eventually("the step", () => probe.attempts === 1);
    assert.equal(await run.status(), "running");
  }
  {
    await using _ = script(sub, [probe.workflow]);
    assert.equal(await run.result({ timeout: LONG }), 1);
  }
  assert.equal(probe.attempts, 1, "the step ended within the grace");
});

eachAdapter("shutdown past the grace loses the step in flight", async (store) => {
  using probe = new Probe((attempt) => (attempt === 1 ? 30_000 : 10));
  const sub = await createOn(store, { partitions: 1 });
  const settings = { shutdownGrace: milliseconds(100), ...FAST };
  const worker = script(sub, [probe.workflow], settings);
  const run = await sub.start(probe.workflow);
  await eventually("the step", () => probe.attempts === 1);
  const began = performance.now();
  await worker.shutdown();
  assert.ok(performance.now() - began < 5000);
  await using _ = script(sub, [probe.workflow], settings);
  assert.equal(await run.result({ timeout: LONG }), 2);
});

eachAdapter("a shutdown past its grace interrupts", async (store) => {
  const reached: string[] = [];
  const slow = step("slow", async () => {
    reached.push("started");
    try {
      await sleep(1000, undefined, { signal: stepSignal() });
    } catch (e) {
      reached.push("interrupted");
      throw e;
    }
    reached.push("after the await");
  });
  const slowly = workflow("slowly", async () => await slow());
  const sub = await createOn(store, { partitions: 1 });
  const worker = script(sub, [slowly], { shutdownGrace: milliseconds(100) });
  await sub.start(slowly);
  await eventually("the step", () => reached.length === 1);
  await worker.shutdown();
  await sleep(1200);
  assert.deepEqual(reached, ["started", "interrupted"]);
});

eachAdapter("a timer fires while a step is in flight", async (store) => {
  const waited: number[] = [];
  const slow = step("slow", () => sleep(1500).then(() => null));
  const racing = workflow("racing", async (c: Context) => {
    const started = performance.now();
    const wait = async () => {
      await c.sleep(milliseconds(100));
      waited.push(performance.now() - started);
    };
    await Promise.all([slow(), wait()]);
    return true;
  });
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [racing]);
  const run = await sub.start(racing);
  assert.equal(await run.result({ timeout: LONG }), true);
  assert.ok(waited.length > 0 && (waited[0] as number) < 1000, `the sleep ended after ${waited}`);
});

eachAdapter("two workers racing for a commit", async (store) => {
  const started = Promise.withResolvers<void>();
  let attempts = 0;
  const work = step("work", async () => {
    started.resolve();
    return ++attempts;
  });
  const raced = workflow("raced", async () => await work());
  const slow = new Recording(store);
  slow.freezeOnSegment = 1500;
  const subA = await createOn(slow, { partitions: 1 });
  const subB = await openOn(store);
  await using _a = script(subA, [raced], FAST);
  const run = await subA.start(raced);
  await started.promise;
  await using _b = script(subB, [raced], FAST);
  const value = await subB.run(raced, run.chain).result({ timeout: LONG });
  assert.equal(value, 2, "the second worker's attempt won");
  await eventually("the frozen commit", () =>
    slow.created.some(([k, c]) => k.includes("/log/") && c === "exists"),
  );
});

eachAdapter(
  "the code runs in its replay's context and a step outside it",
  async (store) => {
    const { current } = await import("../src/scheduler.ts");
    const seen: boolean[] = [];
    const where = step("where", () => {
      seen.push(current.getStore() !== undefined);
      return null;
    });
    const located = workflow("located", async () => {
      seen.push(current.getStore() !== undefined);
      await where();
      seen.push(current.getStore() !== undefined);
    });
    const sub = await createOn(store, { partitions: 1 });
    await using _ = script(sub, [located]);
    await (await sub.start(located)).result({ timeout: LONG });
    assert.deepEqual(seen, [true, false, true]);
  },
  ["memory"],
);
