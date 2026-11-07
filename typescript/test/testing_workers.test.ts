// The test environment of docs/testing.md: workers and steps, crashes, pauses, holds.
import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";
import { setImmediate, setTimeout as sleep } from "node:timers/promises";
import * as s from "../src/mod.ts";
import { iso, T0, withEnv } from "./testing_helpers.ts";

// The PSP's charges and the ledger's ops, as the fakes of a shop would record them.
const charges: string[] = [];
const applied: string[] = [];
const inFlight = [0, 0];

beforeEach(() => {
  charges.length = 0;
  applied.length = 0;
  inFlight.splice(0, 2, 0, 0);
});

const service = { preset: "service" } as const;
type Order = { order: string };

async function charge(order: string): Promise<string> {
  charges.push(order);
  return `pay-${order}`;
}

const checkout = s.workflow("checkout", async (c, { order }: Order) => {
  const payment = await c.step("charge", () => charge(order));
  return { payment, at: (await c.now()).toISOString() };
});

const retrying = s.workflow("retrying", async (c, { order }: Order) => {
  const once = { maxAttempts: 3, initial: s.seconds(1), max: s.seconds(1), multiplier: 1 };
  return await c.step("charge", () => charge(order), { retry: once });
});

async function apply(op: string): Promise<void> {
  inFlight[0] = (inFlight[0] ?? 0) + 1;
  inFlight[1] = Math.max(inFlight[0], inFlight[1] ?? 0);
  await setImmediate();
  applied.push(op);
  inFlight[0] -= 1;
}

/** An actor: applies each op it is sent, in order, and publishes the balance after it. */
const account = s.workflow("account", async (c, _input: { customer: string }) => {
  const ops: { id: string }[] = [];
  c.handle<{ id: string }>("op", (op) => {
    ops.push(op);
  });
  let balance = 0;
  for (;;) {
    const done = balance;
    await c.waitUntil(() => ops.length > done);
    const id = ops[done]?.id as string;
    await c.step("apply", () => apply(id));
    balance += 1;
    c.publish(`op.${id}`, balance);
  }
});

/** A checkout whose charge first does `act` to the worker the test pushed, from inside its body,
 * then waits for `ms`. */
function acting(name: string, act: (worker: s.testing.EnvWorker) => void, ms: number) {
  const workers: s.testing.EnvWorker[] = [];
  const workflow = s.workflow(name, async (c, { order }: Order) => {
    return await c.step("charge", async () => {
      const worker = workers.pop();
      if (worker !== undefined) {
        act(worker);
        await sleep(ms, undefined, { signal: s.stepSignal() });
      }
      return await charge(order);
    });
  });
  return { workflow, workers };
}

// Interrupted at its wait: the crash abandoned this attempt.
const abandoning = acting("abandoning", (w) => w.crash(), 30_000);
const pausing = acting("pausing", (w) => w.pause(), 0);

test(
  "a crash after the charge before it is recorded",
  withEnv(async (env) => {
    await using _a = env.worker([checkout], service);
    await using _b = env.worker([checkout], service);
    const hold = env.hold("charge");
    const run = await env.start(checkout, { order: "a-42" });
    await env.settle();
    assert.equal(hold.held, 1);
    hold.crash();
    assert.equal(hold.held, 0);
    await env.advance(s.seconds(34));
    assert.deepEqual(charges, ["a-42"]);
    await env.advance(s.seconds(2));
    assert.equal((await run.result()).payment, "pay-a-42");
    assert.deepEqual(charges, ["a-42", "a-42"]);
  }),
);

test(
  "claims are renewed and taken over after the last renewal",
  withEnv(async (env) => {
    await using _a = env.worker([checkout], service);
    await using _b = env.worker([checkout], service);
    const hold = env.hold("charge");
    const run = await env.start(checkout, { order: "a-42" });
    // Renewed every 15 s, the claim outlives claimTtl while its worker holds the step.
    await env.advance(s.seconds(61));
    assert.deepEqual(charges, ["a-42"]);
    hold.crash();
    // The last renewal was at 60 s: the run is taken over 35 s later.
    await env.advance(s.seconds(34));
    assert.deepEqual(charges, ["a-42"]);
    await env.advance(s.seconds(1));
    assert.equal((await run.result()).at, "2030-01-07T09:01:35.001Z");
  }),
);

test(
  "a paused workers step body goes on to its end",
  withEnv(async (env) => {
    await using worker = env.worker([pausing.workflow], service);
    pausing.workers.push(worker);
    const run = await env.start(pausing.workflow, { order: "a-42" });
    await env.settle();
    assert.deepEqual(charges, ["a-42"]);
    assert.equal(await run.status(), "running");
    worker.resume();
    assert.equal(await run.result(), "pay-a-42");
  }),
);

test(
  "a crashed worker's step is interrupted",
  withEnv(async (env) => {
    await using first = env.worker([abandoning.workflow], service);
    abandoning.workers.push(first);
    const run = await env.start(abandoning.workflow, { order: "a-42" });
    const started = performance.now();
    await env.settle();
    assert.ok(performance.now() - started < 5000, "settle does not wait for an interrupted body");
    assert.ok(!first.alive);
    assert.equal(first.error, undefined);
    assert.deepEqual(charges, []);
    await using _second = env.worker([abandoning.workflow], service);
    await env.advance(s.minutes(1));
    assert.equal(await run.result(), "pay-a-42");
    assert.deepEqual(charges, ["a-42"]);
  }),
);

test(
  "two pods on one entity never overlap",
  withEnv(async (env) => {
    await using _a = env.worker([account], service);
    await using _b = env.worker([account], service);
    const run = await env.start(account, { customer: "cus-1" }, { id: "customer-cus-1" });
    const ids = Array.from({ length: 20 }, (_, i) => `op-${String(i).padStart(3, "0")}`);
    for (const id of ids) await run.send("op", { id }, { id });
    await env.advance(s.minutes(1));
    assert.equal(await run.read("op.op-019"), 20);
    assert.deepEqual(applied, ids);
    assert.equal(inFlight[1], 1);
  }),
);

test(
  "a paused pod does not apply an op twice",
  withEnv(async (env) => {
    await using paused = env.worker([account], service);
    const hold = env.hold("apply");
    const run = await env.start(account, { customer: "cus-1" }, { id: "customer-cus-1" });
    await run.send("op", { id: "op-1" }, { id: "op-1" });
    await env.settle();
    assert.equal(hold.held, 1);
    await using _other = env.worker([account], service);
    paused.pause();
    await env.advance(s.seconds(65));
    assert.equal(await run.read("op.op-1"), 1);
    paused.resume();
    hold.release();
    await env.settle();
    assert.equal(await run.read("op.op-1"), 1);
    assert.deepEqual(applied, ["op-1", "op-1"]);
  }),
);

test(
  "a pause shorter than the claim changes nothing",
  withEnv(async (env) => {
    await using paused = env.worker([account], service);
    const hold = env.hold("apply");
    const run = await env.start(account, { customer: "cus-1" }, { id: "customer-cus-1" });
    await run.send("op", { id: "op-1" }, { id: "op-1" });
    await env.settle();
    await using _other = env.worker([account], service);
    paused.pause();
    await env.advance(s.seconds(20));
    paused.resume();
    hold.release();
    await env.advance(s.minutes(1));
    assert.equal(await run.read("op.op-1"), 1);
    assert.deepEqual(applied, ["op-1"]);
  }),
);

test(
  "a shutdown leaves held steps and its runs to another worker",
  withEnv(async (env) => {
    await using leaving = env.worker([checkout], service);
    const hold = env.hold("charge");
    const run = await env.start(checkout, { order: "a-42" });
    await env.settle();
    await using _other = env.worker([checkout], service);
    await leaving.shutdown();
    assert.equal(hold.held, 0);
    assert.ok(!leaving.alive);
    // Its claims are deleted: the other worker takes the run at its next scan, at 30 s.
    await env.advance(s.seconds(30));
    assert.deepEqual(await run.result(), { payment: "pay-a-42", at: iso(30_000) });
    assert.deepEqual(charges, ["a-42", "a-42"]);
  }),
);

test("a held attempt released before its timeout counts once", async () => {
  for (const [early, attempts] of [
    [1, 1],
    [0, 2],
  ] as const) {
    charges.length = 0;
    await using env = await s.testing.Env.open({ now: T0 });
    await using _worker = env.worker([retrying], service);
    const hold = env.hold("charge");
    const run = await env.start(retrying, { order: "a-42" });
    await env.advance(s.milliseconds(s.minutes(5).ms - early));
    // At 5:00 the attempt has timed out: the release comes too late.
    assert.equal(hold.held, early ? 1 : 0);
    hold.release();
    await env.advance(s.seconds(1));
    assert.equal(await run.result(), "pay-a-42");
    assert.equal(charges.length, attempts);
  }
});

const refunded = s.workflow("refunded", async (c, { order }: Order) => {
  try {
    await c.saga(async () => {
      await c.step("charge", () => charge(order), {
        undo: (payment) => {
          charges.push(`refund ${payment}`);
        },
      });
      await c.step("ship", () => {
        throw new s.Permanent("no carrier");
      });
    });
  } catch (e) {
    if (e instanceof s.StepFailed) return "refunded";
    throw e;
  }
  return "shipped";
});

test(
  "a hold takes the next attempts and undos",
  withEnv(async (env) => {
    await using _worker = env.worker([refunded], service);
    const charged = env.hold("charge", { times: 2 });
    const undone = env.hold("charge.undo");
    const first = await env.start(refunded, { order: "a" });
    const second = await env.start(refunded, { order: "b" });
    await env.settle();
    assert.equal(charged.held, 2);
    assert.equal(undone.held, 0);
    charged.release();
    await env.settle();
    assert.equal(undone.held, 1);
    undone.release();
    assert.equal(await first.result(), "refunded");
    assert.equal(await second.result(), "refunded");
    assert.deepEqual(charges.sort(), ["a", "b", "refund pay-a", "refund pay-b"]);
  }),
);

test(
  "a redelivered webhook applies once",
  withEnv(async (env) => {
    await using _worker = env.worker([account], service);
    const run = await env.start(account, { customer: "cus-1" }, { id: "customer-cus-1" });
    const again = await env.start(account, { customer: "cus-1" }, { id: "customer-cus-1" });
    assert.equal(run.chain, again.chain);
    for (let i = 0; i < 2; i++) await again.send("op", { id: "op-1" }, { id: "evt-1" });
    await env.advance(s.minutes(1));
    assert.deepEqual(applied, ["op-1"]);
  }),
);

test(
  "a worker reports whether it is alive",
  withEnv(async (env) => {
    // A TypeScript worker runs from its creation.
    const worker = env.worker([checkout], service);
    assert.ok(worker.alive);
    assert.equal(worker.error, undefined);
    await worker.shutdown();
    assert.ok(!worker.alive);
    assert.equal(worker.error, undefined);
  }),
);

const paused: s.testing.EnvWorker[] = [];

const napper = s.workflow("napper", async (c) => {
  await c.sleep(s.seconds(1));
  return "woke";
});

const resumer = s.workflow("resumer", async (c) =>
  c.step("resume", () => {
    paused[0]?.resume();
    return "resumed";
  }),
);

test(
  "a resume during a settle does not hang it",
  withEnv(async (env) => {
    await using napping = env.worker([napper], service);
    await using _resuming = env.worker([resumer], service);
    paused.splice(0, paused.length, napping);
    const nap = await env.start(napper, {}, { id: "nap" });
    await env.settle();
    napping.pause();
    await env.advance(s.seconds(2));
    await env.start(resumer, {}, { id: "res" });
    // The step body resumes the paused worker while the settle runs.
    let timer: ReturnType<typeof setTimeout> | undefined;
    const late = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error("the settle hung")), 5000);
    });
    await Promise.race([env.settle(), late]);
    clearTimeout(timer);
    assert.equal(await nap.result(), "woke");
  }),
);
