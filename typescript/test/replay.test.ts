// Replays of hand-built `RunInput`s, the test playing the agent machine's part in memory.
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { create } from "@bufbuild/protobuf";
import { CoreModule } from "../src/core.ts";
import {
  type Context,
  milliseconds,
  minutes,
  NotInWorkflow,
  Permanent,
  type Retry,
  StepFailed,
  seconds,
  step,
  stepKey,
  workflow,
} from "../src/mod.ts";
import { agent, decodeValue, jsonValue, layout, run, time } from "../src/proto.ts";
import { Channel, type ReplayEvent, replay } from "../src/replay.ts";
import { MemoryStore } from "../src/stores/memory.ts";
import type { Store } from "../src/stores/store.ts";
import type { AnyWorkflow } from "../src/workflow.ts";
import { Recording, token } from "./helpers.ts";

const encoder = new TextEncoder();
const core = (await CoreModule.shipped()).instantiate();

class Replayed {
  readonly commits: run.Commit[];
  readonly end: agent.ReplayEnd;

  constructor(commits: run.Commit[], end: agent.ReplayEnd) {
    this.commits = commits;
    this.end = end;
  }

  #events(): layout.Event[] {
    return this.commits.flatMap((c) => c.segment?.events ?? []);
  }

  events(): (string | undefined)[] {
    return this.#events().map((e) => e.kind.case);
  }

  kinds(): number[] {
    return this.commits.map((c) => c.kind);
  }

  nextWake(): number | undefined {
    assert.equal(this.end.end.case, "idle", "dropped");
    const wake = this.end.end.case === "idle" ? this.end.end.value.nextWake : undefined;
    return wake === undefined ? undefined : Number(wake.ms);
  }

  completed(): unknown {
    const e = this.#events().find((e) => e.kind.case === "completed");
    return e?.kind.case === "completed" ? decodeValue(e.kind.value.value) : undefined;
  }

  failed(): layout.Failure | undefined {
    const e = this.#events().find((e) => e.kind.case === "failed");
    return e?.kind.case === "failed" ? e.kind.value.error : undefined;
  }
}

/** An in-memory stand-in for the agent machine: it wins or loses every commit and keeps the
 * history and inbox a store would hold. */
class Harness {
  input = "{}";
  history: layout.Event[] = [];
  nextN = 0n;
  inbox: run.InboxMessage[] = [];
  now = 0;
  readonly workflow: AnyWorkflow;
  store: Store = new MemoryStore("replay-harness");

  constructor(workflow: AnyWorkflow) {
    this.workflow = workflow;
  }

  message(key: string, name: string, value: string): void {
    const body = {
      id: `id-${key}`,
      name,
      value: jsonValue(encoder.encode(value)),
      sentAt: time(0),
    };
    this.inbox.push(create(run.InboxMessageSchema, { key, body }));
  }

  runInput(): run.RunInput {
    return create(run.RunInputSchema, {
      run: "run-1",
      start: {
        workflow: this.workflow.name,
        input: jsonValue(encoder.encode(this.input)),
        chain: "run-1",
        createdAt: time(0),
      },
      history: this.history,
      nextN: this.nextN,
      inbox: this.inbox,
      now: time(this.now),
      seed: new Uint8Array(32),
      partitions: 1,
      worker: "test",
      settings: {
        continueAsNewAfterSegments: 1000n,
        continueAsNewAfterBytes: 8n << 20n,
        dedupWindowMs: 86_400_000n,
      },
    });
  }

  async replay(now: number, won = true): Promise<Replayed> {
    this.now = now;
    const channel = new Channel();
    const commits: run.Commit[] = [];
    const ended = Promise.withResolvers<agent.ReplayEnd>();
    const emit = (event: ReplayEvent) => {
      if (event.kind === "ended") {
        ended.resolve(event.end);
        return;
      }
      const c = event.commit;
      channel.committed(create(agent.AgentCommand_CommittedSchema, { n: c.segment?.n ?? 0n, won }));
      if (won) {
        this.history.push(...(c.segment?.events ?? []));
        this.nextN = (c.segment?.n ?? 0n) + 1n;
        this.inbox = this.inbox.filter((m) => !c.consumed.includes(m.key));
        commits.push(c);
      }
    };
    const host = { store: this.store, stepTimeout: minutes(5) };
    await replay(core, this.workflow, this.runInput(), () => time(this.now), emit, channel, host);
    return new Replayed(commits, await ended.promise);
  }
}

function counting<T>(name: string, value: T): { runs: number[]; counted: () => Promise<T> } {
  const runs: number[] = [];
  return {
    runs,
    counted: step(name, async () => {
      runs.push(1);
      return value;
    }),
  };
}

test("a step then the end", async () => {
  const { runs, counted: five } = counting("five", 5);
  const double = workflow("double", async () => (await five()) * 2);
  const replayed = await new Harness(double).replay(1_000);
  // The code ran on to its end before the next `next`: one commit records both.
  assert.deepEqual(replayed.kinds(), [run.CommitKind.END]);
  assert.deepEqual(replayed.events(), ["step", "completed"]);
  assert.equal(replayed.completed(), 10);
  assert.equal(replayed.nextWake(), undefined);
  assert.equal(runs.length, 1);
});

test("a replay returns recorded steps and fires sleeps", async () => {
  const a = counting("a", 1);
  const b = counting("b", 2);
  const sleepy = workflow("sleepy", async (c: Context) => {
    const x = await a.counted();
    await c.sleep(seconds(10));
    return x + (await b.counted());
  });
  const h = new Harness(sleepy);
  const first = await h.replay(1_000);
  assert.deepEqual(first.events(), ["step", "timer"]);
  assert.equal(first.nextWake(), 11_000);
  assert.deepEqual((await h.replay(5_000)).commits, [], "nothing to record before the timer");
  assert.equal((await h.replay(11_000)).completed(), 3);
  assert.deepEqual([a.runs.length, b.runs.length], [1, 1]);
});

test("receive takes the next message of its name", async () => {
  const receiver = workflow(
    "receiver",
    async (c: Context) => (await c.receive<number>("by")) * (await c.receive<number>("by")),
  );
  const h = new Harness(receiver);
  h.message("k1", "other", "0");
  h.message("k2", "by", "6");
  const first = await h.replay(1_000);
  assert.equal(first.nextWake(), undefined);
  assert.equal(first.completed(), undefined);
  h.message("k3", "by", "7");
  assert.equal((await h.replay(2_000)).completed(), 42);
  assert.deepEqual(h.inbox, [], "every message consumed");
});

const RETRY: Retry = { maxAttempts: 3, initial: seconds(1), max: seconds(4), multiplier: 2 };

function flaky(failFirst: number, permanent: boolean, retry?: Retry) {
  const runs: number[] = [];
  const attempt = step(
    "flaky",
    () => {
      runs.push(1);
      if (runs.length <= failFirst) {
        throw permanent ? new Permanent("gone") : new Error(`attempt ${runs.length}`);
      }
      return `ok after ${runs.length}`;
    },
    retry === undefined ? {} : { retry },
  );
  return { runs, attempt };
}

test("a failed attempt backs off then retries", async () => {
  const f = flaky(1, false, RETRY);
  const h = new Harness(workflow("retrying", async () => await f.attempt()));
  const first = await h.replay(1_000);
  assert.deepEqual(first.events(), ["stepRetry"]);
  const wake = first.nextWake();
  assert.ok(wake !== undefined && wake > 1_000);
  assert.equal((await h.replay(wake)).completed(), "ok after 2");
  assert.equal(f.runs.length, 2);
});

for (const [permanent, retry] of [
  [false, undefined],
  [true, RETRY],
] as const) {
  test(`steps fail at once ${permanent ? "with a permanent error" : "without retry"}`, async () => {
    const f = flaky(9, permanent, retry);
    const failing = workflow("failing", async () => await f.attempt());
    const failure = (await new Harness(failing).replay(1_000)).failed();
    assert.equal(failure?.type, "StepFailed");
    assert.equal(f.runs.length, 1);
  });
}

test("a late attempt times out", async () => {
  const slow = step("slow", () => sleep(300).then(() => 1), { timeout: milliseconds(20) });
  const late = workflow("late", async () => {
    try {
      await slow();
    } catch (e) {
      if (e instanceof StepFailed) return e.message;
      throw e;
    }
    throw new Error("the attempt did not time out");
  });
  assert.match(String((await new Harness(late).replay(1_000)).completed()), /attempt past/);
});

test("parallel steps replay the same", async () => {
  const a = counting("a", 1);
  const b = counting("b", 2);
  const parallel = workflow("parallel", async (c: Context) => {
    const [x, y] = await Promise.all([a.counted(), b.counted()]);
    await c.sleep(seconds(1));
    return x + y;
  });
  const h = new Harness(parallel);
  assert.equal((await h.replay(1_000)).nextWake(), 2_000);
  assert.equal((await h.replay(2_000)).completed(), 3);
  assert.equal(a.runs.length + b.runs.length, 2);
});

test("other calls than the history fail the run", async () => {
  const { counted: renamed } = counting("renamed", 1);
  const h = new Harness(workflow("changed", async () => await renamed()));
  const recorded = {
    name: "original",
    occurrence: 0n,
    attempt: 1,
    value: jsonValue(encoder.encode("1")),
  };
  h.history.push(create(layout.EventSchema, { kind: { case: "step", value: recorded } }));
  h.nextN = 1n;
  assert.equal((await h.replay(1_000)).failed()?.type, "NonDeterminism");
});

test("a lost commit drops the replay", async () => {
  const { counted: a } = counting("a", 1);
  const h = new Harness(workflow("loser", async () => await a()));
  const replayed = await h.replay(1_000, false);
  assert.equal(replayed.end.end.case, "dropped");
  assert.deepEqual(h.history, []);
});

test("a store failing the read of a workflow's send drops the replay", async () => {
  const h = new Harness(workflow("sender", async (c: Context) => c.send("other", "note", 1)));
  const failing = new Recording(new MemoryStore(`replay-${token()}`));
  failing.failing = true;
  h.store = failing;
  const replayed = await h.replay(1_000);
  assert.equal(replayed.end.end.case, "dropped");
  assert.deepEqual(h.history, []);
  h.store = new MemoryStore(`replay-${token()}`);
  assert.equal((await h.replay(1_000)).failed()?.type, "RunNotFound");
});

test("inputs and errors of the code", async () => {
  class Negative extends Error {}
  const typed = workflow("typed", async (_c: Context, input: { x: number }) => {
    if (typeof input.x !== "number") throw new TypeError("x is not a number");
    if (input.x < 0) throw new Negative("x < 0");
    return input.x + 1;
  });
  for (const [data, failure] of [
    ['{"x": 41}', undefined],
    ['{"x": -1}', "Negative"],
    ['{"y": 1}', "TypeError"],
    ["[1]", "InvalidArgument"],
  ] as const) {
    const h = new Harness(typed);
    h.input = data;
    const replayed = await h.replay(1_000);
    if (failure === undefined) assert.equal(replayed.completed(), 42);
    else assert.equal(replayed.failed()?.type, failure, data);
  }
});

test("steps get their key, the same for each occurrence's attempts", async () => {
  const keyed = workflow("keyed", async (c: Context) => {
    const first = await c.step("where", () => stepKey());
    const second = await c.step("where", async () => stepKey());
    return [first, second];
  });
  assert.deepEqual((await new Harness(keyed).replay(1_000)).completed(), [
    "run-1/where/0",
    "run-1/where/1",
  ]);
});

test("steps outside a workflow are refused", async () => {
  const { counted: a } = counting("a", 1);
  assert.throws(() => a(), NotInWorkflow);
  assert.throws(() => stepKey(), NotInWorkflow);
  const unjson = workflow("unjson", async (c: Context) => await c.step("map", () => new Map()));
  const failure = (await new Harness(unjson).replay(1_000)).failed();
  assert.equal(failure?.type, "StepFailed");
  assert.match(failure?.message ?? "", /Map/);
});
