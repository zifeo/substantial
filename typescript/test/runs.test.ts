// The live runs of a workflow, the history of a run and its retry, in the test environment.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { T0, withEnv } from "./testing_helpers.ts";

const service = { preset: "service" } as const;

const nap = async (c: s.Context, input: { days: number }) => {
  await c.sleep(s.days(input.days));
  return "slept";
};
const sleeper = s.workflow("sleeper", nap);
const sleeperV2 = s.workflow("sleeper", nap, { version: 2 });
const inbox = s.workflow("inbox", async (c) => await c.receive<string>("go"));

function at(ms: number): Date {
  return new Date(T0.getTime() + ms);
}

async function listed(sub: s.Substantial, workflow: Parameters<s.Substantial["runs"]>[0]) {
  const infos: s.RunInfo[] = [];
  for await (const info of sub.runs(workflow)) infos.push(info);
  return infos.sort((a, b) => (a.chain < b.chain ? -1 : 1));
}

test(
  "runs lists the live runs of a workflow, all its versions, each once",
  withEnv(async (env) => {
    await using _worker = env.worker([sleeper, inbox], service);
    await env.start(sleeper, { days: 1 }, { id: "sleeping" });
    const ended = await env.start(sleeper, { days: 0 }, { id: "ended" });
    const waiting = await env.start(inbox, undefined, { id: "waiting" });
    await env.settle();
    assert.equal(await ended.result(), "slept");
    await env.advance(s.hours(1));
    await env.start(sleeperV2, { days: 1 }, { id: "pending" });
    const sleepers = await listed(env, sleeper);
    for (const info of sleepers) assert.equal(typeof info.run, "string");
    assert.deepEqual(
      sleepers.map(({ run: _, ...info }) => info),
      [
        { chain: "pending", version: 2, dueAt: at(s.hours(1).ms), waitingOn: "start" },
        { chain: "sleeping", dueAt: at(s.days(1).ms), waitingOn: "time" },
      ],
    );
    const inboxes = await listed(env, inbox);
    assert.deepEqual(
      inboxes.map(({ run: _, ...info }) => info),
      [{ chain: "waiting", waitingOn: "message" }],
    );
    await waiting.send("go", "now");
    assert.deepEqual(
      (await listed(env, inbox)).map(({ chain, waitingOn }) => [chain, waitingOn]),
      [["waiting", "inbox"]],
    );
    await env.settle();
    assert.equal(await waiting.result(), "now");
    assert.deepEqual(await listed(env, inbox), []);
  }),
);

test(
  "runs can be left early, and takes a workflow",
  withEnv(async (env) => {
    for (const id of ["a", "b", "c"]) await env.start(sleeper, { days: 1 }, { id });
    let first: s.RunInfo | undefined;
    for await (const info of env.runs(sleeper)) {
      first = info;
      break;
    }
    assert.equal(first?.waitingOn, "start");
    assert.deepEqual(
      (await listed(env, sleeper)).map((info) => info.chain),
      ["a", "b", "c"],
    );
    await assert.rejects(env.runs("sleeper" as never).next(), s.InvalidArgument);
  }),
);

test(
  "history gives the events of a run in order",
  withEnv(async (env) => {
    const stepping = s.workflow("stepping", async (c) => {
      const value = await c.step("a", () => 41);
      await c.sleep(s.hours(1));
      return value + 1;
    });
    await using _worker = env.worker([stepping], service);
    const run = await env.start(stepping, undefined, { id: "stepping" });
    await env.advance(s.hours(1));
    assert.equal(await run.result(), 42);
    const history = await run.history();
    assert.deepEqual(
      history.map((e) => e.kind),
      ["step", "timer", "fired", "completed"],
    );
    const [step] = history;
    assert.equal(step?.data.name, "a");
    assert.equal(step?.data.value, 41);
    for (const event of history) {
      assert.equal(event.run, history[0]?.run);
      assert.ok(Number.isSafeInteger(event.segment) && event.segment >= 0);
      assert.ok(event.at instanceof Date && !Number.isNaN(event.at.getTime()));
    }
    await assert.rejects(env.run(stepping, "unknown").history(), s.RunNotFound);
  }),
);

test(
  "retry resumes a failed run with its recorded steps and their keys",
  withEnv(async (env) => {
    let failing = true;
    let a = 0;
    const keys: string[] = [];
    const flaky = s.workflow("flaky", async (c) => {
      const first = await c.step("a", () => {
        a += 1;
        return `a${a}`;
      });
      const second = await c.step("b", () => {
        keys.push(s.stepKey());
        if (failing) throw new s.Permanent("flagged");
        return "b";
      });
      return [first, second];
    });
    await using _worker = env.worker([flaky], service);
    const run = await env.start(flaky, undefined, { id: "flaky" });
    await assert.rejects(run.result(), s.RunFailed);
    failing = false;
    await run.retry();
    assert.deepEqual(await run.result(), ["a1", "b"]);
    assert.equal(a, 1);
    assert.equal(keys.length, 2);
    assert.equal(keys[0], keys[1]);
    assert.equal(new Set((await run.history()).map((e) => e.run)).size, 2);
    await assert.rejects(run.retry(), s.InvalidArgument);
  }),
);

test(
  "retry refuses a live chain, an unknown one and a rollback",
  withEnv(async (env) => {
    const rolling = s.workflow("rolling", async (c) => {
      await c.step("a", () => null);
      await c.rollback("why");
    });
    await using _worker = env.worker([rolling], service);
    const pending = await env.start(sleeper, { days: 1 }, { id: "pending" });
    await assert.rejects(pending.retry(), s.InvalidArgument);
    await assert.rejects(env.run(sleeper, "unknown").retry(), s.RunNotFound);
    const rolled = await env.start(rolling, undefined, { id: "rolled" });
    await assert.rejects(rolled.result(), s.RunFailed);
    await assert.rejects(rolled.retry(), s.InvalidArgument);
  }),
);

test(
  "retry refuses a run whose failure ran undos",
  withEnv(async (env) => {
    const undone: string[] = [];
    const reserve = s.step("reserve", () => "reserved", {
      undo: () => {
        undone.push("reserve");
      },
    });
    const saga = s.workflow("saga", async (c) => {
      await c.saga(async () => {
        await reserve();
        await c.step("charge", () => {
          throw new s.Permanent("declined");
        });
      });
    });
    await using _worker = env.worker([saga], service);
    const run = await env.start(saga, undefined, { id: "saga" });
    await assert.rejects(run.result(), s.RunFailed);
    assert.deepEqual(undone, ["reserve"]);
    await assert.rejects(
      run.retry(),
      (e) => e instanceof s.InvalidArgument && /ran undos/.test(e.message),
    );
    await assert.rejects(run.result(), (e) => e instanceof s.RunFailed && e.run === "saga");
  }),
);
