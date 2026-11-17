// `c.result(chain, { timeout })`: a workflow awaits the end of any chain, a child of its own or
// not, ported from the Python tests under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { withEnv } from "./testing_helpers.ts";

const service = { preset: "service" } as const;

const quick = s.workflow("quick", async () => 1);

const waiter = s.workflow("waiter", async (c) => await c.receive<string>("go"));

const doomed = s.workflow("doomed", async (c) => {
  throw new Error(await c.receive<string>("go"));
});

/** Continues as new at each `go` while `left`, then ends with the last one's value. */
const relay = s.workflow("relay", async (c, { left }: { left: number }): Promise<string> => {
  const got = await c.receive<string>("go");
  if (left > 0) return await c.continueAsNew({ left: left - 1 });
  return got;
});

const watcher = s.workflow("watcher", async (c, { chain }: { chain: string }) => {
  try {
    return { value: await c.result(chain) };
  } catch (e) {
    if (e instanceof s.RunFailed) return { status: e.status, error: e.error.message };
    if (e instanceof s.RunNotFound) return { notFound: e.message, collected: e.collected };
    throw e;
  }
});

const all = [quick, waiter, doomed, relay, watcher];

test(
  "a chain ended before the watch answers at once",
  withEnv(async (env) => {
    await using _worker = env.worker(all, service);
    const target = await env.start(quick);
    assert.equal(await target.result(), 1);
    const run = await env.start(watcher, { chain: target.chain });
    assert.deepEqual(await run.result(), { value: 1 });
  }),
);

test(
  "a chain that ends while watched wakes the watcher",
  withEnv(async (env) => {
    await using _worker = env.worker(all, service);
    const target = await env.start(waiter);
    const run = await env.start(watcher, { chain: target.chain });
    await env.settle();
    assert.equal(await run.status(), "suspended");
    await target.send("go", "done");
    assert.deepEqual(await run.result(), { value: "done" });
  }),
);

test(
  "a failed chain raises RunFailed",
  withEnv(async (env) => {
    await using _worker = env.worker(all, service);
    const target = await env.start(doomed);
    const run = await env.start(watcher, { chain: target.chain });
    await env.settle();
    await target.send("go", "boom");
    assert.deepEqual(await run.result(), { status: "failed", error: "boom" });
  }),
);

test(
  "a cancelled chain raises RunFailed cancelled",
  withEnv(async (env) => {
    await using _worker = env.worker(all, service);
    const target = await env.start(waiter);
    const run = await env.start(watcher, { chain: target.chain });
    await env.settle();
    await target.cancel("enough");
    assert.deepEqual(await run.result(), { status: "cancelled", error: "enough" });
  }),
);

test(
  "a chain that continued is awaited to its last run",
  withEnv(async (env) => {
    await using _worker = env.worker(all, service);
    const target = await env.start(relay, { left: 1 });
    const run = await env.start(watcher, { chain: target.chain });
    await env.settle();
    await target.send("go", "first");
    await env.settle();
    assert.equal(await run.status(), "suspended");
    await target.send("go", "second");
    assert.deepEqual(await run.result(), { value: "second" });
  }),
);

test(
  "a collected chain raises RunNotFound collected",
  withEnv(async (env) => {
    await using _worker = env.worker(all, service);
    const target = await env.start(quick, undefined, { id: "q" });
    assert.equal(await target.result(), 1);
    await env.advance(s.days(31));
    const run = await env.start(watcher, { chain: "q" });
    const result = (await run.result()) as { notFound: string; collected: boolean };
    assert.equal(result.collected, true);
    assert.ok(result.notFound.startsWith("no chain q"), result.notFound);
  }),
);

test(
  "a missing chain raises RunNotFound",
  withEnv(async (env) => {
    await using _worker = env.worker(all, service);
    const run = await env.start(watcher, { chain: "nobody" });
    assert.deepEqual(await run.result(), { notFound: "no chain nobody", collected: false });
  }),
);

const patient = s.workflow("patient", async (c, { chain }: { chain: string }) => {
  try {
    await c.result(chain, { timeout: s.minutes(5) });
  } catch (e) {
    if (!(e instanceof s.WaitTimeout)) throw e;
    return [e.message, (await c.now()).toISOString(), await c.result<string>(chain)];
  }
  throw new Error("the chain ended before the timeout");
});

test(
  "the timeout raises WaitTimeout then awaiting again gets the value",
  withEnv(async (env) => {
    await using _worker = env.worker([waiter, patient], service);
    const target = await env.start(waiter);
    const run = await env.start(patient, { chain: target.chain });
    await env.advance(s.minutes(6));
    await target.send("go", "late");
    assert.deepEqual(await run.result(), [
      `run ${target.chain} did not end before the timeout`,
      "2030-01-07T09:05:00.000Z",
      "late",
    ]);
  }),
);

test(
  "two watchers of one chain both get its end",
  withEnv(async (env) => {
    await using _worker = env.worker(all, service);
    const target = await env.start(waiter);
    const runs = [];
    for (let i = 0; i < 2; i++) runs.push(await env.start(watcher, { chain: target.chain }));
    await env.settle();
    await target.send("go", "both");
    const results = [];
    for (const r of runs) results.push(await r.result());
    assert.deepEqual(results, [{ value: "both" }, { value: "both" }]);
  }),
);

const narcissist = s.workflow("narcissist", async (c) => {
  const refused: string[] = [];
  for (const chain of [c.chain, "Not An Id"]) {
    try {
      void c.result(chain);
    } catch (e) {
      if (!(e instanceof s.InvalidArgument)) throw e;
      refused.push(e.message);
    }
  }
  return refused;
});

test(
  "waiting on its own chain raises InvalidArgument",
  withEnv(async (env) => {
    await using _worker = env.worker([narcissist], service);
    const run = await env.start(narcissist, undefined, { id: "me" });
    const [own, invalid] = (await run.result()) as string[];
    assert.match(own as string, /cannot await its own chain me/);
    assert.ok((invalid as string).startsWith('run id "Not An Id" must match'), invalid);
  }),
);

const resumed = s.workflow("resumed", async (c, { chain }: { chain: string }) => {
  const value = await c.result<string>(chain);
  await c.receive("resume");
  return value;
});

test(
  "the outcome replays",
  withEnv(async (env) => {
    let run: s.Run<string>;
    let target: s.Run<string>;
    {
      await using _worker = env.worker([waiter, resumed], service);
      target = await env.start(waiter);
      run = await env.start(resumed, { chain: target.chain });
      await env.settle();
      await target.send("go", "kept");
      await env.settle();
      // The watched chain is collected: a result read again would raise RunNotFound.
      await env.advance(s.days(31));
      await assert.rejects(target.status(), s.RunNotFound);
    }
    await using _worker = env.worker([waiter, resumed], service);
    await run.send("resume");
    assert.equal(await run.result(), "kept");
    const history = await run.history();
    const watched = history.filter((e) => e.kind === "watched").map((e) => e.data);
    assert.deepEqual(
      watched.map((w) => [w.chain, w.waiting]),
      [[target.chain, target.chain]],
    );
    const ended = history
      .filter((e) => e.data.name === "substantial.watched")
      .map((e) => e.data.ended as { status: string; value: unknown });
    assert.deepEqual(
      ended.map((e) => [e.status, e.value]),
      [["completed", "kept"]],
    );
  }),
);
