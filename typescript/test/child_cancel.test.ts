// `child.cancel`: a workflow cancels a child it started, as a client's `run.cancel` does, ported
// from the Python tests under the same names.
import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { withEnv } from "./testing_helpers.ts";

const service = { preset: "service" } as const;

const waiter = s.workflow("waiter", async (c) => await c.receive<string>("go"));

const quick = s.workflow("quick", async () => 1);

const canceller = s.workflow("canceller", async (c) => {
  const child = await c.start(waiter);
  await child.cancel("no longer needed");
  try {
    await child.result();
  } catch (e) {
    if (e instanceof s.RunFailed)
      return { chain: child.chain, status: e.status, reason: e.error.message };
    throw e;
  }
  throw new Error("the child was not cancelled");
});

test(
  "cancel a running child",
  withEnv(async (env) => {
    await using _worker = env.worker([canceller, waiter], service);
    const run = await env.start(canceller);
    const { chain, status, reason } = await run.result();
    assert.deepEqual([status, reason], ["cancelled", "no longer needed"]);
    await assert.rejects(
      env.run(waiter, chain).result(),
      (e) => e instanceof s.RunFailed && e.status === "cancelled",
    );
  }),
);

const lateCanceller = s.workflow("late_canceller", async (c) => {
  const child = await c.start(quick);
  await child.result();
  try {
    await child.cancel("too late");
  } catch (e) {
    if (e instanceof s.RunEnded) return [child.chain, e.message];
    throw e;
  }
  throw new Error("cancelling an ended child did not raise");
});

test(
  "cancel a child that ended raises RunEnded",
  withEnv(async (env) => {
    await using _worker = env.worker([lateCanceller, quick], service);
    const run = await env.start(lateCanceller);
    const [chain, message] = await run.result();
    assert.equal(message, `run ${chain} has ended, completed`);
  }),
);

const suspendingCanceller = s.workflow("suspending_canceller", async (c) => {
  const child = await c.start(waiter);
  await child.cancel("no longer needed");
  await c.receive("resume");
  try {
    await child.result();
  } catch (e) {
    if (e instanceof s.RunFailed) return { chain: child.chain, status: e.status };
    throw e;
  }
  throw new Error("the child was not cancelled");
});

test(
  "the cancel outcome replays",
  withEnv(async (env) => {
    let run: s.Run<{ chain: string; status: string }>;
    {
      await using _worker = env.worker([suspendingCanceller, waiter], service);
      run = await env.start(suspendingCanceller);
      await env.settle();
    }
    // The child ended cancelled: a cancel read again would raise RunEnded.
    await using _worker = env.worker([suspendingCanceller, waiter], service);
    await run.send("resume");
    const { chain, status } = await run.result();
    assert.equal(status, "cancelled");
    const sent = (await run.history()).filter((e) => e.kind === "sent").map((e) => e.data);
    assert.deepEqual(
      sent.map((e) => [e.name, e.value]),
      [["substantial.cancel", "no longer needed"]],
    );
    const history = await env.run(waiter, chain).history();
    assert.equal(history.filter((e) => e.kind === "cancelled").length, 1);
  }),
);

const oddCanceller = s.workflow("odd_canceller", async (c) => {
  const child = await c.start(waiter);
  try {
    await child.cancel(42 as unknown as string);
  } catch (e) {
    if (!(e instanceof s.InvalidArgument)) throw e;
    await child.send("go", "done");
    await child.result();
    return e.message;
  }
  throw new Error("a reason that is not a string was accepted");
});

test(
  "a non-string reason raises InvalidArgument",
  withEnv(async (env) => {
    await using _worker = env.worker([oddCanceller, waiter], service);
    const run = await env.start(oddCanceller);
    assert.equal(await run.result(), "a reason is a string, not 42");
  }),
);

const impostor = s.workflow("impostor", async (c) => {
  const child = await c.start(waiter);
  const refused: string[] = [];
  for (const send of [
    () => child.send("substantial.cancel", "x"),
    () => c.send(child.chain, "substantial.cancel", "x"),
  ]) {
    try {
      send();
    } catch (e) {
      if (e instanceof s.InvalidArgument) refused.push(e.message);
    }
  }
  await child.send("go", "done");
  await child.result();
  return refused;
});

test(
  "a workflow sending substantial.cancel raises InvalidArgument",
  withEnv(async (env) => {
    await using _worker = env.worker([impostor, waiter], service);
    const run = await env.start(impostor);
    const message = 'message name "substantial.cancel" starts with substantial., which is reserved';
    assert.deepEqual(await run.result(), [message, message]);
  }),
);
