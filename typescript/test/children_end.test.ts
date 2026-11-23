// Every end of a run cancels its children still running but those started with `abandon`, and a
// replay takes a claim only before its first step attempt. The ports of `rust/tests/child_ends.rs`
// and `rust/tests/lazy_claims.rs`.
import assert from "node:assert/strict";
import { test } from "node:test";
import { fromBinary } from "@bufbuild/protobuf";
import * as s from "../src/mod.ts";
import { layout } from "../src/proto.ts";
import { worldOf } from "../src/testing/env.ts";
import { withEnv } from "./testing_helpers.ts";

const service = { preset: "service" } as const;

const waiter = s.workflow("waiter", async (c) => await c.receive<string>("go"));

const leaving = s.workflow("leaving", async (c, input: { fail: boolean; abandon: boolean }) => {
  const child = await c.start(waiter, undefined, { id: "child", abandon: input.abandon });
  await c.step("noted", () => child.chain);
  if (input.fail) throw new s.Fail("Refused", "the order is refused");
  return child.chain;
});

/** The status and the reason of the child's end, once it ended. */
async function childEnd(env: s.testing.Env): Promise<[string, string]> {
  try {
    await env.run(waiter, "child").result();
  } catch (e) {
    if (e instanceof s.RunFailed) return [e.status, e.error.message];
    throw e;
  }
  return ["completed", ""];
}

test(
  "a parent that completes cancels its running child",
  withEnv(async (env) => {
    await using _worker = env.worker([leaving, waiter], service);
    const run = await env.start(leaving, { fail: false, abandon: false }, { id: "parent" });
    assert.equal(await run.result(), "child");
    await env.settle();
    // § fca4h4ker@zmgfb
    const [status, reason] = await childEnd(env);
    assert.equal(status, "cancelled");
    // § end fca4h4ker
    // § fca4h4ker.b@k216j
    const expected = `the parent run ${run.chain} completed`;
    assert.equal(reason, expected);
    // § end fca4h4ker.b
  }),
);

test(
  "a parent that fails cancels its running child",
  withEnv(async (env) => {
    await using _worker = env.worker([leaving, waiter], service);
    const run = await env.start(leaving, { fail: true, abandon: false }, { id: "parent" });
    await assert.rejects(run.result(), s.RunFailed);
    await env.settle();
    // § fca4h4ker.a@3z74k
    const [status, reason] = await childEnd(env);
    assert.deepEqual([status, reason], ["cancelled", `the parent run ${run.chain} failed`]);
    // § end fca4h4ker.a
  }),
);

test(
  "a child started with abandon goes on after its parent ends",
  withEnv(async (env) => {
    await using _worker = env.worker([leaving, waiter], service);
    const run = await env.start(leaving, { fail: false, abandon: true }, { id: "parent" });
    assert.equal(await run.result(), "child");
    await env.settle();
    const child = env.run(waiter, "child");
    // § fca4h4ker@p13v7
    const status = await child.status();
    assert.equal(status, "suspended");
    // § end fca4h4ker
    await child.send("go", "done");
    assert.equal(await child.result(), "done");
  }),
);

test(
  "abandon is a boolean",
  withEnv(async (env) => {
    const starting = s.workflow("starting", async (c) => {
      await c.start(waiter, undefined, { abandon: "yes" as unknown as boolean });
    });
    await using _worker = env.worker([starting, waiter], service);
    const run = await env.start(starting);
    await env.settle();
    assert.equal(await run.status(), "blocked");
    const [info] = await Array.fromAsync(env.runs(starting));
    assert.equal(info?.error?.type, "InvalidArgument");
    assert.match(info?.error?.message ?? "", /abandon is a boolean/);
  }),
);

const stepping = s.workflow("stepping", async (c) => {
  await c.receive<string>("go");
  return await c.step("work", () => 1);
});

test(
  "a replay takes a claim only before its first step attempt",
  withEnv(async (env) => {
    const world = worldOf(env);
    // The lease each write of the chain's head holds: a claim is a head written with one.
    const claims: string[] = [];
    const written = world.written.bind(world);
    world.written = (key, data) => {
      if (key === "runs/r") {
        const run = fromBinary(layout.RunSchema, data);
        const lease = run.kind.case === "head" ? run.kind.value.lease : undefined;
        if (lease !== undefined) claims.push(lease.worker);
      }
      written(key, data);
    };
    await using _worker = env.worker([stepping], service);
    const run = await env.start(stepping, undefined, { id: "r" });
    await env.settle();
    assert.equal(await run.status(), "suspended");
    // § j1nq6rqpf@x193h
    const unclaimed = [...claims];
    assert.deepEqual(unclaimed, [], "a replay that runs no step takes no claim");
    await run.send("go", "now");
    assert.equal(await run.result(), 1);
    assert.ok(claims.length > 0, "a replay that runs a step takes a claim");
    // § end j1nq6rqpf
    // § j1nq6rqpf.c@mb6jk
    const head = world.head("r");
    assert.equal(head.lease, undefined);
    // § end j1nq6rqpf.c
  }),
);
