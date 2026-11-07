// Handlers, waits and messages sent from a workflow (I-8, I-9), end to end.
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { createOn } from "../src/client.ts";
import {
  type Context,
  InvalidArgument,
  milliseconds,
  RunEnded,
  RunFailed,
  RunNotFound,
  seconds,
  workflow,
} from "../src/mod.ts";
import { eachAdapter, eventually, LONG, script } from "./helpers.ts";

/** The names approved so far, kept up to date by a handler of "approve". */
function approvals(c: Context): string[] {
  const names: string[] = [];
  c.handle<string>("approve", (name) => {
    if (!names.includes(name)) names.push(name);
  });
  return names;
}

const approver = workflow("approver", async (c: Context, input: { target: string }) => {
  const names = approvals(c);
  await c.waitUntil(() => names.length >= 2);
  await c.send(input.target, "note", names.join(","));
  return names;
});

const waiting = workflow("waiting", async (c: Context) => c.receive<string>("note"));

eachAdapter("a handler sees past messages and a workflow sends", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  const target = await sub.start(waiting, {}, { id: "target" });
  const run = await sub.start(approver, { target: "target" });
  for (const name of ["alice", "alice", "bob"]) await run.send("approve", name);
  await using _ = script(sub, [approver, waiting]);
  assert.deepEqual(await run.result({ timeout: LONG }), ["alice", "bob"]);
  assert.equal(await target.result({ timeout: LONG }), "alice,bob");
});

const quick = workflow("quick", async () => "done");

const refused = workflow("refused", async (c: Context, input: { ended: string }) => {
  const seen: string[] = [];
  for (const target of ["nobody", input.ended]) {
    try {
      await c.send(target, "note", "hi");
      seen.push("sent");
    } catch (e) {
      if (!(e instanceof RunNotFound || e instanceof RunEnded)) throw e;
      seen.push(`${e.name}: ${e.message}`);
    }
  }
  await c.receive("go");
  return seen;
});

eachAdapter("a workflow's send to no chain or an ended run throws, recorded", async (store) => {
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [quick, refused]);
  await (await sub.start(quick, {}, { id: "ended" })).result({ timeout: LONG });
  const run = await sub.start(refused, { ended: "ended" });
  await eventually("the sends recorded", async () => (await run.status()) === "suspended");
  // The chain now exists: a replay that read it again would send.
  await sub.start(quick, {}, { id: "nobody" });
  await run.send("go", null);
  assert.deepEqual(await run.result({ timeout: LONG }), [
    "RunNotFound: no chain nobody",
    "RunEnded: run ended has ended, completed",
  ]);
});

eachAdapter("handlers see every message and receive takes the next", async (store) => {
  const notes = workflow("notes", async (c: Context) => {
    const seen: string[] = [];
    const first = await c.receive<string>("note");
    c.handle<string>("note", (note) => seen.push(note));
    await c.waitUntil(() => seen.length >= 3);
    return { first, seen, second: await c.receive<string>("note") };
  });
  const sub = await createOn(store, { partitions: 1 });
  const run = await sub.start(notes);
  for (const note of ["a", "b", "c"]) await run.send("note", note);
  await using _ = script(sub, [notes]);
  assert.deepEqual(await run.result({ timeout: LONG }), {
    first: "a",
    seen: ["a", "b", "c"],
    second: "b",
  });
});

eachAdapter("handlers bound together see past messages in arrival order", async (store) => {
  const ordered = workflow("ordered", async (c: Context) => {
    await c.receive("go");
    const seen: string[] = [];
    c.handle({
      add: (n: number) => seen.push(`add:${n}`),
      remove: (n: number) => seen.push(`remove:${n}`),
    });
    const adds: number[] = [];
    c.handle<number>("add", (n) => adds.push(n));
    return { seen, adds };
  });
  const sub = await createOn(store, { partitions: 1 });
  const run = await sub.start(ordered);
  for (const [name, n] of [
    ["add", 1],
    ["remove", 1],
    ["add", 2],
    ["remove", 2],
  ] as const) {
    await run.send(name, n);
  }
  await run.send("go");
  await using _ = script(sub, [ordered]);
  assert.deepEqual(await run.result({ timeout: LONG }), {
    seen: ["add:1", "remove:1", "add:2", "remove:2"],
    adds: [1, 2],
  });
});

eachAdapter("a wait returns false when its timeout passes", async (store) => {
  const impatient = workflow("impatient", async (c: Context) => {
    const names = approvals(c);
    const late = await c.waitUntil(() => names.length >= 1, { timeout: milliseconds(200) });
    const onTime = await c.waitUntil(() => names.length >= 1, { timeout: seconds(30) });
    return [late, onTime];
  });
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [impatient]);
  const run = await sub.start(impatient);
  await sleep(600);
  await run.send("approve", "alice");
  assert.deepEqual(await run.result({ timeout: LONG }), [false, true]);
});

eachAdapter("a message no handler can decode fails the run", async (store) => {
  const asNumber = (value: unknown): number => {
    if (typeof value !== "number") throw new TypeError(`${JSON.stringify(value)} is not a number`);
    return value;
  };
  const typed = workflow("typed", async (c: Context) => {
    const seen: number[] = [];
    c.handle("n", (n: number) => seen.push(n), { decode: asNumber });
    await c.waitUntil(() => seen.length >= 2);
  });
  const sub = await createOn(store, { partitions: 1 });
  await using _ = script(sub, [typed]);
  const run = await sub.start(typed);
  await run.send("n", 1);
  await run.send("n", "not a number");
  const failed = await run.result({ timeout: LONG }).catch((e) => e);
  assert.ok(failed instanceof RunFailed, String(failed));
  assert.equal(failed.status, "failed");
  assert.equal(failed.error.type, "MessageDecode", failed.error.message);
});

eachAdapter(
  "a predicate that throws fails the run",
  async (store) => {
    const broken = workflow("broken", async (c: Context) => {
      await c.waitUntil(() => {
        throw new RangeError("no way");
      });
    });
    const sub = await createOn(store, { partitions: 1 });
    await using _ = script(sub, [broken]);
    const failed = await (await sub.start(broken)).result({ timeout: LONG }).catch((e) => e);
    assert.ok(failed instanceof RunFailed, String(failed));
    assert.equal(failed.error.type, "RangeError");
    assert.match(failed.error.message, /no way/);
  },
  ["memory"],
);

eachAdapter(
  "handlers and waits refuse what they cannot use",
  async (store) => {
    const refusing = workflow("refusing", async (c: Context) => {
      const refused: string[] = [];
      const attempts: [string, () => unknown][] = [
        ["async handler", () => c.handle("x", async () => {})],
        ["predicate", () => c.waitUntil(true as never)],
        ["timeout", () => c.waitUntil(() => true, { timeout: 5 as never })],
      ];
      for (const [what, attempt] of attempts) {
        try {
          attempt();
        } catch (e) {
          if (e instanceof InvalidArgument) refused.push(what);
        }
      }
      return refused;
    });
    const sub = await createOn(store, { partitions: 1 });
    await using _ = script(sub, [refusing]);
    assert.deepEqual(await (await sub.start(refusing)).result({ timeout: LONG }), [
      "async handler",
      "predicate",
      "timeout",
    ]);
  },
  ["memory"],
);
