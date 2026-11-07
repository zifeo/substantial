// Opening namespaces and running workers on an idle namespace.
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { createOn, openOn } from "../src/client.ts";
import {
  ConfigError,
  InvalidArgument,
  LayoutError,
  milliseconds,
  RunFailed,
  Substantial,
  seconds,
  step,
  workflow,
} from "../src/mod.ts";
import { MemoryStore } from "../src/stores/memory.ts";
import type { Listing, ListOptions, ListWait } from "../src/stores/store.ts";
import { wake } from "../src/worker.ts";
import { eachAdapter, eventually, LONG, Recording, script } from "./helpers.ts";

const example = workflow("example", async () => 1);

eachAdapter("an idle worker lists every partition then shuts down", async (store) => {
  const recording = new Recording(store);
  const sub = await createOn(recording, { partitions: 3 });
  const worker = script(sub, [example]);
  await eventually("listings", () => recording.listed.length >= 9);
  for (let p = 0; p < 3; p++) {
    const prefix = `queues/example/${String(p).padStart(4, "0")}/due/`;
    const count = recording.listed.filter((l) => l === prefix).length;
    assert.ok(count >= 2, `${prefix} listed again: ${recording.listed}`);
  }
  wake(worker);
  const began = performance.now();
  await worker.shutdown();
  assert.ok(performance.now() - began < 5000);
});

test("a failing store never stops the worker", async () => {
  const recording = new Recording(new MemoryStore("worker-failing"));
  const sub = await createOn(recording, { partitions: 1 });
  recording.failing = true;
  await using _ = script(sub, [example]);
  const before = recording.listed.length;
  await eventually("listings", () => recording.listed.length >= before + 3);
});

test("workers refuse invalid configuration", async () => {
  const sub = await Substantial.create("memory://worker-config", { partitions: 1 });
  assert.throws(() => script(sub, [example, example]), ConfigError);
  assert.throws(() => script(sub, []), ConfigError);
  assert.throws(() => script(sub, [((c: unknown) => c) as never]), ConfigError);
  for (const settings of [{ concurrency: 0 }, { concurency: 1 }, { maxIdle: 1 }]) {
    assert.throws(() => script(sub, [example], settings as never), ConfigError);
  }
  assert.throws(() => sub.worker([example], { preset: "daemon" as never }), ConfigError);
  assert.throws(() => sub.worker([example], {} as never), ConfigError);
  assert.throws(
    () => sub.worker([example], { preset: "script", duePages: 20 } as never),
    (e) => e instanceof ConfigError && e.message === 'unknown settings ["duePages"]',
  );
  await sub.worker([example], { preset: "script", maxIdle: milliseconds(50) }).shutdown();
  assert.throws(() => workflow("v", async () => 1, { version: -1 }), InvalidArgument);
  assert.throws(() => workflow("v", async () => 1, { verison: 1 } as never), InvalidArgument);
  await assert.rejects(
    Substantial.create("memory://worker-config", undefined as never),
    (e) => e instanceof ConfigError && /needs partitions, a positive whole number/.test(e.message),
  );
});

test("create states what is unknown or not built", async () => {
  for (const url of ["file//tmp/x", "/tmp/x", "ftp://host/x"]) {
    await assert.rejects(
      Substantial.create(url, { partitions: 1 }),
      (e) =>
        e instanceof ConfigError && /^unknown scheme .* memory:\/\/, file:, s3:/.test(e.message),
    );
  }
  for (const url of ["redis://localhost:6379/0?namespace=x", "sqlite:x.db", "mysql://u@h/db"]) {
    await assert.rejects(
      Substantial.create(url, { partitions: 1 }),
      (e) => e instanceof ConfigError && /not available in this host yet/.test(e.message),
    );
  }
  await assert.rejects(
    Substantial.create("file:~/x", { partitions: 1 }),
    (e) => e instanceof ConfigError && /give an absolute or relative path/.test(e.message),
  );
});

eachAdapter("create and open check the layout", async (store) => {
  await assert.rejects(openOn(store), LayoutError);
  assert.equal((await createOn(store, { partitions: 4 })).partitions, 4);
  assert.equal((await createOn(store, { partitions: 4 })).partitions, 4);
  assert.equal((await openOn(store)).partitions, 4);
  await assert.rejects(createOn(store, { partitions: 2 }), LayoutError);
  for (const partitions of [0, -1, true, 1.5, "1"]) {
    await assert.rejects(createOn(store, { partitions: partitions as number }), ConfigError);
  }
});

test("create and open take a URL and its credentials only", async () => {
  const url = "memory://create-options";
  const refused: [string, () => Promise<unknown>][] = [
    ["credentials", () => Substantial.create(url, { partitions: 1, credentials: "env" })],
    ["extra", () => Substantial.create(url, { partitions: 1, extra: 1 } as never)],
    ["storeRetry", () => Substantial.open(url, { storeRetry: {} } as never)],
    [
      "store object",
      () => Substantial.create(new MemoryStore("object") as never, { partitions: 1 }),
    ],
    ["incomplete URL", () => Substantial.open("s3://bucket/ns", { credentials: "env" })],
  ];
  for (const [what, call] of refused) await assert.rejects(call(), ConfigError, what);
  await assert.rejects(
    Substantial.create(3 as never, { partitions: 1 }),
    (e) => e instanceof ConfigError && /a store is a URL string/.test(e.message),
  );
});

test("a store that accepts two creates fails the probe", async () => {
  class Overwriting extends MemoryStore {
    override async create(key: string, data: Uint8Array): Promise<"created" | "exists"> {
      await this.delete(key);
      return super.create(key, data);
    }
  }
  await assert.rejects(
    createOn(new Overwriting("probe-overwrite"), { partitions: 1 }),
    LayoutError,
  );
});

for (const signal of ["SIGTERM", "SIGINT"] as const) {
  test(`serve shuts down on ${signal}`, async () => {
    const sub = await Substantial.create(`memory://serve-${signal.toLowerCase()}`, {
      partitions: 1,
    });
    const worker = script(sub, [example]);
    const served = worker.serve();
    const run = await sub.start(example);
    assert.equal(await run.result({ timeout: LONG }), 1);
    process.kill(process.pid, signal);
    await served;
    assert.equal(process.listenerCount(signal), 0, "the handler is removed");
  });
}

test("workflow names are checked when declared", () => {
  assert.throws(() => workflow("substantial.reserved", async () => null), InvalidArgument);
});

test("await using a client closes it", async (t) => {
  const closed = t.mock.method(MemoryStore.prototype, "close");
  {
    await using sub = await Substantial.create("memory://dispose", { partitions: 1 });
    assert.equal(sub.partitions, 1);
  }
  assert.equal(closed.mock.callCount(), 1);
});

test("declarations refuse bad arguments with InvalidArgument", async () => {
  const work = async () => 1;
  const refused = [
    () => step("s", work, { retry: null as never }),
    () =>
      step("s", work, { retry: { maxAttempts: 1, initial: seconds(1), max: seconds(1) } as never }),
    () =>
      step("s", work, {
        retry: {
          maxAttempts: 1,
          initial: seconds(1),
          max: seconds(1),
          multiplier: 1,
          x: 1,
        } as never,
      }),
    () => workflow("w", work, null as never),
    () => workflow("myFlow", work),
    () => step("a step", work),
  ];
  for (const declare of refused) assert.throws(declare, InvalidArgument, String(declare));
  const sub = await Substantial.create("memory://declarations", { partitions: 1 });
  const run = sub.run(example, "r");
  await assert.rejects(run.send("note", 1, null as never), InvalidArgument);
  await assert.rejects(run.result(null as never), InvalidArgument);
  await assert.rejects(sub.start(example, {}, { idd: "x" } as never), InvalidArgument);
});

test("a worker on a waiting store lists with waits", async () => {
  const waits: ListWait[] = [];
  class Waiting extends MemoryStore {
    override readonly capabilities = { wait: true };
    override async list(prefix: string, options: ListOptions): Promise<Listing> {
      const { wait, ...rest } = options;
      const listing = await super.list(prefix, rest);
      if (wait !== undefined) {
        // The worker's own: the client's `result` waits on the run's key too.
        if (prefix.startsWith("queues/")) waits.push(wait);
        // A stand-in for an h3 server: answers after a short wait whatever changed.
        await sleep(50);
      }
      return { keys: listing.keys, etag: "etag" };
    }
  }
  const sub = await createOn(new Waiting("waiting-store"), { partitions: 1 });
  {
    await using _ = script(sub, [example], { wait: seconds(3) });
    assert.equal(await (await sub.start(example)).result({ timeout: LONG }), 1);
    await sleep(500);
  }
  // The core first lists without waiting to learn an etag, then waits on it.
  const seen = new Set(waits.map((w) => `${w.seconds}/${w.etag}`));
  assert.deepEqual(seen, new Set(["0/undefined", "3/etag"]));
});

test("a worker that dies fails the waits pending on its namespace", async () => {
  const { AgentMachine } = await import("../src/machines.ts");
  const feed = AgentMachine.prototype.feed;
  const trap = new Error("injected trap");
  const errors: unknown[][] = [];
  const error = console.error;
  console.error = (...data: unknown[]) => errors.push(data);
  try {
    const waiting = workflow("dies-waiting", async (c) => {
      await c.step("first", () => null);
      await c.receive("never");
    });
    const sub = await Substantial.create("memory://worker-dies", { partitions: 1 });
    const worker = script(sub, [waiting]);
    const run = await sub.start(waiting);
    const pending = run.result({ timeout: LONG });
    await eventually("a suspended run", async () => (await run.status()) === "suspended");
    AgentMachine.prototype.feed = () => {
      throw trap;
    };
    const began = performance.now();
    await assert.rejects(pending, (e) => e === trap);
    assert.ok(performance.now() - began < 2000, "failed at once, not at the timeout");
    assert.ok(errors.some(([what, e]) => /worker \w+ died:/.test(String(what)) && e === trap));
    await assert.rejects(worker.shutdown(), (e) => e === trap);
  } finally {
    AgentMachine.prototype.feed = feed;
    console.error = error;
  }
});

test("a run that ends failed or cancelled logs one warning", async () => {
  const warnings: string[] = [];
  const warn = console.warn;
  console.warn = (...data: unknown[]) => warnings.push(data.join(" "));
  try {
    const failing = workflow("ends-failed", async (c) => {
      await c.sleep(milliseconds(50));
      await c.step("work", () => null);
      throw new RangeError("boom");
    });
    const waiting = workflow("ends-cancelled", async (c) => {
      await c.step("first", () => null);
      await c.receive("never");
    });
    const sub = await Substantial.create("memory://worker-warns", { partitions: 1 });
    await using _ = script(sub, [failing, waiting, example]);
    const failed = await sub.start(failing, {}, { id: "warn-failed" });
    await assert.rejects(failed.result({ timeout: LONG }), RunFailed);
    const cancelled = await sub.start(waiting, {}, { id: "warn-cancelled" });
    await eventually("a suspended run", async () => (await cancelled.status()) === "suspended");
    await cancelled.cancel("not needed");
    await assert.rejects(cancelled.result({ timeout: LONG }), RunFailed);
    await (await sub.start(example)).result({ timeout: LONG });
  } finally {
    console.warn = warn;
  }
  assert.deepEqual(
    warnings.filter((w) => w.startsWith("substantial: run ")),
    [
      "substantial: run warn-failed failed: RangeError: boom",
      "substantial: run warn-cancelled cancelled: not needed",
    ],
  );
});
