// Opening namespaces and running workers on an idle namespace.
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { createOn, type Host, openOn } from "../src/client.ts";
import { RealClock } from "../src/clock.ts";
import {
  ConfigError,
  Fail,
  InvalidArgument,
  LayoutError,
  milliseconds,
  type Observed,
  RunFailed,
  Skewed,
  StoreOp,
  Substantial,
  seconds,
  step,
  workflow,
} from "../src/mod.ts";
import { ulid } from "../src/plan.ts";
import { Run } from "../src/run.ts";
import { MemoryStore } from "../src/stores/memory.ts";
import type { Create, Listing, ListOptions, ListWait } from "../src/stores/store.ts";
import { Worker, wake } from "../src/worker.ts";
import { eachAdapter, eventually, LONG, Recording, script } from "./helpers.ts";

const example = workflow("example", async () => 1);

eachAdapter("an idle worker lists every workflow then shuts down", async (store) => {
  const recording = new Recording(store);
  const sub = await createOn(recording);
  const other = workflow("other", async () => 2);
  const worker = script(sub, [example, other]);
  await eventually("listings", () => recording.listed.length >= 4);
  for (const name of ["example", "other"]) {
    const prefix = `queues/${name}/due/`;
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
  const sub = await createOn(recording);
  recording.failing = true;
  await using _ = script(sub, [example]);
  const before = recording.listed.length;
  await eventually("listings", () => recording.listed.length >= before + 3);
});

test("workers refuse invalid configuration", async () => {
  const sub = await Substantial.create("memory://worker-config");
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
});

test("create states what is unknown or not built", async () => {
  for (const url of ["file//tmp/x", "/tmp/x", "ftp://host/x"]) {
    await assert.rejects(
      Substantial.create(url),
      (e) =>
        e instanceof ConfigError && /^unknown scheme .* memory:\/\/, file:, s3:/.test(e.message),
    );
  }
  for (const url of ["sqlite:x.db", "mysql://u@h/db"]) {
    await assert.rejects(
      Substantial.create(url),
      (e) => e instanceof ConfigError && /not available in this host yet/.test(e.message),
    );
  }
  await assert.rejects(
    Substantial.create("file:~/x"),
    (e) => e instanceof ConfigError && /give an absolute or relative path/.test(e.message),
  );
});

eachAdapter("create and open check the layout", async (store) => {
  await assert.rejects(openOn(store), LayoutError);
  await createOn(store);
  await createOn(store);
  await openOn(store);
  const marker = new TextDecoder().decode((await store.get("substantial.json"))?.data);
  assert.equal(marker, '{"layout":3,"encoding":"protobuf"}');
});

test("a namespace of the layout before is refused", async () => {
  // With its partitions: drained by the version that wrote it, never opened by this one.
  const old = new MemoryStore("worker-layout-1");
  await old.create("probe/exclusive", new Uint8Array(), undefined);
  const marker = '{"layout":1,"encoding":"protobuf","partitions":4,"hash":"xxh3-64"}';
  await old.create("substantial.json", new TextEncoder().encode(marker), undefined);
  for (const opened of [openOn(old), createOn(old)]) {
    await assert.rejects(opened, (e) => e instanceof LayoutError && /has layout 1/.test(e.message));
  }
  // Layout 2, Substantial 0.3, is never read.
  const second = new MemoryStore("worker-layout-2");
  await second.create("probe/exclusive", new Uint8Array(), undefined);
  const layout2 = '{"layout":2,"encoding":"protobuf"}';
  await second.create("substantial.json", new TextEncoder().encode(layout2), undefined);
  for (const opened of [openOn(second), createOn(second)]) {
    await assert.rejects(opened, (e) => e instanceof ConfigError && /layout 2/.test(e.message));
  }
});

test("create and open take a URL and its credentials only", async () => {
  const url = "memory://create-options";
  const refused: [string, () => Promise<unknown>][] = [
    ["credentials", () => Substantial.create(url, { credentials: "env" })],
    ["extra", () => Substantial.create(url, { extra: 1 } as never)],
    ["storeRetry", () => Substantial.open(url, { storeRetry: {} } as never)],
    ["store object", () => Substantial.create(new MemoryStore("object") as never)],
    ["incomplete URL", () => Substantial.open("s3://bucket/ns", { credentials: "env" })],
  ];
  for (const [what, call] of refused) await assert.rejects(call(), ConfigError, what);
  await assert.rejects(
    Substantial.create(3 as never),
    (e) => e instanceof ConfigError && /a store is a URL string/.test(e.message),
  );
});

test("a store that accepts two creates fails the probe", async () => {
  class Overwriting extends MemoryStore {
    override async create(
      key: string,
      data: Uint8Array,
      expiry: number | undefined,
    ): Promise<Create> {
      await this.delete(key);
      return super.create(key, data, expiry);
    }
  }
  await assert.rejects(
    createOn(new Overwriting("probe-overwrite")),
    (e) => e instanceof ConfigError && /its creates are not exclusive/.test(e.message),
  );
});

test("openers at once each probe keys of their own", async () => {
  const store = new Recording(new MemoryStore("probe-own"));
  await createOn(store);
  await Promise.all(Array.from({ length: 4 }, () => openOn(store)));
  // § mgfgmd25p.e@rn5dg
  const probes = store.writes.filter(([key]) => key.startsWith("probe/"));
  const ids = new Set(probes.map(([key]) => key.split("/")[1]));
  assert.equal(ids.size, 5, `one id per opener: ${JSON.stringify(probes)}`);
  assert.ok(
    [...ids].every((id) => id?.length === 26),
    JSON.stringify([...ids]),
  );
  assert.deepEqual((await store.list("probe/", { maxKeys: 10 })).keys, []);
  // § end mgfgmd25p.e
  // § mgfgmd25p.g@3m38r
  const unexpiring = probes.filter(([, expiry]) => expiry !== 1);
  assert.deepEqual(unexpiring, []);
  // § end mgfgmd25p.g
});

test("a worker checks the rule of the probe keys at start", async () => {
  const store = new Recording(new MemoryStore("probe-rule"));
  const sub = await createOn(store);
  const worker = script(sub, [example], { retention: 30 });
  const run = await sub.start(example);
  assert.equal(await run.result({ timeout: LONG }), 1);
  await worker.shutdown();
  // § cfdckf61n.c@sdhnf
  assert.equal(store.checked.length, 1, JSON.stringify(store.checked));
  const [days] = store.checked;
  assert.ok(days?.includes(30) && days.includes(1), JSON.stringify(days));
  // § end cfdckf61n.c
});

for (const signal of ["SIGTERM", "SIGINT"] as const) {
  test(`serve shuts down on ${signal}`, async () => {
    const sub = await Substantial.create(`memory://serve-${signal.toLowerCase()}`, {});
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
    await using _sub = await Substantial.create("memory://dispose");
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
  const sub = await Substantial.create("memory://declarations");
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
  const sub = await createOn(new Waiting("waiting-store"));
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
    const sub = await Substantial.create("memory://worker-dies");
    const worker = script(sub, [waiting]);
    const run = await sub.start(waiting);
    const pending = run.result({ timeout: LONG });
    await eventually("a suspended run", async () => (await run.status()) === "suspended");
    assert.ok(worker.alive);
    assert.equal(worker.error, undefined);
    AgentMachine.prototype.feed = () => {
      throw trap;
    };
    const began = performance.now();
    await assert.rejects(pending, (e) => e === trap);
    assert.ok(performance.now() - began < 2000, "failed at once, not at the timeout");
    assert.ok(errors.some(([what, e]) => /worker \w+ died:/.test(String(what)) && e === trap));
    // What a readiness probe reads.
    assert.ok(!worker.alive);
    assert.equal(worker.error, trap);
    await assert.rejects(worker.shutdown(), (e) => e === trap);
  } finally {
    AgentMachine.prototype.feed = feed;
    console.error = error;
  }
});

/** A memory store whose listings claim to wait: one given a wait answers after 20 ms whatever
 * changed, unless its signal aborts it first. */
class Waiting extends MemoryStore {
  override readonly capabilities = { wait: true };
  readonly resultWaits: ListOptions[] = [];

  override async list(prefix: string, options: ListOptions): Promise<Listing> {
    const { wait, signal, ...rest } = options;
    const listing = await super.list(prefix, rest);
    if (wait !== undefined) {
      if (prefix.startsWith("runs/")) this.resultWaits.push(options);
      await sleep(20, undefined, signal === undefined ? {} : { signal });
    }
    return { keys: listing.keys, etag: "etag" };
  }
}

test("a result wait abandoned when its worker dies stops listing", async () => {
  const { AgentMachine } = await import("../src/machines.ts");
  const feed = AgentMachine.prototype.feed;
  const error = console.error;
  console.error = () => {};
  try {
    const waiting = workflow("abandoned-waiting", async (c) => {
      await c.step("first", () => null);
      await c.receive("never");
    });
    const store = new Waiting("worker-abandoned");
    const sub = await createOn(store);
    const worker = script(sub, [waiting]);
    const run = await sub.start(waiting);
    await eventually("a suspended run", async () => (await run.status()) === "suspended");
    const pending = run.result({ timeout: LONG });
    await eventually("a result wait", () => store.resultWaits.length >= 2);
    AgentMachine.prototype.feed = () => {
      throw new Error("injected trap");
    };
    await assert.rejects(pending, /injected trap/);
    const after = store.resultWaits.length;
    await sleep(200);
    assert.equal(store.resultWaits.length, after, "no listing after the wait was abandoned");
    assert.ok(store.resultWaits.at(-1)?.signal?.aborted, "the listing in flight aborted");
    await assert.rejects(worker.shutdown(), /injected trap/);
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
      throw new Fail("Boom", "boom");
    });
    const waiting = workflow("ends-cancelled", async (c) => {
      await c.step("first", () => null);
      await c.receive("never");
    });
    const sub = await Substantial.create("memory://worker-warns");
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
    warnings.filter((w) => w.startsWith("run ")),
    ["run warn-failed failed: Boom: boom", "run warn-cancelled cancelled: not needed"],
  );
});

const aSecond = workflow("a-second", async (c) => {
  await c.sleep(seconds(1));
  return 1;
});

test("result polls up to max interval", async () => {
  const gets: StoreOp[] = [];
  const observe = (event: Observed) => {
    if (event instanceof StoreOp && event.op === "get") gets.push(event);
  };
  const url = "memory://max-interval";
  await using sub = await Substantial.create(url);
  await using client = await Substantial.open(url, { observe });
  await using _ = script(sub, [aSecond]);
  const run = await client.start(aSecond);
  assert.equal(await run.result({ maxInterval: milliseconds(20) }), 1);
  // Every 20 ms for a second, where doubling from 10 ms up to 30 s polls 8 times.
  assert.ok(gets.length > 20, String(gets.length));
  await assert.rejects(
    run.result({ maxInterval: milliseconds(0) }),
    (e) => e instanceof InvalidArgument && /maxInterval/.test(e.message),
  );
});

/** The real clock `by` ms ahead. */
class Ahead extends RealClock {
  readonly by: number;

  constructor(by: number) {
    super();
    this.by = by;
  }

  override now(): number {
    return super.now() + this.by;
  }
}

/** A host whose workers run on the real clock `by` ms ahead. */
function aheadBy(by: number): Host {
  return {
    now: () => Date.now(),
    ulid,
    worker: (module, s, workflows, settings, hooks, metering) =>
      new Worker(
        { core: module.instantiate(), store: s, clock: new Ahead(by), id: "ahead" },
        workflows,
        settings,
        hooks,
        metering,
      ),
    handle: (ns, w, chain) => new Run(ns, w, chain),
  };
}

test("a worker whose clock is behind a run's last segment leaves it alone", async () => {
  const told: Skewed[] = [];
  const store = new MemoryStore(`skewed-${crypto.randomUUID()}`);
  const sub = await createOn(store, {
    observe: (e: Observed) => {
      if (e instanceof Skewed) told.push(e);
    },
  });
  const waiting = workflow("waiting", async (c) => await c.receive<number>("go"));
  // A minute ahead, past `claim_skew`, 5 s in the script preset: its segment is in the future of
  // every right clock.
  const early = await openOn(store, {}, aheadBy(60_000));
  const ahead = script(early, [waiting]);
  const run = await sub.start(waiting);
  await eventually("the wait", async () => (await run.status()) === "suspended");
  await ahead.shutdown();
  const right = script(sub, [waiting]);
  await run.send("go", 1);
  // § kr6b4hm8k@2h5qg
  await eventually("the skew told", () => told.length > 0);
  const [skew] = told;
  assert.equal(skew?.run, run.chain);
  // Not continued: the chain's current run is its first.
  assert.equal(skew.current, run.chain);
  assert.ok(skew.segmentAt.getTime() - skew.now.getTime() > 55_000, String(skew.segmentAt));
  assert.equal(await run.status(), "suspended", "left alone");
  // § end kr6b4hm8k
  await right.shutdown();
  await early.close();
  await sub.close();
});

test("a message from a sender whose clock is ahead is consumed at once", async () => {
  const store = new MemoryStore(`sender-ahead-${crypto.randomUUID()}`);
  const sub = await createOn(store);
  const waiting = workflow("waiting_for_go", async (c) => await c.receive<number>("go"));
  const sending = workflow("sending_go", async (c, input: { to: string }) => {
    await c.send(input.to, "go", 7);
    return 0;
  });
  const right = script(sub, [waiting]);
  // An hour ahead: a message keyed by its sender's time would not be due for an hour.
  const early = await openOn(store, {}, aheadBy(3_600_000));
  const ahead = script(early, [sending]);
  const run = await sub.start(waiting);
  const sender = await sub.start(sending, { to: run.chain });
  await sender.result({ timeout: LONG });
  // § d9vmk6y6y.e@79v7r
  assert.equal(await run.result({ timeout: LONG }), 7);
  // § end d9vmk6y6y.e
  await ahead.shutdown();
  await right.shutdown();
  await early.close();
  await sub.close();
});
