// Waiting listings of the S3 adapter on `h3+http://`, against the fake h3 server of
// `fake_h3.ts`, as the Rust tests do.
import assert from "node:assert/strict";
import { test } from "node:test";
import { type Context, Credentials, minutes, Substantial, seconds, workflow } from "../src/mod.ts";
import { openStore } from "../src/stores/open.ts";
import { type ListWait, type Store, StoreFailure } from "../src/stores/store.ts";
import { accessKeyId, FakeH3, signedHeaders } from "./fake_h3.ts";
import { eventually, script, token, value } from "./helpers.ts";

const credentials = new Credentials({ accessKeyId: "fake", secretAccessKey: "fake-secret" });
const bytes = (text: string) => new TextEncoder().encode(text);

function list(store: Store, wait?: ListWait) {
  return store.list("q/", { maxKeys: 10, ...(wait === undefined ? {} : { wait }) });
}

async function withH3(fn: (h3: FakeH3, store: Store) => Promise<void>): Promise<void> {
  const h3 = await FakeH3.start();
  const store = await openStore(h3.url(token()), credentials);
  try {
    await fn(h3, store);
  } finally {
    await store.close();
    await h3.close();
  }
}

test("an immediate answer carries the etag", () =>
  withH3(async (h3, store) => {
    assert.equal(store.capabilities.wait, true);
    await store.create("q/a", bytes("a"), undefined);
    assert.equal((await list(store)).etag, undefined);
    assert.equal(h3.listed.at(-1)?.headers["x-herald-wait-seconds"], undefined);
    const began = performance.now();
    const first = await list(store, { seconds: 0 });
    assert.deepEqual(first.keys, ["q/a"]);
    assert.ok(first.etag);
    const headers = h3.listed.at(-1)?.headers ?? {};
    assert.equal(headers["x-herald-wait-seconds"], "0");
    assert.equal(headers["x-herald-wait-etag"], undefined);
    assert.ok(signedHeaders(headers).includes("x-herald-wait-seconds"));
    assert.deepEqual(await list(store, { seconds: 0, etag: first.etag }), first);
    assert.ok(performance.now() - began < 1000);
  }));

test("a held wait is answered by a change", () =>
  withH3(async (h3, store) => {
    await store.create("q/a", bytes("a"), undefined);
    const { etag } = await list(store, { seconds: 0 });
    assert.ok(etag);
    const held = list(store, { seconds: 10, etag });
    await eventually("a held listing", () => h3.holds === 1);
    const began = performance.now();
    await store.create("q/b", bytes("b"), undefined);
    const woken = await held;
    assert.ok(performance.now() - began < 2000);
    assert.deepEqual(woken.keys, ["q/a", "q/b"]);
    assert.ok(woken.etag && woken.etag !== etag);
    const listed = h3.listed.at(-1);
    assert.ok(listed?.woken);
    assert.equal(listed.headers["x-herald-wait-seconds"], "10");
    assert.equal(listed.headers["x-herald-wait-etag"], etag);
    for (const name of ["x-herald-wait-etag", "x-herald-wait-seconds"]) {
      assert.ok(signedHeaders(listed.headers).includes(name), name);
    }
  }));

test("a wait that ends unchanged keeps the etag", () =>
  withH3(async (h3, store) => {
    await store.create("q/a", bytes("a"), undefined);
    await store.create("other/x", bytes("x"), undefined);
    const before = await list(store, { seconds: 0 });
    const began = performance.now();
    const held = list(store, { seconds: 2, ...(before.etag ? { etag: before.etag } : {}) });
    await eventually("a held listing", () => h3.holds === 1);
    await store.create("other/y", bytes("y"), undefined);
    assert.deepEqual(await held, before);
    assert.ok(performance.now() - began >= 1900);
    assert.equal(h3.listed.at(-1)?.woken, false);
  }));

test("a missing etag is a store failure, not retried", () =>
  withH3(async (h3, store) => {
    h3.omitEtag = true;
    await assert.rejects(list(store, { seconds: 0 }), /x-herald-listing-etag/);
    assert.equal(h3.listed.length, 1);
    await list(store);
  }));

test("waits an h3 server cannot answer are refused before any request", () =>
  withH3(async (h3, store) => {
    await assert.rejects(list(store, { seconds: 21 }));
    await assert.rejects(store.list("q/", { maxKeys: 1001, wait: { seconds: 0 } }));
    assert.equal(h3.listed.length, 0);
  }));

test("a start from another client comes through the wait", async () => {
  const h3 = await FakeH3.start();
  const url = h3.url("e2e");
  const answer = workflow("answer", async () => 42);
  const sub = await Substantial.create(url, { credentials });
  const other = await Substantial.open(url, { credentials });
  try {
    await using _ = script(sub, [answer], { maxIdle: minutes(5), wait: seconds(3) });
    await eventually("a held listing", () => h3.holds >= 1);
    const began = performance.now();
    const run = await other.start(answer);
    assert.equal(await run.result({ timeout: seconds(10) }), 42);
    assert.ok(performance.now() - began < 3000, "woken by the change, not by the wait's end");
    assert.ok(h3.listed.some((l) => l.woken));
  } finally {
    await other.close();
    await sub.close();
    await h3.close();
  }
});

test("a credentials provider is called again near expiry, and after it failed", () =>
  withH3(async (h3) => {
    let calls = 0;
    let failing = false;
    const provider = () => {
      calls += 1;
      if (failing) throw new Error("no keys today");
      // Already within 5 minutes of its expiry: each request asks again.
      return {
        accessKeyId: `key-${calls}`,
        secretAccessKey: "secret",
        expiresAt: new Date(Date.now() + 60_000),
      };
    };
    const store = await openStore(h3.url(token()), provider);
    assert.equal(calls, 0);
    await store.create("q/a", bytes("a"), undefined);
    await value(store, "q/a");
    assert.equal(calls, 2);
    const ids = h3.requests.map(accessKeyId);
    assert.deepEqual(ids, ["key-1", "key-2"]);
    failing = true;
    await assert.rejects(store.get("q/a"), /credentials provider: no keys today/);
    assert.equal(calls, 3, "a provider that throws fails the request at once");
    assert.equal(h3.requests.length, 2, "no request without keys");
    failing = false;
    await value(store, "q/a");
    assert.equal(h3.requests.length, 3);
    await store.close();

    let lasting = 0;
    const late = await openStore(h3.url(token()), async () => {
      lasting += 1;
      return {
        accessKeyId: "lasting",
        secretAccessKey: "secret",
        expiresAt: new Date(Date.now() + 3_600_000),
      };
    });
    let never = 0;
    const forever = await openStore(h3.url(token()), () => {
      never += 1;
      return { accessKeyId: "forever", secretAccessKey: "secret" };
    });
    await Promise.all([late.get("a"), late.get("b"), forever.get("a"), forever.get("b")]);
    await value(late, "c");
    await value(forever, "c");
    assert.deepEqual([lasting, never], [1, 1], "keys far from expiry, or without one, are kept");
    await late.close();
    await forever.close();
  }));

test("a provider that throws fails the request and is asked again", () =>
  withH3(async (h3) => {
    let calls = 0;
    const flaky = () => {
      calls += 1;
      if (calls === 1) throw new Error("metadata endpoint down");
      return { accessKeyId: "key", secretAccessKey: "secret" };
    };
    const store = await openStore(h3.url(token()), flaky);
    await assert.rejects(
      store.get("runs/a/start"),
      (e) => e instanceof StoreFailure && /credentials provider: metadata/.test(e.message),
    );
    assert.equal(await value(store, "runs/a/start"), undefined);
    assert.equal(calls, 2);
    await store.close();
  }));

test("a provider that never answers fails each attempt within its timeout", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let calls = 0;
  const store = await openStore("s3+http://127.0.0.1:1/b/ns?region=r", () => {
    calls += 1;
    return new Promise<never>(() => {});
  });
  let outcome: unknown;
  store.get("k").then(
    () => {
      outcome = "settled";
    },
    (e) => {
      outcome = e;
    },
  );
  for (let i = 0; i < 100 && outcome === undefined; i++) {
    await new Promise((resolve) => setImmediate(resolve));
    t.mock.timers.tick(60_000);
  }
  await store.close();
  assert.ok(
    outcome instanceof StoreFailure &&
      /no keys within 60000 ms \(after 5 attempts\)/.test(outcome.message),
    String(outcome),
  );
  assert.equal(calls, 5, "asked again at each attempt");
});

test("a provider's keys are checked", async () => {
  const h3 = await FakeH3.start();
  try {
    const bad = [
      () => ({ accessKeyId: "", secretAccessKey: "s" }),
      () => ({ accessKeyId: "k", secretAccessKey: "s", expiresAt: "soon" }),
    ];
    for (const provider of bad) {
      const store = await openStore(h3.url(token()), provider as never);
      await assert.rejects(store.get("a"), /the credentials provider returned/);
      await store.close();
    }
    assert.equal(h3.requests.length, 0);
    await assert.rejects(
      Substantial.create("memory://provider", {
        credentials: () => bad[0]?.() as never,
      }),
      /takes no credentials/,
    );
  } finally {
    await h3.close();
  }
});

const countdown = workflow("countdown", async (c: Context, input: { n: number }) => {
  await c.sleep(seconds(1));
  if (input.n > 0) await c.continueAsNew({ n: input.n - 1 });
  return "liftoff";
});

test("a result is waited on its key through a continuation", async () => {
  const h3 = await FakeH3.start();
  const sub = await Substantial.create(h3.url("results"), { credentials });
  try {
    await using _ = script(sub, [countdown]);
    const run = await sub.start(countdown, { n: 1 });
    assert.equal(await run.result({ timeout: seconds(10) }), "liftoff");
    // The chain's head, rewritten at every commit, the continuation's included.
    const waits = h3.listed.filter((l) => l.prefix === `results/runs/${run.chain}`);
    const heads = h3.methods.filter((m) => m === "PUT").length;
    assert.ok(waits.length > 1, "a listing for the etag, then held ones");
    assert.ok(waits.length <= heads + 1, `${waits.length} listings for ${heads} writes`);
    assert.ok(waits.every((l) => l.headers["x-herald-wait-seconds"] !== undefined));
    assert.ok(waits.some((l) => l.woken));
  } finally {
    await sub.close();
    await h3.close();
  }
});

test("a get is not queued behind waiting listings", () =>
  withH3(async (h3, store) => {
    const first = await store.list("p0/", { maxKeys: 1, wait: { seconds: 0 } });
    // As 16 workers serving 4 workflows each hold 64.
    const wait = { seconds: 2, etag: first.etag as string };
    const held = Array.from({ length: 64 }, (_, i) => store.list(`p${i}/`, { maxKeys: 1, wait }));
    await eventually("64 held listings", () => h3.holds >= 64, 1000);
    const began = performance.now();
    assert.equal(await value(store, "k"), undefined);
    const took = performance.now() - began;
    await Promise.all(held);
    assert.ok(took < 500, `a get took ${Math.round(took)} ms behind 64 held listings`);
  }));

test("a run ends promptly with 4 workflows waiting", async () => {
  const h3 = await FakeH3.start();
  const first = workflow("pool-a", async () => 1);
  const flows = [first, ...["b", "c", "d"].map((n) => workflow(`pool-${n}`, async () => 1))];
  const sub = await Substantial.create(h3.url("pool"), { credentials });
  try {
    await using _ = sub.worker(flows, { preset: "script" });
    await eventually("a held listing per workflow", () => h3.holds >= 4);
    const began = performance.now();
    const run = await sub.start(first, {});
    assert.equal(await run.result({ timeout: seconds(10) }), 1);
    const took = performance.now() - began;
    assert.ok(took < 2000, `a run took ${Math.round(took)} ms`);
  } finally {
    await sub.close();
    await h3.close();
  }
});

test("an abandoned waiting listing is aborted, not retried", () =>
  withH3(async (h3, store) => {
    const first = await store.list("q/", { maxKeys: 1, wait: { seconds: 0 } });
    const abandoned = new AbortController();
    const wait = { seconds: 20, etag: first.etag as string };
    const held = store.list("q/", { maxKeys: 1, wait, signal: abandoned.signal });
    await eventually("a held listing", () => h3.holds >= 1);
    const began = performance.now();
    abandoned.abort();
    await assert.rejects(held, (e) => e instanceof StoreFailure && /abandoned/.test(e.message));
    assert.ok(performance.now() - began < 1000, "at once, not at the wait's end");
    assert.equal(h3.holds, 1, "not retried");
  }));

test("a bucket is used whatever its versioning", () =>
  withH3(async (h3) => {
    // The fake answers no versioning request: an adapter reading it would fail to open.
    const url = h3.url("unversioned");
    // § x79k6r9e5@b7d3r
    await (await Substantial.create(url, { credentials })).close();
    await (await Substantial.open(url, { credentials })).close();
  }));
