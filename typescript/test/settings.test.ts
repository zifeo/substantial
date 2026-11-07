// Presets and their overrides (I-18), validated strictly; the adapters' retries (I-26).
import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigError, InvalidArgument, milliseconds, minutes, seconds } from "../src/mod.ts";
import { type InternalOptions, presetSettings, resolveSettings } from "../src/settings.ts";
import * as retry from "../src/stores/retry.ts";
import { Ambiguous, Refused, Transient } from "../src/stores/retry.ts";
import { StoreFailure } from "../src/stores/store.ts";

test("presets differ where the table says", () => {
  const script = presetSettings("script", false);
  const service = presetSettings("service", false);
  assert.deepEqual([script.concurrency, service.concurrency], [1, 16]);
  assert.deepEqual(script.maxIdle, seconds(1));
  assert.deepEqual(service.maxIdle, seconds(30));
  assert.deepEqual(presetSettings("service", true).maxIdle, minutes(5));
  assert.deepEqual([script.shutdownGrace, service.shutdownGrace], [seconds(5), seconds(25)]);
  assert.deepEqual(script.continueAsNewAfter, { segments: 1000, bytes: 8 * 1024 * 1024 });
  assert.deepEqual(script.wait, seconds(20));
  assert.equal(script.duePage, 100);
  assert.deepEqual([script.stepTimeout, service.stepTimeout], [minutes(5), minutes(5)]);
});

test("overrides replace and refuse", () => {
  const s = resolveSettings(
    {
      preset: "script",
      concurrency: 4,
      commitEvery: milliseconds(250),
      continueAsNewAfter: 0,
      duePage: 1000,
      wait: seconds(1),
    },
    false,
    true,
  );
  assert.equal(s.concurrency, 4);
  assert.deepEqual(s.commitEvery, milliseconds(250));
  assert.deepEqual(s.continueAsNewAfter, { segments: 0, bytes: 0 });
  const refused: Record<string, unknown>[] = [
    { concurency: 4 },
    { commitEvery: 250 },
    { inlineMax: 1024 },
    { concurrency: 0 },
    { concurrency: true },
    { concurrency: 1.5 },
    { concurrency: undefined },
    { claimTtl: milliseconds(0) },
    { stepTimeout: milliseconds(0) },
    { stepTimeout: 1000 },
    { maxIdle: 1 },
    { duePage: 0 },
    { duePage: 1001 },
    { wait: milliseconds(0) },
    { wait: seconds(21) },
    { wait: milliseconds(1500) },
    { continueAsNewAfter: 1 },
    { continueAsNewAfter: { segments: 1 } },
    { continueAsNewAfter: { segments: 1, bytes: 2, more: 3 } },
  ];
  for (const override of refused) {
    assert.throws(
      () => resolveSettings({ preset: "script", ...override } as InternalOptions, false, true),
      ConfigError,
      JSON.stringify(override),
    );
  }
  assert.throws(() => resolveSettings({ preset: "daemon" } as never, false, true), ConfigError);
  assert.throws(() => resolveSettings({} as never, false, true), ConfigError);
  assert.throws(() => seconds(-1), InvalidArgument);
});

test("only the public settings are given to a worker", () => {
  const s = resolveSettings(
    {
      preset: "service",
      concurrency: 2,
      maxIdle: seconds(2),
      shutdownGrace: seconds(3),
      claimTtl: seconds(4),
      continueAsNewAfter: { segments: 5, bytes: 6 },
      stepTimeout: seconds(7),
    },
    false,
    false,
  );
  assert.deepEqual(
    [s.concurrency, s.maxIdle, s.shutdownGrace, s.claimTtl, s.continueAsNewAfter, s.stepTimeout],
    [2, seconds(2), seconds(3), seconds(4), { segments: 5, bytes: 6 }, seconds(7)],
  );
  const hidden: Record<string, unknown> = {
    commitEvery: milliseconds(250),
    claimSkew: seconds(1),
    wait: seconds(1),
    duePage: 10,
    duePages: 20,
    snapshotEvery: 8,
    dedupWindow: seconds(1),
    snapshotGrace: seconds(1),
    storeRetry: { attempts: 3, initial: milliseconds(10), max: seconds(1) },
  };
  for (const [name, value] of Object.entries(hidden)) {
    assert.throws(
      () => resolveSettings({ preset: "script", [name]: value } as never, false, false),
      (e) => e instanceof ConfigError && e.message === `unknown settings ["${name}"]`,
      name,
    );
  }
  assert.throws(
    () => resolveSettings({ preset: "script", snapshotGrace: seconds(1) } as never, false, true),
    ConfigError,
    "snapshotGrace is gone",
  );
});

const POLICY = { attempts: 3, initial: milliseconds(1), max: milliseconds(5) };

function script<T>(steps: (T | Error)[]): () => Promise<T> {
  const queue = [...steps];
  return async () => {
    assert.ok(queue.length > 0, "unexpected call");
    const outcome = queue.shift() as T | Error;
    if (outcome instanceof Error) throw outcome;
    return outcome;
  };
}

test("transient failures are retried, then fail", async () => {
  assert.equal(await retry.withRetry(POLICY, script([new Transient("a"), 7])), 7);
  await assert.rejects(
    retry.withRetry(
      POLICY,
      script([new Transient("down"), new Transient("down"), new Transient("down")]),
    ),
    (e) => e instanceof StoreFailure && /down \(after 3 attempts\)/.test(e.message),
  );
  await assert.rejects(
    retry.withRetry({ ...POLICY, attempts: 1 }, script([new Transient("down")])),
    (e) => e instanceof StoreFailure && /down \(after 1 attempt\)$/.test(e.message),
  );
  await assert.rejects(retry.withRetry(POLICY, script([new Refused("bad key"), 1])), /bad key/);
});

const mine = new TextEncoder().encode("mine");
const theirs = new TextEncoder().encode("theirs");

function resolve(
  creates: ("created" | "exists" | Error)[],
  gets: (Uint8Array | undefined | Error)[],
) {
  return retry.create(POLICY, mine, script(creates), script(gets));
}

test("ambiguous creates are resolved with a get", async () => {
  const lost = () => new Ambiguous("timeout");
  assert.equal(await resolve([lost()], [mine]), "created");
  assert.equal(await resolve([lost()], [theirs]), "exists");
  assert.equal(await resolve([lost(), "created"], [undefined]), "created");
  assert.equal(await resolve([lost(), "exists"], [undefined, mine]), "created", "landed late");
  assert.equal(await resolve([lost()], [new Transient("get"), mine]), "created");
  await assert.rejects(resolve([lost(), lost()], [undefined, new Transient("x")]), StoreFailure);
  assert.equal(await resolve(["exists"], []), "exists", "no ambiguity, no get");
  assert.equal(await resolve([new Transient("409"), "created"], []), "created");
});
