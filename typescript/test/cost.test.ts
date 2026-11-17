// What runs cost: prices, charges, `c.cost()` and budgets, in the test environment.
import assert from "node:assert/strict";
import { test } from "node:test";
import { costOf, priceOf } from "../src/cost.ts";
import * as s from "../src/mod.ts";
import { T0, withEnv } from "./testing_helpers.ts";

const S3 = "aws-s3-standard-us-east-1-2026-11-09";
const MAX = 2n ** 63n - 1n;
const service = { preset: "service" } as const;

/** A step that charges `nano` nano-dollars. */
async function pay(c: s.Context, nano: bigint): Promise<void> {
  await c.step("pay", () => s.charge("USD", nano));
}

/** Pays 6 nano-dollars `n` times, then returns its cost. */
function payer(name: string, budget?: s.Budget) {
  return s.workflow(
    name,
    async (c, input: { n: number }) => {
      for (let i = 0; i < input.n; i++) await pay(c, 6n);
      return plain(await c.cost());
    },
    budget === undefined ? {} : { budget },
  );
}

/** A cost as JSON carries it out of a run: amounts as decimal strings. */
function plain(cost: s.Cost) {
  const amounts = (a: Record<string, bigint>) =>
    Object.fromEntries(Object.entries(a).map(([k, v]) => [k, String(v)]));
  const meters = (m: s.Meters) => ({ ...m, amounts: amounts(m.amounts) });
  return {
    ...meters(cost),
    store: meters(cost.store),
    children: meters(cost.children),
    steps: Object.fromEntries(Object.entries(cost.steps).map(([k, v]) => [k, amounts(v)])),
  };
}

function budgetExceeded(e: unknown): boolean {
  return e instanceof s.RunFailed && e.status === "failed" && e.error.type === "BudgetExceeded";
}

test("the named prices and what prices refuse", () => {
  const named = new s.Prices(S3);
  assert.deepEqual(
    { ...named },
    { currency: "USD", create: 5_000_000n, get: 400_000n, list: 5_000_000n, delete: 0n },
  );
  assert.deepEqual(
    named,
    new s.Prices({
      currency: "USD",
      create: 5_000_000n,
      get: 400_000n,
      list: 5_000_000n,
      delete: 0n,
    }),
  );
  const overridden = new s.Prices(S3, { get: 500_000n });
  assert.deepEqual(
    [overridden.currency, overridden.get, overridden.list],
    ["USD", 500_000n, 5_000_000n],
  );
  assert.equal(new s.Prices(S3, { create: MAX }).create, MAX);
  assert.deepEqual(priceOf(named, "create", 2), new s.Amount("USD", 10_000n));
  assert.deepEqual(priceOf(named, "get", 1), new s.Amount("USD", 400n));
  assert.deepEqual(priceOf(named, "delete", 3), new s.Amount("USD", 0n));
  assert.equal(priceOf(new s.Prices(S3, { get: 1001n }), "get", 1).nano, 2n, "a share rounds up");
  const refused: (() => s.Prices)[] = [
    () => new s.Prices("zero"),
    () => new s.Prices({ currency: "USD", create: 1n, get: 1n, list: 1n } as never),
    () => new s.Prices(S3, { currency: "usd" }),
    () => new s.Prices(S3, { currency: "US1" }),
    () => new s.Prices(S3, { currency: "USDT" }),
    () => new s.Prices(S3, { create: -1n }),
    () => new s.Prices(S3, { create: MAX + 1n }),
    () => new s.Prices(S3, { create: 1 as never }),
    () => new s.Prices(S3, { get: "1" as never }),
    () => new s.Prices(S3, { put: 1n } as never),
    () => new s.Prices(5 as never),
  ];
  for (const prices of refused) assert.throws(prices, s.ConfigError);
});

test("prices are given to create and checked by the worker", async () => {
  await assert.rejects(
    s.Substantial.create("memory://prices-wrong", { prices: "USD" as never }),
    s.ConfigError,
  );
  await assert.rejects(
    s.Substantial.create("memory://prices-wrong", { observe: "USD" as never }),
    s.ConfigError,
  );
  await using _sub = await s.Substantial.create("memory://prices-fine", {
    prices: new s.Prices(S3),
  });
  // Three uppercase letters, not a currency: the worker refuses it when it starts.
  await using env = await s.testing.Env.open({
    now: T0,
    prices: new s.Prices(S3, { currency: "QQQ" }),
  });
  assert.throws(() => env.worker([payer("p")], service), s.ConfigError);
});

test("charges reach the cost by step and in total", async () => {
  const charged = s.workflow("charged", async (c) => {
    await c.step("pay", async () => {
      s.charge("USD", 5n);
      s.charge("EUR", 3n);
      s.charge("USD", 5n);
    });
    await c.step("fee", () => s.charge("USD", 1n));
    return plain(await c.cost());
  });
  await using env = await s.testing.Env.open({ now: T0, prices: new s.Prices(S3) });
  await using _worker = env.worker([charged], service);
  const cost = await (await env.start(charged)).result();
  assert.deepEqual(cost.steps, { fee: { USD: "1" }, pay: { EUR: "3", USD: "10" } });
  const store = BigInt(cost.store.amounts.USD ?? "0");
  assert.ok(store > 0n, JSON.stringify(cost));
  assert.deepEqual(cost.amounts, { EUR: "3", USD: String(store + 11n) });
  assert.ok(cost.requests > 0 && cost.requests === cost.store.requests, JSON.stringify(cost));
  assert.equal(cost.requests, cost.create + cost.get + cost.list + cost.delete);
  assert.equal(cost.children.requests, 0);
  await env.check();
});

test(
  "charge refuses a wrong amount and runs only in a step",
  withEnv(async (env) => {
    const refusing = s.workflow("refusing", async (c) => {
      const refused = await c.step("pay", () =>
        [MAX + 1n, -1n, 1, true, "1"].map((nano) => {
          try {
            s.charge("USD", nano as bigint);
            return false;
          } catch (e) {
            return e instanceof s.InvalidArgument;
          }
        }),
      );
      const key = await c.step("blocking", () => {
        s.charge("USD", 2n);
        return s.stepKey();
      });
      let outside: string;
      try {
        s.charge("USD", 1n);
        outside = "charged";
      } catch (e) {
        outside = (e as Error).name;
      }
      const { amounts } = plain(await c.cost());
      return { refused, key, outside, amounts };
    });
    await using _worker = env.worker([refusing], service);
    const out = await (await env.start(refusing, undefined, { id: "p" })).result();
    assert.deepEqual(out, {
      refused: [true, true, true, true, true],
      key: "p/blocking/0",
      outside: "NotInWorkflow",
      amounts: { USD: "2" },
    });
    assert.throws(() => s.charge("USD", 1n), s.NotInWorkflow);
  }),
);

test(
  "an unknown currency fails the step with InvalidArgument",
  withEnv(async (env) => {
    const lowercase = s.workflow("lowercase", async (c) => {
      try {
        await c.step("pay", () => s.charge("usd", 1n));
      } catch (e) {
        if (e instanceof s.StepFailed) return e.cause.type;
        throw e;
      }
      return "paid";
    });
    await using _worker = env.worker([lowercase], service);
    assert.equal(await (await env.start(lowercase)).result(), "InvalidArgument");
  }),
);

test(
  "the cost is recorded and replays the same",
  withEnv(async (env) => {
    const seen: number[] = [];
    const twice = s.workflow("twice", async (c) => {
      const before = await c.cost();
      seen.push(before.requests);
      await c.sleep(s.minutes(1));
      const after = await c.cost();
      return [before.requests, after.requests];
    });
    await using _worker = env.worker([twice], service);
    const run = await env.start(twice);
    await env.advance(s.minutes(1));
    const [before, after] = (await run.result()) as [number, number];
    assert.deepEqual(seen, [before, before], "the replay got the recorded value");
    assert.ok(after > before);
    assert.ok((await run.history()).every((e) => e.kind !== "budget_exceeded"));
  }),
);

test("costs read the core JSON", () => {
  const meters = { create: 1, get: 2, list: 3, delete: 0, requests: 6 };
  const none = { create: 0, get: 0, list: 0, delete: 0, requests: 0, amounts: {} };
  const cost = costOf({
    ...meters,
    amounts: { USD: "9007199254740993" },
    store: { ...meters, amounts: {} },
    steps: { pay: { USD: "5" } },
    children: none,
  });
  assert.ok(cost instanceof s.Cost && cost.store instanceof s.Meters);
  assert.deepEqual(cost.amounts, { USD: 9_007_199_254_740_993n });
  assert.deepEqual(cost.steps, { pay: { USD: 5n } });
  assert.deepEqual(cost.store, new s.Meters(1, 2, 3, 0, 6, {}));
  assert.equal(cost.children.requests, 0);
});

test(
  "a budget on requests passed throws and fails the run",
  withEnv(async (env) => {
    const spender = s.workflow("spender", async (c) => {
      try {
        for (let i = 0; i < 100; i++) {
          await c.step("s", () => i);
          await c.sleep(s.minutes(1));
        }
      } catch (e) {
        if (e instanceof s.BudgetExceeded) {
          c.publish("over", [e.meter, String(e.limit), String(e.used)]);
        }
        throw e;
      }
      return 0;
    });
    await using _worker = env.worker([spender], service);
    const run = await env.start(spender, undefined, { budget: { requests: 20n } });
    await env.advance(s.minutes(30));
    await assert.rejects(run.result(), budgetExceeded);
    assert.equal(await run.status(), "failed");
    const [meter, limit, used] = (await run.read("over")) as [string, string, string];
    assert.deepEqual([meter, limit], ["requests", "20"]);
    assert.ok(BigInt(used) > 20n);
  }),
);

test(
  "a budget in a currency is passed by charges",
  withEnv(async (env) => {
    const wf = payer("payer");
    await using _worker = env.worker([wf], service);
    const run = await env.start(wf, { n: 3 }, { budget: { USD: 10n } });
    await assert.rejects(run.result(), budgetExceeded);
  }),
);

test(
  "a declared budget, a start that overrides it, and the same id with another",
  withEnv(async (env) => {
    assert.throws(() => payer("declared", { Requests: 1n }), s.InvalidArgument);
    assert.throws(() => payer("declared", { requests: 1 as never }), s.InvalidArgument);
    const wf = payer("declared", { USD: 10n });
    await using _worker = env.worker([wf], service);
    await assert.rejects((await env.start(wf, { n: 3 }, { id: "d" })).result(), budgetExceeded);
    const given = await env.start(wf, { n: 3 }, { id: "g", budget: { USD: 100n } });
    assert.deepEqual((await given.result()).amounts, { USD: "18" });
    // The budget is part of the start's identity.
    await env.start(wf, { n: 3 }, { id: "g", budget: { USD: 100n } });
    await assert.rejects(env.start(wf, { n: 3 }, { id: "g" }), s.RunExists);
    const malformed = [{ Requests: 1n }, { get: MAX + 1n }, { get: 1 }, { QQQ: 1n }, ["USD"]];
    for (const budget of malformed) {
      await assert.rejects(env.start(wf, { n: 3 }, { budget: budget as never }), s.InvalidArgument);
    }
  }),
);

test(
  "a retry with a larger budget completes and keeps the cost spent",
  withEnv(async (env) => {
    const wf = payer("retried");
    await using _worker = env.worker([wf], service);
    const run = await env.start(wf, { n: 3 }, { budget: { USD: 10n } });
    await assert.rejects(run.result(), budgetExceeded);
    await assert.rejects(run.retry({ budget: { USD: 1 as never } }), s.InvalidArgument);
    await run.retry({ budget: { USD: 100n } });
    await assert.rejects(run.retry({ budget: { USD: 1000n } }), s.InvalidArgument);
    // The two payments of the failed run, and the retry's.
    assert.deepEqual((await run.result()).amounts, { USD: "18" });
  }),
);

test(
  "a child's cost reaches its parent",
  withEnv(async (env) => {
    const kid = payer("kid");
    const parent = s.workflow("parent", async (c) => {
      const child = await c.start(kid, { n: 2 });
      const theirs = await child.result();
      return [theirs, plain(await c.cost())] as const;
    });
    await using _worker = env.worker([parent, kid], service);
    const [theirs, mine] = await (await env.start(parent)).result();
    assert.deepEqual(mine.children.amounts, { USD: "12" });
    assert.ok(mine.children.requests >= theirs.requests, JSON.stringify(mine));
    assert.deepEqual(mine.amounts, { USD: "12" });
    assert.deepEqual(mine.steps, {});
  }),
);

test(
  "a child start takes its own budget, else its workflow's",
  withEnv(async (env) => {
    const kid = payer("kid", { USD: 100n });
    const parent = s.workflow("parent", async (c) => {
      const out: string[] = [];
      for (const budget of [{ USD: 5n }, undefined]) {
        // The second payment's attempt does not start past the limit.
        const child = await c.start(kid, { n: 2 }, budget === undefined ? {} : { budget });
        try {
          await child.result();
          out.push("completed");
        } catch (e) {
          if (!(e instanceof s.RunFailed)) throw e;
          out.push(e.error.type);
        }
      }
      return out;
    });
    await using _worker = env.worker([parent, kid], service);
    assert.deepEqual(await (await env.start(parent)).result(), ["BudgetExceeded", "completed"]);
  }),
);

test(
  "a child start past the budget throws at once",
  withEnv(async (env) => {
    const kid = payer("kid");
    const parent = s.workflow("parent", async (c) => {
      await pay(c, 20n);
      let started: string;
      try {
        await c.start(kid, { n: 0 });
        started = "started";
      } catch (e) {
        if (!(e instanceof s.BudgetExceeded)) throw e;
        started = e.meter;
      }
      c.publish("start", started);
      return started;
    });
    await using _worker = env.worker([parent, kid], service);
    const run = await env.start(parent, undefined, { budget: { USD: 10n } });
    await assert.rejects(run.result(), budgetExceeded);
    assert.equal(await run.read("start"), "USD");
  }),
);

test(
  "a run failed by its budget or a step gives the failure details",
  withEnv(async (env) => {
    const over = s.workflow("over", async (c) => {
      await pay(c, 1_500_000n);
      await c.step("after", () => {});
    });
    class Declined extends s.Permanent {}
    const declined = s.workflow("declined", async (c) => {
      await c.step("pay", () => {
        throw new Declined("not today");
      });
    });
    await using _worker = env.worker([over, declined], service);
    const run = await env.start(over, undefined, { budget: { USD: 1_000_000n } });
    const budget = await run.result().then(
      () => assert.fail("the run completed"),
      (e: unknown) => e,
    );
    assert.ok(budgetExceeded(budget), String(budget));
    assert.deepEqual((budget as s.RunFailed).error.details, {
      meter: "USD",
      limit: "1000000",
      used: "1500000",
    });
    const stepped = await (await env.start(declined)).result().then(
      () => assert.fail("the run completed"),
      (e: unknown) => e,
    );
    assert.ok(stepped instanceof s.RunFailed, String(stepped));
    assert.equal(stepped.error.type, "StepFailed");
    assert.equal(stepped.error.details?.step, "pay");
    assert.deepEqual(stepped.cause, { type: "Declined", message: "not today", retryable: false });
  }),
);
