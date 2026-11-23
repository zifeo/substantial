// Schedules: the cron arithmetic against the vectors every host checks (`dev/cross/cron.json`),
// and `c.schedule()` with `sub.schedule` on the test environment, ported from the Python tests
// under the same names.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import * as s from "../src/mod.ts";
import { config, iso, messageId, nextFire, type Plan, parse, plan } from "../src/schedule.ts";
import { blockedIn, withEnv } from "./testing_helpers.ts";

interface Vectors {
  next: { cron: string; tz: string; after: string; fires: string[] }[];
  invalid: { cron: string; tz: string }[];
  plan: {
    cron: string;
    tz: string;
    after: string;
    now: string;
    backfill: number;
    result: Plan;
  }[];
}

const VECTORS: Vectors = JSON.parse(
  readFileSync(new URL("../../dev/cross/cron.json", import.meta.url), "utf8"),
);
const HOURLY = new s.Cron("0 * * * *", "UTC");
const service = { preset: "service" } as const;

for (const v of VECTORS.next) {
  test(`fire times match the shared vectors [${v.cron} ${v.tz}]`, () => {
    const cron = new s.Cron(v.cron, v.tz);
    const fires: string[] = [];
    let t = parse(v.after);
    for (const _ of v.fires) {
      t = nextFire(cron, t);
      fires.push(iso(t));
    }
    assert.deepEqual(fires, v.fires);
  });
}

for (const v of VECTORS.invalid) {
  test(`invalid crons and zones are refused [${JSON.stringify(v.cron)} ${v.tz}]`, () => {
    assert.throws(() => new s.Cron(v.cron, v.tz), s.InvalidArgument);
  });
}

for (const v of VECTORS.plan) {
  test(`plans of missed fires match the shared vectors [${v.cron} ${v.now}]`, () => {
    assert.deepEqual(plan(v.cron, v.tz, v.after, v.now, v.backfill), v.result);
  });
}

const fired: string[] = [];

const ticker = s.workflow("ticker", async (c) => {
  for await (const at of c.schedule()) {
    await c.step("tick", () => {
      fired.push(at.toISOString());
    });
  }
});

/** Clears what fired, then runs `fn` in a fresh environment at `T0`. */
function scheduled(fn: (env: s.testing.Env) => Promise<void>): () => Promise<void> {
  return withEnv(async (env) => {
    fired.length = 0;
    await fn(env);
  });
}

function hours(...hs: number[]): string[] {
  return hs.map((h) => new Date(Date.UTC(2030, 0, 7, h)).toISOString());
}

test(
  "a schedule fires at each cron time",
  scheduled(async (env) => {
    await using _worker = env.worker([ticker], service);
    const run = await env.schedule(ticker, "tick", HOURLY, { revision: 1 });
    await env.advance(s.hours(3));
    assert.deepEqual(fired, hours(10, 11, 12));
    assert.deepEqual(await run.read("schedule"), {
      revision: 1,
      cron: "0 * * * *",
      tz: "UTC",
      backfill: 5,
      last: "2030-01-07T12:00:00.000Z",
      recent: ["2030-01-07T10:00:00.000Z", "2030-01-07T11:00:00.000Z", "2030-01-07T12:00:00.000Z"],
      skipped: { count: 0, oldest: null, newest: null },
      next: "2030-01-07T13:00:00.000Z",
    });
  }),
);

test(
  "missed fires are backfilled oldest first up to backfill",
  scheduled(async (env) => {
    let run: s.Run;
    {
      await using _worker = env.worker([ticker], service);
      run = await env.schedule(ticker, "tick", HOURLY, { revision: 1 });
      await env.settle();
    }
    await env.advance(s.hours(8));
    assert.deepEqual(fired, []);
    {
      await using _worker = env.worker([ticker], service);
      await env.settle();
    }
    assert.deepEqual(fired, hours(13, 14, 15, 16, 17));
    const published = (await run.read("schedule")) as Record<string, unknown>;
    assert.deepEqual(published.skipped, {
      count: 3,
      oldest: "2030-01-07T10:00:00.000Z",
      newest: "2030-01-07T12:00:00.000Z",
    });
    assert.equal(published.next, "2030-01-07T18:00:00.000Z");
  }),
);

test(
  "backfill is part of the config",
  scheduled(async (env) => {
    {
      await using _worker = env.worker([ticker], service);
      await env.schedule(ticker, "tick", HOURLY, { revision: 1, backfill: 2 });
      await env.settle();
    }
    await env.advance(s.hours(4));
    {
      await using _worker = env.worker([ticker], service);
      await env.settle();
    }
    assert.deepEqual(fired, hours(12, 13));
  }),
);

test(
  "a config applies only if its revision is higher",
  scheduled(async (env) => {
    await using _worker = env.worker([ticker], service);
    const run = await env.schedule(ticker, "tick", HOURLY, { revision: 2 });
    await env.settle();
    const stale = { revision: 1, cron: "*/10 * * * *", tz: "UTC", backfill: 5 };
    await run.send("schedule", stale);
    await env.advance(s.hours(1));
    assert.deepEqual(fired, hours(10));
    assert.equal(((await run.read("schedule")) as { revision: number }).revision, 2);
  }),
);

test(
  "sub schedule does nothing twice and refuses a stale or another config",
  scheduled(async (env) => {
    await using _worker = env.worker([ticker], service);
    await env.schedule(ticker, "tick", HOURLY, { revision: 1 });
    await env.settle();
    await env.schedule(ticker, "tick", HOURLY, { revision: 1 });
    await assert.rejects(
      env.schedule(ticker, "tick", new s.Cron("0 * * * *", "Europe/Zurich"), { revision: 1 }),
      (e) =>
        e instanceof s.ConfigError && /revision 1 is applied with another config/.test(e.message),
    );
    await env.schedule(ticker, "tick", new s.Cron("30 * * * *", "UTC"), { revision: 2 });
    await env.settle();
    await assert.rejects(
      env.schedule(ticker, "tick", HOURLY, { revision: 1 }),
      (e) =>
        e instanceof s.ConfigError &&
        /revision 2 is applied already, higher than 1/.test(e.message),
    );
    await env.advance(s.hours(1));
    assert.deepEqual(fired, ["2030-01-07T09:30:00.000Z"]);
  }),
);

test(
  "a malformed schedule message blocks the run",
  scheduled(async (env) => {
    const run = await env.schedule(ticker, "tick", HOURLY, { revision: 1 });
    await run.send("schedule", { revision: 2, cron: "0 * * * *" });
    await using _worker = env.worker([ticker], service);
    const error = await blockedIn(env, ticker, run);
    assert.equal(error.type, "InvalidArgument");
    assert.match(error.message, /a schedule message is a config of sub.schedule/);
  }),
);

test("message ids are the same in every host", async () => {
  const hourly = new s.Cron("0 * * * *", "UTC");
  assert.equal(await messageId(config(1, hourly, 5)), "schedule-1-0a984120fb588a41");
  assert.equal(await messageId(config(2, null, 1)), "schedule-2-1669c891556cdd13");
});

test(
  "two configs of one revision block the run",
  scheduled(async (env) => {
    await env.schedule(ticker, "tick", HOURLY, { revision: 1 });
    const run = await env.schedule(ticker, "tick", new s.Cron("30 * * * *", "UTC"), {
      revision: 1,
    });
    await using _worker = env.worker([ticker], service);
    const error = await blockedIn(env, ticker, run);
    assert.equal(error.type, "ConfigError");
    assert.match(error.message, /revision 1 came with two configs/);
  }),
);

test(
  "null disables a schedule and a new cron fires from when it applies",
  scheduled(async (env) => {
    await using _worker = env.worker([ticker], service);
    const run = await env.schedule(ticker, "tick", HOURLY, { revision: 1 });
    await env.advance(s.hours(1));
    await env.schedule(ticker, "tick", null, { revision: 2 });
    await env.advance(s.hours(3));
    assert.deepEqual(fired, hours(10));
    assert.equal(((await run.read("schedule")) as { cron: string | null }).cron, null);
    assert.equal(await run.status(), "suspended");
    await env.schedule(ticker, "tick", HOURLY, { revision: 3 });
    await env.advance(s.hours(1));
    assert.deepEqual(fired, hours(10, 14));
  }),
);

test(
  "a schedule crosses compressions",
  scheduled(async (env) => {
    await using _worker = env.worker([ticker], { ...service, continueAsNewAfter: 0 });
    const run = await env.schedule(ticker, "tick", HOURLY, { revision: 1 });
    await env.advance(s.hours(5));
    await env.schedule(ticker, "tick", HOURLY, { revision: 2, backfill: 1 });
    await env.advance(s.hours(2));
    assert.deepEqual(fired, hours(10, 11, 12, 13, 14, 15, 16));
    const published = (await run.read("schedule")) as {
      revision: number;
      backfill: number;
      recent: string[];
    };
    assert.deepEqual([published.revision, published.backfill, published.recent.length], [2, 1, 7]);
  }),
);

test(
  "the schedule step name is reserved",
  scheduled(async (env) => {
    assert.throws(
      () => s.step("schedule.next", () => null),
      (e) => e instanceof s.InvalidArgument && /reserved/.test(e.message),
    );
    const inline = s.workflow("inline", async (c) => {
      await c.step("schedule.next", () => null);
    });
    await using _worker = env.worker([inline], service);
    const run = await env.start(inline);
    assert.match((await blockedIn(env, inline, run)).message, /reserved/);
  }),
);

test(
  "sub schedule checks its arguments",
  scheduled(async (env) => {
    for (const options of [{ revision: 0 }, { revision: 1, backfill: 0 }, { revision: true }]) {
      await assert.rejects(
        env.schedule(ticker, "tick", HOURLY, options as s.ScheduleOptions),
        s.InvalidArgument,
      );
    }
    await assert.rejects(
      env.schedule(ticker, "tick", "0 * * * *" as unknown as s.Cron, { revision: 1 }),
      (e) => e instanceof s.InvalidArgument && /s\.Cron/.test(e.message),
    );
  }),
);

test(
  "a config publishes its next fire at once",
  scheduled(async (env) => {
    await using _worker = env.worker([ticker], service);
    const run = await env.schedule(ticker, "tick", HOURLY, { revision: 1 });
    await env.settle();
    const published = (await run.read("schedule")) as Record<string, unknown>;
    assert.deepEqual([published.next, published.last], ["2030-01-07T10:00:00.000Z", null]);
    await env.schedule(ticker, "tick", null, { revision: 2 });
    await env.settle();
    assert.equal(((await run.read("schedule")) as Record<string, unknown>).next, null);
  }),
);
