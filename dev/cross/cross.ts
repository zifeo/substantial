// The TypeScript side of the cross-language run, driven by dev/cross/run.py.
//
//   node --conditions=substantial-source dev/cross/cross.ts worker <url>   serves the workflows until SIGTERM
//   node --conditions=substantial-source dev/cross/cross.ts client <url>   starts a run with a budget, waits until it waits on messages alone, messages it and prints its result and the kinds of its history as JSON, with the failure of a run of `over` that passes its budget and the config a run of `ticks` published
//   node --conditions=substantial-source dev/cross/cross.ts start <url>    starts the run `same-input`, its input written its own way, and prints its chain and status as JSON
import {
  type Context,
  charge,
  ConfigError,
  Cron,
  type Failure,
  milliseconds,
  minutes,
  RunFailed,
  Substantial,
  step,
  workflow,
} from "substantial";

const LANGUAGE = "typescript";

type Input = { name: string; delay_ms: number };
type Suffix = { text: string; times: number };
type Output = {
  greeting: string;
  suffix: string;
  worker: string;
  charged: string;
  counted: boolean;
};

const greet = step("greet", (name: string) => `hello ${name}`);

const repeat = step("repeat", (text: string, times: number) => {
  charge("USD", 1500000n);
  return text.repeat(times);
});

const cross = workflow<Input, Output>(
  "cross",
  async (c: Context, input: Input) => {
    const greeting = await greet(input.name);
    await c.sleep(milliseconds(input.delay_ms));
    const suffix = await c.receive<Suffix>("suffix");
    const repeated = await repeat(suffix.text, suffix.times);
    const cost = await c.cost();
    const charged = cost.steps.repeat?.USD;
    if (charged === undefined) throw new Error("the cost has no USD charged by repeat");
    return {
      greeting,
      suffix: repeated,
      worker: LANGUAGE,
      charged: charged.toString(),
      counted: cost.requests > 0,
    };
  },
  { version: 1 },
);

/** Charges more than a budget of USD 1000000 allows, so its next step is refused. */
const over = workflow<{ text: string }, string>(
  "over",
  async (_c: Context, input: { text: string }) => greet(await repeat(input.text, 1)),
  { version: 1 },
);

type Overrun = { type: string; meter: unknown; limit: unknown; used: unknown };

/** Starts `cross-over` with a budget it passes; its failure as its result gives it, its history
 * ending failed. */
async function overrun(): Promise<Overrun> {
  const run = await sub.start(over, { text: "!" }, { id: "cross-over", budget: { USD: 1000000n } });
  let failure: Failure;
  try {
    await run.result({ timeout: minutes(1) });
    throw new Error("cross-over completed within a budget it passes");
  } catch (e) {
    if (!(e instanceof RunFailed)) throw e;
    failure = e.error;
  }
  const ended = (await run.history()).at(-1);
  if (ended?.kind !== "failed") {
    throw new Error(`cross-over failed, its history ending with ${ended?.kind}`);
  }
  const { details } = failure;
  if (details === undefined) throw new Error("cross-over failed without details");
  return { type: failure.type, meter: details.meter, limit: details.limit, used: details.used };
}

const ticks = workflow(
  "ticks",
  async (c: Context) => {
    for await (const _ of c.schedule()) {
      // Each fire does nothing: the case checks the config's way across hosts.
    }
  },
  { version: 1 },
);

const NIGHTLY = new Cron("0 3 * * *", "Europe/Zurich");

/** Configures `cross-ticks`, served by another host, until it publishes the config; the same call
 * again does nothing, an older revision is refused. */
async function scheduled(): Promise<Record<string, unknown>> {
  const run = await sub.schedule(ticks, "cross-ticks", NIGHTLY, { revision: 2, backfill: 3 });
  let published: Record<string, unknown> | undefined;
  for (;;) {
    published = (await run.read("schedule")) as Record<string, unknown> | undefined;
    if (published?.revision === 2) break;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  await sub.schedule(ticks, "cross-ticks", NIGHTLY, { revision: 2, backfill: 3 });
  let stale = "accepted";
  try {
    await sub.schedule(ticks, "cross-ticks", NIGHTLY, { revision: 1 });
  } catch (e) {
    if (!(e instanceof ConfigError)) throw e;
    stale = "ConfigError";
  }
  const { revision, cron, tz, backfill, last, recent } = published;
  return { revision, cron, tz, backfill, last, recent, stale };
}

const awaited = workflow(
  "awaited",
  async (c: Context) => ({ worker: LANGUAGE, go: await c.receive<string>("go") }),
  { version: 1 },
);

/** `watch-<language>`, which awaits the end of a chain it did not start: served by that
 * language's worker, a stub elsewhere. */
function watcher(language: string) {
  return workflow(
    `watch-${language}`,
    async (c: Context, input: { chain: string }) => ({
      watcher: LANGUAGE,
      result: await c.result(input.chain),
    }),
    { version: 1 },
  );
}

/** Starts `cross-awaited`, served by the worker, and `cross-watch`, served by the watcher of
 * `language`, which awaits it; once the watcher waits, lets `cross-awaited` end. */
async function watched(language: string): Promise<unknown> {
  const run = await sub.start(awaited, {}, { id: "cross-awaited" });
  const watch = await sub.start(watcher(language), { chain: run.chain }, { id: "cross-watch" });
  while ((await watch.status()) !== "suspended") {
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  await run.send("go", "now");
  return watch.result({ timeout: minutes(1) });
}

/** Until `cross-1`, served by another host, is listed waiting on messages alone. */
async function waitsOnMessages(): Promise<void> {
  for (;;) {
    for await (const info of sub.runs(cross)) {
      if (info.run === "cross-1" && info.waitingOn === "message") return;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

const [mode, url, watching] = process.argv.slice(2);
const modes = ["worker", "watcher", "client", "start"];
if (!modes.includes(mode) || url === undefined || (mode === "client") !== (watching !== undefined)) {
  console.error("usage: cross.ts worker|watcher|start <url>, cross.ts client <url> <watcher>");
  process.exit(2);
}

const sub = await Substantial.create(
  url,
  url.startsWith("s3") ? { credentials: "env" as const } : {},
);
if (mode === "worker") {
  await sub.worker([cross, over, ticks, awaited], { preset: "script" }).serve();
} else if (mode === "watcher") {
  await sub.worker([watcher(LANGUAGE)], { preset: "script" }).serve();
} else if (mode === "start") {
  const run = await sub.start(cross, { name: "ada", delay_ms: 200 }, { id: "same-input" });
  console.log(JSON.stringify({ chain: run.chain, status: await run.status() }));
} else {
  const budget = { requests: 100000n, USD: 10000000n };
  const run = await sub.start(cross, { name: "ada", delay_ms: 200 }, { id: "cross-1", budget });
  await waitsOnMessages();
  await run.send("suffix", { text: "!", times: 3 }, { id: "suffix-1" });
  const result = await run.result({ timeout: minutes(1) });
  const status = await run.status();
  const history = (await run.history()).map((e) => e.kind);
  const failed = await overrun();
  const schedule = await scheduled();
  const seen = await watched(watching);
  const line = { result, status, history, over: failed, schedule, watched: seen, client: LANGUAGE };
  console.log(JSON.stringify(line));
}
await sub.close();
