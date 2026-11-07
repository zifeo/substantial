// The TypeScript side of the cross-language run, driven by dev/cross/run.py.
//
//   node --conditions=substantial-source dev/cross/cross.ts worker <url>   serves the workflow until SIGTERM
//   node --conditions=substantial-source dev/cross/cross.ts client <url>   starts a run, messages it and prints its result as JSON
//   node --conditions=substantial-source dev/cross/cross.ts start <url>    starts the run `same-input`, its input written its own way, and prints its chain and status as JSON
import { type Context, milliseconds, minutes, Substantial, step, workflow } from "substantial";

const LANGUAGE = "typescript";

type Input = { name: string; delay_ms: number };
type Suffix = { text: string; times: number };
type Output = { greeting: string; suffix: string; worker: string };

const greet = step("greet", (name: string) => `hello ${name}`);

const repeat = step("repeat", (text: string, times: number) => text.repeat(times));

const cross = workflow<Input, Output>(
  "cross",
  async (c: Context, input: Input) => {
    const greeting = await greet(input.name);
    await c.sleep(milliseconds(input.delay_ms));
    const suffix = await c.receive<Suffix>("suffix");
    return { greeting, suffix: await repeat(suffix.text, suffix.times), worker: LANGUAGE };
  },
  { version: 1 },
);

const [mode, url] = process.argv.slice(2);
if ((mode !== "worker" && mode !== "client" && mode !== "start") || url === undefined) {
  console.error("usage: cross.ts worker|client|start <url>");
  process.exit(2);
}

const sub = await Substantial.create(url, {
  partitions: 1,
  ...(url.startsWith("s3") ? { credentials: "env" as const } : {}),
});
if (mode === "worker") {
  await sub.worker([cross], { preset: "script" }).serve();
} else if (mode === "start") {
  const run = await sub.start(cross, { name: "ada", delay_ms: 200 }, { id: "same-input" });
  console.log(JSON.stringify({ chain: run.chain, status: await run.status() }));
} else {
  const run = await sub.start(cross, { name: "ada", delay_ms: 200 }, { id: "cross-1" });
  await run.send("suffix", { text: "!", times: 3 }, { id: "suffix-1" });
  const result = await run.result({ timeout: minutes(1) });
  const status = await run.status();
  console.log(JSON.stringify({ result, status, client: LANGUAGE }));
}
await sub.close();
