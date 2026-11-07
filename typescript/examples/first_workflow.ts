// The workflow of I-3 on a file store, to try durability by hand:
//
//   node --conditions=substantial-source typescript/examples/first_workflow.ts /tmp/substantial-demo-ts
//
// Stop it with Ctrl-C during the sleep and run it again: the run resumes, and the steps it had
// recorded do not print again.
import * as s from "substantial";

const roulette = s.step(
  "roulette",
  () => {
    console.log("step roulette");
    if (Math.random() < 1 / 6) throw new Error("shot!");
    return 100;
  },
  {
    retry: { maxAttempts: 6, initial: s.seconds(1), max: s.minutes(1), multiplier: 2 },
  },
);

const multiply = s.step("multiply", (a: number, b: number) => {
  console.log(`step multiply(${a}, ${b})`);
  return a * b;
});

export const example = s.workflow("example", async (c) => {
  let res = await roulette();
  console.log("at the sleep (code outside steps runs again on every replay)");
  await c.sleep(s.seconds(10));
  res = await multiply(res, 0.5);

  const n = await c.receive<number>("by");
  res = await multiply(res, n);

  let stopped = false;
  c.handle("stop", () => {
    stopped = true;
  });
  await c.waitUntil(() => stopped);
  return res;
});

const directory = process.argv[2];
if (directory === undefined) {
  console.error("usage: first_workflow.ts <directory>");
  process.exit(2);
}

await using sub = await s.Substantial.create(`file:${directory}`, { partitions: 1 });
await using _worker = sub.worker([example], { preset: "script" });
// The id makes the start idempotent: running the script again finds the same run. A send to a
// run that ended throws RunEnded, so the messages go only to a run still going.
const run = await sub.start(example, {}, { id: "example-1" });
if (!["completed", "failed", "cancelled"].includes(await run.status())) {
  await run.send("by", 2, { id: "by-1" });
  await run.send("stop", null, { id: "stop-1" });
}
console.log("Final output", await run.result({ timeout: s.minutes(2) }));
console.log("status", await run.status());
