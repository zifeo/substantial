// The examples: I-3 resumes after a kill during its sleep, its recorded steps not run again, and
// runs again once ended; I-4 runs to its end.
import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { rm } from "node:fs/promises";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { fromBinary } from "@bufbuild/protobuf";
import { layout } from "../src/proto.ts";
import { tempDir, token } from "./helpers.ts";

const EXAMPLE = fileURLToPath(new URL("../examples/first_workflow.ts", import.meta.url));

/** Whether a segment of the run holds the value of `roulette`: a kill then loses nothing. */
function rouletteRecorded(directory: string): boolean {
  const log = join(directory, "runs", "example-1", "log");
  if (!existsSync(log)) return false;
  return readdirSync(log).some((name) => {
    const segment = fromBinary(layout.SegmentSchema, readFileSync(join(log, name)));
    return segment.events.some((e) => e.kind.case === "step" && e.kind.value.name === "roulette");
  });
}

function example(directory: string): {
  child: ChildProcess;
  output: () => string;
  exited: Promise<number | null>;
} {
  const child = spawn(process.execPath, ["--conditions=substantial-source", EXAMPLE, directory], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout?.on("data", (chunk) => {
    output += chunk;
  });
  child.stderr?.on("data", (chunk) => {
    output += chunk;
  });
  const exited = new Promise<number | null>((resolve) => child.on("exit", resolve));
  return { child, output: () => output, exited };
}

const BACKUP = fileURLToPath(new URL("../examples/backup.ts", import.meta.url));

test("the backup of I-4 runs on memory and file", { timeout: 90_000 }, async () => {
  const dir = tempDir();
  try {
    for (const url of [`memory://backup-${token(4)}`, `file:${join(dir, "backup")}`]) {
      const child = spawn(process.execPath, ["--conditions=substantial-source", BACKUP, url], {
        stdio: ["ignore", "pipe", "inherit"],
      });
      let output = "";
      child.stdout?.on("data", (chunk) => {
        output += chunk;
      });
      const code = await new Promise((resolve) => child.on("exit", resolve));
      assert.equal(code, 0, output);
      const lines = output.trim().split("\n");
      assert.ok(lines.includes("archived 2 files"), output);
      const result = JSON.parse(lines.at(-1) ?? "");
      assert.deepEqual(result.approved_by, ["alice", "bob"]);
      assert.equal(result.check, true);
      assert.match(result.url, /^s3:\/\/archive\/backup-/);
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("the first workflow resumes after a kill", { timeout: 90_000 }, async () => {
  const dir = tempDir();
  try {
    const first = example(join(dir, "demo"));
    const deadline = performance.now() + 20_000;
    while (!rouletteRecorded(join(dir, "demo"))) {
      assert.ok(performance.now() < deadline, `no sleep reached: ${first.output()}`);
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    first.child.kill("SIGKILL");
    await first.exited;
    assert.match(first.output(), /step roulette/);
    assert.doesNotMatch(first.output(), /multiply/);

    const second = example(join(dir, "demo"));
    assert.equal(await second.exited, 0, second.output());
    assert.doesNotMatch(second.output(), /step roulette/, "the recorded step does not run again");
    assert.match(second.output(), /Final output 100/);
    assert.match(second.output(), /status completed/);

    const third = example(join(dir, "demo"));
    assert.equal(await third.exited, 0, third.output());
    assert.match(third.output(), /Final output 100/, "an ended run sends nothing again");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
