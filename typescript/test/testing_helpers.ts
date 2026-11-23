// What the tests of the test environment share.
import * as s from "../src/mod.ts";
import { type Env, worldOf } from "../src/testing/env.ts";
import type { AnyWorkflow } from "../src/workflow.ts";

export const T0 = new Date("2030-01-07T09:00:00Z");

/** The ISO time `ms` after `T0`, as `c.now()` gives it. */
export function iso(ms: number): string {
  return new Date(T0.getTime() + ms).toISOString();
}

/** A test in a fresh environment at `T0`, which ends with `check`. */
export function withEnv(fn: (env: Env) => Promise<void>): () => Promise<void> {
  return async () => {
    await using env = await s.testing.Env.open({ now: T0 });
    await fn(env);
    await env.check();
  };
}

/** The error blocking `run` of `workflow`: it settles first, and throws if the run is not
 * blocked. */
export async function blockedIn(
  env: Env,
  workflow: AnyWorkflow,
  run: s.Run<unknown>,
): Promise<s.Failure> {
  await env.settle();
  for await (const info of env.runs(workflow)) {
    if (info.chain === run.chain && info.waitingOn === "blocked" && info.error !== undefined) {
      return info.error;
    }
  }
  throw new Error(`run ${run.chain} is not blocked: it is ${await run.status()}`);
}

/** The id of the current life of `worker`. */
export function idOf(env: Env, worker: s.Worker): string | undefined {
  return worldOf(env).pods.find((p) => p.worker === worker)?.clock?.id;
}
