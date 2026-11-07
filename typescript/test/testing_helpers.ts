// What the tests of the test environment share.
import * as s from "../src/mod.ts";
import { type Env, worldOf } from "../src/testing/env.ts";

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

/** The id of the current life of `worker`. */
export function idOf(env: Env, worker: s.Worker): string | undefined {
  return worldOf(env).pods.find((p) => p.worker === worker)?.clock?.id;
}
