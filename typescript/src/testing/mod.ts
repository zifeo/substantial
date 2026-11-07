/**
 * The test environment of docs/testing.md: a namespace in memory on a virtual clock, whose
 * workers run in the test's process and move only when the test says so.
 *
 * @module
 */

export { Env, type EnvOptions, type HoldOptions, Unchecked } from "./env.ts";
export { Chaos, type Faults } from "./store.ts";
export { Stuck } from "./time.ts";
export type { EnvWorker, Hold } from "./workers.ts";
