/**
 * Brokerless durable execution: the TypeScript host of the Substantial core.
 *
 * The protocol runs in a core compiled to WebAssembly, loaded once per process; each worker and
 * client instantiates it. Workflow code runs on the caller's event loop, its calls answered by
 * the host one at a time, and the host waits for the microtask queue to drain between two.
 *
 * @module
 */

export { Child, type ChildOptions, type MessageOptions } from "./child.ts";
export { type CreateOptions, type OpenOptions, type StartOptions, Substantial } from "./client.ts";
export type { Context, ContinueOptions, ReceiveOptions, WaitOptions } from "./context.ts";
export { Duration, days, hours, milliseconds, minutes, seconds } from "./durations.ts";
export {
  Cancelled,
  ConfigError,
  ContinuedAsNew,
  Control,
  type EndedStatus,
  Failed,
  type Failure,
  InvalidArgument,
  isControl,
  LayoutError,
  NonDeterminism,
  NotCompressible,
  NotInWorkflow,
  RolledBack,
  RunEnded,
  RunExists,
  RunFailed,
  RunNotFound,
  StepFailed,
  StoreError,
  SubstantialError,
  UndoFailed,
  ValueTooLarge,
  WaitTimeout,
} from "./errors.ts";
export type { Log } from "./log.ts";
export type { HandleOptions } from "./messages.ts";
export { type ResultOptions, Run, type RunStatus, type SendOptions } from "./run.ts";
export { FOREVER, type Preset, type Retention, type WorkerOptions } from "./settings.ts";
export {
  type DeclaredStepOptions,
  type StepOptions,
  step,
  stepKey,
  stepSignal,
} from "./step.ts";
export {
  Credentials,
  type CredentialsInit,
  type CredentialsProvider,
  Password,
  type PasswordInit,
  type PasswordProvider,
  type ProvidedCredentials,
  type ProvidedPassword,
} from "./stores/credentials.ts";
export * as testing from "./testing/mod.ts";
export { Worker } from "./worker.ts";
export {
  Permanent,
  type Retry,
  Workflow,
  type WorkflowFn,
  type WorkflowOptions,
  workflow,
} from "./workflow.ts";
