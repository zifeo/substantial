/**
 * Brokerless durable execution: the TypeScript host of the Substantial core.
 *
 * The protocol runs in a core compiled to WebAssembly, loaded once per process; each worker and
 * client instantiates it. Workflow code runs on the caller's event loop, its calls answered by
 * the host one at a time, and the host waits for the microtask queue to drain between two.
 *
 * @module
 */

export {
  type ChainResultOptions,
  Child,
  type ChildOptions,
  type MessageOptions,
} from "./child.ts";
export {
  type OpenOptions,
  type RunInfo,
  type ScheduleOptions,
  type StartOptions,
  Substantial,
} from "./client.ts";
export type {
  Context,
  ContinueOptions,
  PollOptions,
  ReceiveOptions,
  WaitOptions,
} from "./context.ts";
export {
  Amount,
  type Budget,
  Cost,
  Meters,
  type PriceFields,
  Prices,
} from "./cost.ts";
export { Duration, days, hours, milliseconds, minutes, seconds } from "./durations.ts";
export {
  BudgetExceeded,
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
export { Attempt, type Observed, Replay, Scan, StoreOp } from "./observe.ts";
export {
  type HistoryEvent,
  type ResultOptions,
  type RetryOptions,
  Run,
  type RunStatus,
  type SendOptions,
} from "./run.ts";
export { Cron } from "./schedule.ts";
export type { Preset, WorkerOptions } from "./settings.ts";
export {
  charge,
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
export { type DrainOptions, Worker } from "./worker.ts";
export {
  Permanent,
  type Retry,
  Workflow,
  type WorkflowFn,
  type WorkflowOptions,
  workflow,
} from "./workflow.ts";
