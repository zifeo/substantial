// One replay: a run machine driven by the workflow's code (I-23, I-24).
import { setImmediate } from "node:timers";
import { create } from "@bufbuild/protobuf";
import { abandonedReplay, type Clock, Future, Inbox } from "./clock.ts";
import { RunContext } from "./context.ts";
import type { Core } from "./core.ts";
import type { Duration } from "./durations.ts";
import {
  Cancelled,
  ContinuedAsNew,
  type Failure,
  failureOf,
  InvalidArgument,
  messageOf,
  ProtocolError,
  StepFailed,
  StoreError,
} from "./errors.ts";
import { RunMachine } from "./machines.ts";
import { Attempt, type Metering, type Observed, ObserverRaised, Replay } from "./observe.ts";
import {
  agent,
  type boundary,
  capped,
  decodeValue,
  jsonText,
  jsonValue,
  type layout,
  run,
} from "./proto.ts";
import { current, makeCall, type PendingStep, Shared, stepFailures } from "./scheduler.ts";
import { attempt } from "./step.ts";
import { perform } from "./stores/open.ts";
import type { Store } from "./stores/store.ts";
import type { AnyWorkflow } from "./workflow.ts";

export type ReplayEvent =
  | { readonly kind: "commit"; readonly run: string; readonly commit: run.Commit }
  | { readonly kind: "ended"; readonly run: string; readonly end: agent.ReplayEnd }
  /** The observer raised: the worker stops with its error. */
  | { readonly kind: "died"; readonly error: unknown };

export type Emit = (event: ReplayEvent) => void;

/** What a replay takes of its worker: the store a workflow's send and result read a chain on, the
 * timeout of a step that neither it nor its workflow gives, and the prices and observer of its
 * `Substantial`. */
export interface ReplayHost {
  readonly store: Store;
  readonly stepTimeout: Duration;
  readonly metering: Metering;
}

/** Tells the observer; one that raises stops the worker with its error. */
function tell(host: ReplayHost, emit: Emit, event: Observed): void {
  try {
    host.metering.tell(event);
  } catch (error) {
    emit({ kind: "died", error });
  }
}

/** What the agent loop tells a replay: the answer to its commit, or to stop. */
export class Channel {
  readonly stopping: Future = new Future();
  readonly #committed = new Inbox<agent.AgentCommand_Committed>();

  get stopped(): boolean {
    return this.stopping.done;
  }

  stop(): void {
    this.stopping.resolve();
  }

  committed(c: agent.AgentCommand_Committed): void {
    this.#committed.push(c);
  }

  hasCommitted(): boolean {
    return !this.#committed.empty;
  }

  takeCommitted(): agent.AgentCommand_Committed | undefined {
    return this.#committed.take();
  }

  /** Done once a `committed` arrives. */
  arrival(): Future {
    return this.#committed.signal();
  }
}

export function dropped(): agent.ReplayEnd {
  return create(agent.ReplayEndSchema, { end: { case: "dropped", value: {} } });
}

/** The kind of the last commit of a replay that won. */
type Committed = { kind?: run.CommitKind };

/** Runs one replay to its end, which it always reports, dropped when it could not go on;
 * `dueAt` is the earliest due entry it serves. */
export async function replay(
  core: Core,
  workflow: AnyWorkflow,
  data: run.RunInput,
  dueAt: layout.Time | undefined,
  clock: Clock,
  emit: Emit,
  channel: Channel,
  host: ReplayHost,
): Promise<void> {
  const [started, now] = [clock.elapsed(), clock.now()];
  const committed: Committed = {};
  let end: agent.ReplayEnd;
  try {
    end = await drive(core, workflow, data, clock, emit, channel, host, committed);
  } catch (e) {
    console.error(`substantial: replay of ${data.run} dropped:`, e);
    end = dropped();
  }
  let ended: Replay["end"] = "idle";
  if (end.end.case === "dropped") ended = "dropped";
  else if (committed.kind === run.CommitKind.SUSPEND) ended = "suspended";
  else if (committed.kind === run.CommitKind.END) ended = "ended";
  const delay = dueAt === undefined ? undefined : (now - Number(dueAt.ms)) / 1000;
  tell(host, emit, new Replay(workflow.name, delay, (clock.elapsed() - started) / 1000, ended));
  emit({ kind: "ended", run: data.run, end });
}

// Every microtask runs before a macrotask: once it fires, the code waits only on the host.
function quiescent(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

type Settled = { ok: true; value: unknown } | { ok: false; error: unknown };

function inputOf(start: layout.Start): unknown {
  const input = start.input?.kind.case === undefined ? {} : decodeValue(start.input);
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    throw new InvalidArgument(`an input is a JSON object, not ${jsonText(input)}`);
  }
  return input;
}

/** Starts the code in the replay's context; `settled` is set once its promise settles. */
function spawn(shared: Shared, workflow: AnyWorkflow): { settled?: Settled } {
  const code: { settled?: Settled } = {};
  const context = new RunContext(shared);
  const promise = current.run(shared, async () => workflow.fn(context, inputOf(shared.start)));
  promise.then(
    (value) => {
      code.settled = { ok: true, value };
    },
    (error) => {
      code.settled = { ok: false, error };
    },
  );
  return code;
}

function endOf(workflow: AnyWorkflow, settled: Settled): run.End {
  if (!settled.ok) {
    const e = settled.error;
    if (e instanceof Cancelled) {
      return create(run.EndSchema, { end: { case: "cancelled", value: { reason: e.reason } } });
    }
    if (e instanceof ContinuedAsNew) {
      const continued = {
        input: jsonValue(e.input),
        ...(e.workflow === undefined ? {} : { workflow: e.workflow }),
        ...(e.version === undefined ? {} : { version: e.version }),
        ...(e.state === undefined ? {} : { state: jsonValue(e.state) }),
      };
      return create(run.EndSchema, { end: { case: "continued", value: continued } });
    }
    return create(run.EndSchema, {
      end: { case: "failed", value: { error: endFailure(settled.error) } },
    });
  }
  let text: string;
  try {
    text = jsonText(settled.value);
  } catch (e) {
    const error = { type: "Json", message: `the value of ${workflow.name}: ${messageOf(e)}` };
    return create(run.EndSchema, { end: { case: "failed", value: { error } } });
  }
  let json: Uint8Array;
  try {
    json = capped(new TextEncoder().encode(text), `the value of ${workflow.name}`);
  } catch (e) {
    return create(run.EndSchema, { end: { case: "failed", value: { error: failureOf(e) } } });
  }
  return create(run.EndSchema, { end: { case: "completed", value: { value: jsonValue(json) } } });
}

/** The failure an error of the code ends its run with. */
function endFailure(e: unknown) {
  const failure = endedWith(failureOf(e));
  if (!(e instanceof StepFailed)) return failure;
  const core = stepFailures.get(e);
  if (core !== undefined) return { ...failure, details: core };
  // A client's RunFailed reads the step's cause from it, as a StepFailed's from the core.
  const { type, message, retryable } = e.cause;
  const details = JSON.stringify({ error: { type, message, retryable } });
  return { ...failure, details: jsonValue(new TextEncoder().encode(details)) };
}

/** A failure the code ends its run with, as the core takes it: a run ends without the details of
 * the failure it rethrows, but a StepFailed's. */
function endedWith({ type, message, retryable }: Failure) {
  return { type, message, retryable };
}

/** Records the end of the code: its value, its failure, its cancellation or its
 * continuation. */
function endRun(shared: Shared, workflow: AnyWorkflow, settled: Settled): void {
  shared.call(makeCall({ case: "end", value: endOf(workflow, settled) }));
  shared.abandoned = true;
}

type Code = { settled?: Settled } | undefined;

/** Ends the run with a failure the code could not see, a handler's, and drops the code. */
function failCode(shared: Shared, code: Code): Code {
  const f = shared.codeFailure;
  shared.codeFailure = undefined;
  if (f === undefined || code === undefined) return code;
  if (shared.fatal === undefined && shared.diverged === undefined) {
    const end = create(run.EndSchema, { end: { case: "failed", value: { error: endedWith(f) } } });
    shared.call(makeCall({ case: "end", value: end }));
  }
  shared.abandoned = true;
  return undefined;
}

/** Logs a failed or cancelled end the commit recorded, once per run as only one commit wins. */
function warnEnd(chain: string, c: run.Commit): void {
  for (const event of c.segment?.events ?? []) {
    const k = event.kind;
    if (k.case === "failed") {
      const error = k.value.error;
      console.warn(`substantial: run ${chain} failed: ${error?.type}: ${error?.message}`);
    } else if (k.case === "cancelled") {
      console.warn(`substantial: run ${chain} cancelled: ${k.value.reason}`);
    }
  }
}

/** Hands a commit to the agent; whether the replay goes on. */
async function commit(
  shared: Shared,
  runId: string,
  c: run.Commit,
  emit: Emit,
  channel: Channel,
): Promise<boolean> {
  const clock = shared.clock;
  const n = c.segment?.n;
  emit({ kind: "commit", run: runId, commit: c });
  for (;;) {
    if (channel.stopped) return false;
    const answer = channel.takeCommitted();
    if (answer !== undefined) {
      if (!answer.won) return false;
      if (n !== undefined && n !== answer.n) {
        throw new ProtocolError(`commit of segment ${n} acknowledged as ${answer.n}`);
      }
      warnEnd(shared.start.chain, c);
      if (answer.cost !== undefined) shared.arrived(answer.inbox, answer.cost);
      else if (answer.inbox.length > 0) {
        throw new ProtocolError("messages arrived without the chain's cost");
      }
      return true;
    }
    await clock.wait([channel.arrival(), channel.stopping], undefined);
  }
}

/** Performs the operation `Next.store` asks for, a read or a create; `undefined` when the replay is stopped meanwhile, or
 * when the observer raised, which stops the worker. */
async function read(
  clock: Clock,
  store: Store,
  op: boundary.StoreOp,
  channel: Channel,
  runId: string,
  emit: Emit,
): Promise<boundary.OpResult | undefined> {
  const reading = clock.spawn(async () => {
    try {
      return await perform(store, op);
    } catch (e) {
      if (!(e instanceof ObserverRaised)) throw e;
      emit({ kind: "died", error: e.error });
      return undefined;
    }
  }, `read of ${runId}`);
  await clock.wait([reading, channel.stopping], undefined);
  return channel.stopped ? undefined : reading.result();
}

type Ended = [bigint, number, run.Outcome, layout.Amount[]];

/** How an attempt ended, as its observer is told. */
function attemptOutcome(outcome: run.Outcome, attempt: number, max: number): Attempt["outcome"] {
  const o = outcome.outcome;
  if (o.case === "value") return "ok";
  if (o.value?.type === "StepTimeout") return "timeout";
  if (o.value?.retryable && attempt < max) return "retry";
  return "failed";
}

/** Runs an attempt of `step` as a task of the replay, its observer told once it ended. */
function spawnAttempt(
  clock: Clock,
  host: ReplayHost,
  emit: Emit,
  workflow: AnyWorkflow,
  step: PendingStep,
  e: run.Next_Execute,
  abandoned: AbortSignal,
): Future<Ended> {
  const timeout = e.timeoutMs === undefined ? undefined : Number(e.timeoutMs);
  return clock.spawn(async (): Promise<Ended> => {
    const started = clock.elapsed();
    // Unless it reaches its outcome: abandoned by a shutdown, a lost claim or a stop.
    let ended: Attempt["outcome"] = "abandoned";
    try {
      const done = await attempt(clock, step.name, step.body, step.key, timeout, abandoned);
      if (!done.abandoned) ended = attemptOutcome(done.outcome, e.attempt, step.maxAttempts);
      return [e.call, e.attempt, done.outcome, done.charges];
    } finally {
      const seconds = (clock.elapsed() - started) / 1000;
      tell(host, emit, new Attempt(workflow.name, step.name, e.attempt, seconds, ended));
    }
  }, `attempt of ${step.key}`);
}

async function drive(
  core: Core,
  workflow: AnyWorkflow,
  data: run.RunInput,
  clock: Clock,
  emit: Emit,
  channel: Channel,
  host: ReplayHost,
  committed: Committed,
): Promise<agent.ReplayEnd> {
  if (data.start === undefined) throw new ProtocolError(`replay of ${data.run} without start`);
  const machine = new RunMachine(core, data);
  const stepTimeout = workflow.stepTimeout ?? host.stepTimeout;
  const shared = new Shared(machine, clock, data, data.start, stepTimeout);
  const attempts = new Set<Future<Ended>>();
  // Aborted as the replay ends: an attempt still in flight is abandoned.
  const abandoned = new AbortController();
  try {
    let code: Code = spawn(shared, workflow);
    for (;;) {
      code = failCode(shared, code);
      if (code !== undefined) {
        await quiescent();
        code = failCode(shared, code);
        if (code?.settled !== undefined) {
          if (shared.fatal === undefined && shared.diverged === undefined) {
            endRun(shared, workflow, code.settled);
          }
          code = undefined;
        }
      }
      // The core recorded the failure, which `next` commits: the code is abandoned, its steps in
      // flight never reported.
      if (shared.diverged !== undefined) {
        code = undefined;
        for (const a of attempts) if (a.done) attempts.delete(a);
      }
      if (shared.fatal !== undefined) {
        throw new ProtocolError(`a call of the code failed: ${shared.fatal}`);
      }
      if (channel.stopped) return dropped();
      if (channel.hasCommitted()) throw new ProtocolError("committed without a commit");
      shared.evaluateWaits();
      if (shared.codeFailure !== undefined) continue;
      for (const a of attempts) {
        if (!a.done) continue;
        attempts.delete(a);
        shared.stepEnded(...a.result());
      }
      const next = shared.next().next;
      switch (next.case) {
        case "resolve":
          shared.resolve(next.value);
          break;
        case "deliver":
          shared.deliver(next.value);
          break;
        case "childEnded":
          shared.ended.add(next.value);
          break;
        case "execute": {
          const e = next.value;
          const step = shared.steps.get(e.call);
          if (step === undefined)
            throw new ProtocolError(`execute of call ${e.call} without a step`);
          attempts.add(spawnAttempt(clock, host, emit, workflow, step, e, abandoned.signal));
          break;
        }
        case "commit":
          if (!(await commit(shared, data.run, next.value, emit, channel))) return dropped();
          committed.kind = next.value.kind;
          break;
        case "waitSteps": {
          if (attempts.size === 0) throw new ProtocolError("wait_steps with no step in flight");
          const until = next.value.until;
          await clock.wait([...attempts, channel.stopping], until && Number(until.ms));
          break;
        }
        case "store": {
          const result = await read(clock, host.store, next.value, channel, data.run, emit);
          if (result === undefined) return dropped();
          if (result.result.case === "failed") {
            throw new StoreError(
              `a store operation of a workflow's send or result: ${result.result.value.message}`,
            );
          }
          machine.stored(result);
          break;
        }
        case "idle":
          return create(agent.ReplayEndSchema, { end: { case: "idle", value: next.value } });
        default:
          throw new ProtocolError("empty next");
      }
    }
  } finally {
    abandoned.abort(abandonedReplay());
    shared.closed = true;
    machine.close();
  }
}
