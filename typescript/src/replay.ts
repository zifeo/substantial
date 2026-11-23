// One replay: a run machine driven by the workflow's code (I-23, I-24).
import { setImmediate } from "node:timers";
import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { abandonedReplay, type Clock, Future, Inbox } from "./clock.ts";
import { RunContext } from "./context.ts";
import type { Core } from "./core.ts";
import type { Duration } from "./durations.ts";
import {
  Cancelled,
  ContinuedAsNew,
  endsRun,
  type Failure,
  failureOf,
  InvalidArgument,
  messageOf,
  NEW_VERSION,
  NonDeterminism,
  ProtocolError,
  StepFailed,
  StoreError,
} from "./errors.ts";
import { RunMachine } from "./machines.ts";
import {
  Attempt,
  Blocked,
  type Metering,
  NearLimit,
  type Observed,
  ObserverRaised,
  Replay,
} from "./observe.ts";
import {
  agent,
  type boundary,
  capped,
  decodeValue,
  failureFrom,
  jsonText,
  jsonValue,
  layout,
  run,
} from "./proto.ts";
import { current, makeCall, type PendingStep, Shared, stepFailures } from "./scheduler.ts";
import { attempt } from "./step.ts";
import { perform } from "./stores/open.ts";
import type { Store } from "./stores/store.ts";
import type { AnyWorkflow } from "./workflow.ts";

export type ReplayEvent =
  | { readonly kind: "commit"; readonly chain: string; readonly commit: run.Commit }
  | { readonly kind: "claim"; readonly chain: string }
  | { readonly kind: "ended"; readonly chain: string; readonly end: agent.ReplayEnd }
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
  // § yp7fv5sa7.b@dqzkv
  try {
    host.metering.tell(event);
  } catch (error) {
    emit({ kind: "died", error });
  }
}

/** What the agent loop tells a replay: the answer to its commit or its claim, or to stop. */
export class Channel {
  readonly stopping: Future = new Future();
  readonly #committed = new Inbox<agent.AgentCommand_Committed>();
  readonly claim: Future = new Future();

  get stopped(): boolean {
    return this.stopping.done;
  }

  stop(): void {
    this.stopping.resolve();
  }

  committed(c: agent.AgentCommand_Committed): void {
    this.#committed.push(c);
  }

  claimed(): void {
    this.claim.resolve();
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

/** Runs one replay of `chain` to its end, which it always reports, dropped when it could not go
 * on; `dueAt` is the earliest due entry it serves. Its requests to the agent name the chain, its
 * logs and observer events the run, `data.run`. */
export async function replay(
  core: Core,
  workflow: AnyWorkflow,
  chain: string,
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
    end = await drive(core, workflow, chain, data, clock, emit, channel, host, committed);
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
  emit({ kind: "ended", chain, end });
}

// Every microtask runs before a macrotask: once it fires, the code waits only on the host.
function quiescent(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

type Settled = { ok: true; value: unknown } | { ok: false; error: unknown };

function inputOf(start: layout.Start): unknown {
  // § e1dc4dar4.a@exdgg
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
  // § one g4kv2kj51@9zmz6
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

/** How the code's end ends its run, or the failure that blocks the run instead. */
type Outcome = { readonly end: run.End } | { readonly blocked: Failure };

function ending(end: MessageInitShape<typeof run.EndSchema>["end"]): Outcome {
  return { end: create(run.EndSchema, { end }) };
}

function endOf(workflow: AnyWorkflow, settled: Settled): Outcome {
  // § dxgjekysz@ba8vq
  if (!settled.ok) {
    const e = settled.error;
    if (e instanceof Cancelled) return ending({ case: "cancelled", value: { reason: e.reason } });
    if (e instanceof ContinuedAsNew) {
      const continued = {
        input: jsonValue(e.input),
        ...(e.workflow === undefined ? {} : { workflow: e.workflow }),
        ...(e.version === undefined ? {} : { version: e.version }),
        ...(e.state === undefined ? {} : { state: jsonValue(e.state) }),
        highest: e.highest,
      };
      return ending({ case: "continued", value: continued });
    }
    if (!endsRun(e)) return { blocked: failureOf(e) };
    return ending({ case: "failed", value: { error: endFailure(e) } });
  }
  let text: string;
  try {
    text = jsonText(settled.value);
  } catch (e) {
    // § dxgjekysz.a@6qq3p
    const message = `the value of ${workflow.name}: ${messageOf(e)}`;
    return { blocked: { type: "Json", message, retryable: false } };
  }
  let json: Uint8Array;
  try {
    json = capped(new TextEncoder().encode(text), `the value of ${workflow.name}`);
  } catch (e) {
    // § h8wg39cct.e@96tkv
    return { blocked: { ...failureOf(e), retryable: false } };
  }
  return ending({ case: "completed", value: { value: jsonValue(json) } });
}

/** The failure an error of the code ends its run with. */
function endFailure(e: unknown) {
  // § r3sd261h4.b@zbchy
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

/** Records the end of the code: its value, its failure, its cancellation or its continuation;
 * the failure that blocks the run instead, recording nothing. */
function endRun(shared: Shared, workflow: AnyWorkflow, settled: Settled): Failure | undefined {
  shared.abandoned = true;
  const outcome = endOf(workflow, settled);
  if ("blocked" in outcome) return outcome.blocked;
  shared.call(makeCall({ case: "end", value: outcome.end }));
  return undefined;
}

type Code = { settled?: Settled } | undefined;

/** Whether the run's code may still end it: no core error, no divergence, no end the core made. */
function open(shared: Shared): boolean {
  return shared.fatal === undefined && shared.diverged === undefined && !shared.tooLarge;
}

/** Drops the code on a failure it could not see, a handler's, which blocks the run. */
function failCode(shared: Shared, code: Code): [Code, Failure | undefined] {
  const f = shared.codeFailure;
  shared.codeFailure = undefined;
  if (f === undefined || code === undefined) return [code, undefined];
  shared.abandoned = true;
  return [undefined, open(shared) ? endedWith(f) : undefined];
}

/** Logs a failed or cancelled end the commit recorded, once per run as only one commit wins. */
function warnEnd(runId: string, c: run.Commit): void {
  for (const event of c.segment?.events ?? []) {
    const k = event.kind;
    if (k.case === "failed") {
      const error = k.value.error;
      console.warn(`run ${runId} failed: ${error?.type}: ${error?.message}`);
    } else if (k.case === "cancelled") {
      console.warn(`run ${runId} cancelled: ${k.value.reason}`);
    }
  }
}

/** Hands a commit to the agent; whether the replay goes on. */
async function commit(
  shared: Shared,
  chain: string,
  c: run.Commit,
  emit: Emit,
  channel: Channel,
): Promise<boolean> {
  const clock = shared.clock;
  const n = c.segment?.n;
  emit({ kind: "commit", chain, commit: c });
  for (;;) {
    if (channel.stopped) return false;
    const answer = channel.takeCommitted();
    if (answer !== undefined) {
      if (!answer.won) return false;
      if (n !== undefined && n !== answer.n) {
        throw new ProtocolError(`commit of segment ${n} acknowledged as ${answer.n}`);
      }
      warnEnd(shared.run, c);
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
    // § yp7fv5sa7.a@jnaaz
    const started = clock.elapsed();
    // Unless it reaches its outcome: abandoned by a shutdown, a lost claim or a stop.
    let ended: Attempt["outcome"] = "abandoned";
    try {
      const { name, body, key, heartbeatMs } = step;
      const done = await attempt(clock, name, body, key, timeout, abandoned, heartbeatMs);
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
  chain: string,
  data: run.RunInput,
  clock: Clock,
  emit: Emit,
  channel: Channel,
  host: ReplayHost,
  committed: Committed,
): Promise<agent.ReplayEnd> {
  if (data.start === undefined) throw new ProtocolError(`replay of ${data.run} without start`);
  const machine = new RunMachine(core, data);
  // § one aym52h8ch@ea0k1
  const stepTimeout = workflow.stepTimeout ?? host.stepTimeout;
  const shared = new Shared(
    machine,
    clock,
    data,
    data.start,
    stepTimeout,
    workflow.heartbeatTimeout,
  );
  const attempts = new Set<Future<Ended>>();
  // Aborted as the replay ends: an attempt still in flight is abandoned.
  const abandoned = new AbortController();
  /** Stops the code and its attempts as for a dropped replay, and reports the run blocked. */
  const block = async (raised: Failure): Promise<agent.ReplayEnd> => {
    // § dxgjekysz.e@sdxtb
    const hinted = raised.type === "NonDeterminism" && !raised.message.endsWith(NEW_VERSION);
    const failure = hinted ? { ...raised, message: `${raised.message}; ${NEW_VERSION}` } : raised;
    abandoned.abort(abandonedReplay());
    attempts.clear();
    shared.abandoned = true;
    if (clock.checksOnly()) {
      clock.checked(data.run, `${failure.type}: ${failure.message}`);
      await clock.wait([], undefined);
    }
    // § yp7fv5sa7.c@w5ngw
    console.warn(`run ${data.run} blocked: ${failure.type}: ${failure.message}`);
    tell(host, emit, new Blocked(workflow.name, data.run, failure));
    // § end yp7fv5sa7.c
    const error = create(layout.FailureSchema, endedWith(failure));
    return create(agent.ReplayEndSchema, { end: { case: "blocked", value: error } });
    // § end dxgjekysz.e
  };
  try {
    let code: Code = spawn(shared, workflow);
    for (;;) {
      // § k3zma9hjs@4w2t0
      let blocked: Failure | undefined;
      [code, blocked] = failCode(shared, code);
      if (code !== undefined) {
        await quiescent();
        [code, blocked] = failCode(shared, code);
        if (code?.settled !== undefined) {
          if (open(shared)) blocked = endRun(shared, workflow, code.settled);
          code = undefined;
        }
      }
      if (blocked !== undefined) return await block(blocked);
      // The core blocks the run, which `next` says, or ended it past a limit, which `next`
      // commits: the code is abandoned and its attempts in flight stopped, never reported.
      if (shared.diverged !== undefined || shared.tooLarge) {
        code = undefined;
        // § v4n6ja42e.a@zp1z1
        abandoned.abort(abandonedReplay());
        attempts.clear();
        // § end v4n6ja42e.a
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
      let answer: run.Next | undefined;
      let failed: unknown;
      try {
        answer = shared.next();
      } catch (e) {
        failed = e;
      }
      // § awhhtbr5n.b@5sajg
      if (clock.checksOnly()) {
        const next = answer?.next;
        const diverged =
          shared.diverged ??
          (failed instanceof NonDeterminism ? failed.message : undefined) ??
          (next?.case === "blocked" ? next.value.message : undefined);
        const replaying =
          failed !== undefined ||
          next?.case === "resolve" ||
          next?.case === "deliver" ||
          next?.case === "childEnded";
        if (diverged !== undefined || !replaying) {
          clock.checked(data.run, diverged);
          // Parked for good, so nothing past the history runs or is written: the tester crashes
          // the worker once it read the outcome.
          await clock.wait([], undefined);
        }
      }
      // § end awhhtbr5n.b
      if (answer === undefined) throw failed;
      const next = answer.next;
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
          if (!(await commit(shared, chain, next.value, emit, channel))) return dropped();
          committed.kind = next.value.kind;
          for (const near of next.value.nearLimits) {
            const { limit, used, max } = near;
            console.warn(`run ${data.run} near its ${limit} limit: ${used} of ${max}`);
            // § yp7fv5sa7.c@m8vbz
            tell(
              host,
              emit,
              new NearLimit(workflow.name, data.run, limit, Number(used), Number(max)),
            );
          }
          break;
        case "claim":
          emit({ kind: "claim", chain });
          await clock.wait([channel.claim, channel.stopping], undefined);
          if (channel.stopped) return dropped();
          machine.claimed();
          break;
        case "blocked":
          return await block(failureFrom(next.value));
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
          // § one d9818h4m4@1aq2w
          return create(agent.ReplayEndSchema, { end: { case: "idle", value: next.value } });
        default:
          throw new ProtocolError("empty next");
      }
    }
  } finally {
    // § one v4n6ja42e.a@0pjxz
    abandoned.abort(abandonedReplay());
    shared.closed = true;
    machine.close();
  }
}
