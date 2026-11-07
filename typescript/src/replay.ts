// One replay: a run machine driven by the workflow's code (I-23, I-24).
import { setImmediate } from "node:timers";
import { create } from "@bufbuild/protobuf";
import { RunContext } from "./context.ts";
import type { Core } from "./core.ts";
import { type Duration, delay } from "./durations.ts";
import {
  Cancelled,
  ContinuedAsNew,
  failureOf,
  InvalidArgument,
  messageOf,
  ProtocolError,
  StoreError,
} from "./errors.ts";
import { RunMachine } from "./machines.ts";
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
import { type Clock, current, makeCall, Shared } from "./scheduler.ts";
import { attempt } from "./step.ts";
import { perform } from "./stores/open.ts";
import type { Store } from "./stores/store.ts";
import type { AnyWorkflow } from "./workflow.ts";

export type ReplayEvent =
  | { readonly kind: "commit"; readonly run: string; readonly commit: run.Commit }
  | { readonly kind: "ended"; readonly run: string; readonly end: agent.ReplayEnd };

export type Emit = (event: ReplayEvent) => void;

/** What a replay takes of its worker: the store a workflow's send reads its chain on, and the
 * timeout of a step that neither it nor its workflow gives. */
export interface ReplayHost {
  readonly store: Store;
  readonly stepTimeout: Duration;
}

/** What the agent loop tells a replay: the answer to its commit, or to stop. */
export class Channel {
  #stopped = false;
  #committed: agent.AgentCommand_Committed[] = [];
  #signal = Promise.withResolvers<void>();

  get stopped(): boolean {
    return this.#stopped;
  }

  #notify(): void {
    this.#signal.resolve();
    this.#signal = Promise.withResolvers<void>();
  }

  stop(): void {
    this.#stopped = true;
    this.#notify();
  }

  committed(c: agent.AgentCommand_Committed): void {
    this.#committed.push(c);
    this.#notify();
  }

  hasCommitted(): boolean {
    return this.#committed.length > 0;
  }

  takeCommitted(): agent.AgentCommand_Committed | undefined {
    return this.#committed.shift();
  }

  /** Resolves at the next `stop` or `committed`. */
  changed(): Promise<void> {
    return this.#signal.promise;
  }
}

export function dropped(): agent.ReplayEnd {
  return create(agent.ReplayEndSchema, { end: { case: "dropped", value: {} } });
}

/** Runs one replay to its end, which it always reports, dropped when it could not go on. */
export async function replay(
  core: Core,
  workflow: AnyWorkflow,
  data: run.RunInput,
  clock: Clock,
  emit: Emit,
  channel: Channel,
  host: ReplayHost,
): Promise<void> {
  let end: agent.ReplayEnd;
  try {
    end = await drive(core, workflow, data, clock, emit, channel, host);
  } catch (e) {
    console.error(`substantial: replay of ${data.run} dropped:`, e);
    end = dropped();
  }
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
      end: { case: "failed", value: { error: failureOf(settled.error) } },
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
    const end = create(run.EndSchema, { end: { case: "failed", value: { error: f } } });
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
      if (answer.inbox.length > 0) shared.arrived(answer.inbox);
      return true;
    }
    await channel.changed();
  }
}

/** Performs the read `Next.store` asks for; `undefined` when the replay is stopped meanwhile. */
async function read(
  store: Store,
  op: boundary.StoreOp,
  channel: Channel,
): Promise<boundary.OpResult | undefined> {
  const reading = perform(store, op);
  for (;;) {
    if (channel.stopped) return undefined;
    const result = await Promise.race([reading, channel.changed().then(() => undefined)]);
    if (result !== undefined) return result;
  }
}

async function drive(
  core: Core,
  workflow: AnyWorkflow,
  data: run.RunInput,
  clock: Clock,
  emit: Emit,
  channel: Channel,
  host: ReplayHost,
): Promise<agent.ReplayEnd> {
  if (data.start === undefined) throw new ProtocolError(`replay of ${data.run} without start`);
  const machine = new RunMachine(core, data);
  const stepTimeout = workflow.stepTimeout ?? host.stepTimeout;
  const shared = new Shared(machine, clock, data, data.start, stepTimeout);
  const attempts = new Set<Promise<void>>();
  const finished: [bigint, number, run.Outcome][] = [];
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
        finished.length = 0;
      }
      if (shared.fatal !== undefined) {
        throw new ProtocolError(`a call of the code failed: ${shared.fatal}`);
      }
      if (channel.stopped) return dropped();
      if (channel.hasCommitted()) throw new ProtocolError("committed without a commit");
      shared.evaluateWaits();
      if (shared.codeFailure !== undefined) continue;
      for (const [call, n, outcome] of finished.splice(0)) shared.stepEnded(call, n, outcome);
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
          const timeout = e.timeoutMs === undefined ? undefined : Number(e.timeoutMs);
          const running: Promise<void> = attempt(step.body, step.key, timeout).then((outcome) => {
            finished.push([e.call, e.attempt, outcome]);
            attempts.delete(running);
          });
          attempts.add(running);
          break;
        }
        case "commit":
          if (!(await commit(shared, data.run, next.value, emit, channel))) return dropped();
          break;
        case "waitSteps": {
          if (attempts.size === 0) throw new ProtocolError("wait_steps with no step in flight");
          const until = next.value.until;
          const timer = until === undefined ? undefined : delay(Number(until.ms - clock().ms));
          const waits: Promise<void>[] = [...attempts, channel.changed()];
          if (timer !== undefined) waits.push(timer.promise);
          await Promise.race(waits);
          timer?.cancel();
          break;
        }
        case "store": {
          const result = await read(host.store, next.value, channel);
          if (result === undefined) return dropped();
          if (result.result.case === "failed") {
            throw new StoreError(
              `a workflow's send reading its chain: ${result.result.value.message}`,
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
    shared.closed = true;
    machine.close();
  }
}
