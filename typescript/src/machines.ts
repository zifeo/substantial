// Typed wrappers over the machines of the core: run, agent and plan (I-23).
import {
  create,
  fromBinary,
  type Message,
  type MessageInitShape,
  toBinary,
} from "@bufbuild/protobuf";
import type { GenMessage } from "@bufbuild/protobuf/codegenv2";
import { type Core, Kind } from "./core.ts";
import {
  CoreError,
  LayoutError,
  NonDeterminism,
  ProtocolError,
  Trap,
  ValueTooLarge,
} from "./errors.ts";
import { agent, type boundary, type layout, plan, run } from "./proto.ts";

const ERROR_KINDS = [
  "UNSPECIFIED",
  "NON_DETERMINISM",
  "INVALID",
  "ENDED",
  "CORRUPT",
  "LAYOUT",
  "VALUE_TOO_LARGE",
];

export function coreError(e: boundary.CoreError): Error {
  const kind = ERROR_KINDS[e.kind] ?? String(e.kind);
  if (kind === "NON_DETERMINISM") return new NonDeterminism(e.message);
  if (kind === "LAYOUT") return new LayoutError(e.message);
  if (kind === "VALUE_TOO_LARGE") return new ValueTooLarge(e.message);
  return new CoreError(kind, e.message);
}

type Oneof = { case: string; value: unknown } | { case: undefined; value?: undefined };
type Response = Message & { response: Oneof };
type Answer<R extends Response, K extends string> = Extract<R["response"], { case: K }>["value"];

/** A machine of the core, dropped with `sub_drop` by `close`. */
class Machine<Req extends Message, Res extends Response> {
  readonly #core: Core;
  readonly #request: GenMessage<Req>;
  readonly #response: GenMessage<Res>;
  #id: number | undefined;

  constructor(
    core: Core,
    kind: Kind,
    input: Uint8Array,
    request: GenMessage<Req>,
    response: GenMessage<Res>,
  ) {
    this.#core = core;
    this.#request = request;
    this.#response = response;
    this.#id = core.newMachine(kind, input);
  }

  close(): void {
    if (this.#id !== undefined) {
      const id = this.#id;
      this.#id = undefined;
      this.#core.dropMachine(id);
    }
  }

  protected expect<K extends Exclude<Res["response"]["case"], "error" | undefined>>(
    request: Req,
    expected: K,
  ): Answer<Res, K> {
    if (this.#id === undefined) {
      throw new ProtocolError("a call on a dropped machine");
    }
    let out: Res;
    try {
      out = fromBinary(this.#response, this.#core.call(this.#id, toBinary(this.#request, request)));
    } catch (e) {
      if (e instanceof Trap) throw e;
      throw new ProtocolError(`undecodable response: ${e instanceof Error ? e.message : e}`);
    }
    const answer = out.response;
    if (answer.case === "error") {
      throw coreError(answer.value as boundary.CoreError);
    }
    if (answer.case !== expected) {
      throw new ProtocolError(`expected ${expected}, got ${answer.case}`);
    }
    return answer.value as Answer<Res, K>;
  }
}

/** The run machine of one replay. */
export class RunMachine extends Machine<run.RunRequest, run.RunResponse> {
  constructor(core: Core, input: run.RunInput) {
    const data = toBinary(run.RunInputSchema, input);
    super(core, Kind.RUN, data, run.RunRequestSchema, run.RunResponseSchema);
  }

  call(call: run.Call, now: layout.Time): run.Called {
    const value = create(run.RunRequest_MakeCallSchema, { call, now });
    return this.expect(
      create(run.RunRequestSchema, { request: { case: "call", value } }),
      "called",
    );
  }

  stepEnded(call: bigint, attempt: number, outcome: run.Outcome, now: layout.Time): void {
    const value = create(run.RunRequest_StepEndedSchema, { call, attempt, outcome, now });
    this.expect(create(run.RunRequestSchema, { request: { case: "stepEnded", value } }), "done");
  }

  satisfied(call: bigint): void {
    this.expect(
      create(run.RunRequestSchema, { request: { case: "satisfied", value: call } }),
      "done",
    );
  }

  arrived(messages: run.InboxMessage[]): void {
    const value = create(run.RunRequest_ArrivedSchema, { messages });
    this.expect(create(run.RunRequestSchema, { request: { case: "arrived", value } }), "done");
  }

  next(now: layout.Time): run.Next {
    return this.expect(
      create(run.RunRequestSchema, { request: { case: "next", value: now } }),
      "next",
    );
  }

  /** The answer to `Next.store`, never a failed one. */
  stored(result: boundary.OpResult): void {
    this.expect(
      create(run.RunRequestSchema, { request: { case: "stored", value: result } }),
      "done",
    );
  }
}

/** The agent machine of one worker. */
export class AgentMachine extends Machine<agent.AgentRequest, agent.AgentResponse> {
  constructor(core: Core, config: agent.AgentConfig) {
    const data = toBinary(agent.AgentConfigSchema, config);
    super(core, Kind.AGENT, data, agent.AgentRequestSchema, agent.AgentResponseSchema);
  }

  #done(request: MessageInitShape<typeof agent.AgentRequestSchema>["request"]): void {
    this.expect(create(agent.AgentRequestSchema, { request }), "done");
  }

  next(now: layout.Time): agent.AgentCommand {
    const request = create(agent.AgentRequestSchema, { request: { case: "next", value: now } });
    return this.expect(request, "command");
  }

  feed(id: bigint, result: boundary.OpResult): void {
    const value = create(agent.AgentRequest_FeedSchema, { id, result });
    this.#done({ case: "feed", value });
  }

  commit(runId: string, commit: run.Commit): void {
    const value = create(agent.AgentRequest_RunCommitSchema, { run: runId, commit });
    this.#done({ case: "commit", value });
  }

  replayEnded(runId: string, end: agent.ReplayEnd): void {
    const value = create(agent.AgentRequest_ReplayEndedSchema, { run: runId, end });
    this.#done({ case: "replayEnded", value });
  }

  wake(): void {
    this.#done({ case: "wake", value: {} });
  }

  shutdown(): void {
    this.#done({ case: "shutdown", value: {} });
  }
}

/** The plan machine of one client action. */
export class PlanMachine extends Machine<plan.PlanRequest, plan.PlanResponse> {
  constructor(core: Core, input: plan.PlanInput) {
    const data = toBinary(plan.PlanInputSchema, input);
    super(core, Kind.PLAN, data, plan.PlanRequestSchema, plan.PlanResponseSchema);
  }

  next(): plan.PlanStep {
    return this.expect(
      create(plan.PlanRequestSchema, { request: { case: "next", value: {} } }),
      "step",
    );
  }

  feed(result: boundary.OpResult): void {
    const request = create(plan.PlanRequestSchema, { request: { case: "feed", value: result } });
    this.expect(request, "done");
  }
}
