// Children (I-11) and messages sent from a workflow (I-9).
import { InvalidArgument, onlyOptions } from "./errors.ts";
import { checkKey, checkName } from "./names.ts";
import { decodeValue, encodeJson, jsonValue } from "./proto.ts";
import { makeCall, type Shared } from "./scheduler.ts";
import { type AnyWorkflow, Workflow } from "./workflow.ts";

export interface MessageOptions {
  /** Delivered once per id; the core derives one from this call when none is given. */
  readonly id?: string;
}

export interface ChildOptions {
  /** The child's id; the core derives one from this run's when none is given. */
  readonly id?: string;
}

/** The JSON bytes of an input: an object, `{}` when none is given. */
export function inputObject(input: unknown): Uint8Array {
  const object = input === undefined ? {} : input;
  if (typeof object !== "object" || object === null || Array.isArray(object)) {
    throw new InvalidArgument(`an input is a JSON object, not ${JSON.stringify(object)}`);
  }
  return encodeJson(object, "the input");
}

export function send(
  shared: Shared,
  runId: string,
  name: string,
  value: unknown,
  options: MessageOptions,
): Promise<void> {
  onlyOptions(options, ["id"], "send");
  const call = makeCall({
    case: "send",
    value: {
      run: checkKey("run id", runId),
      name: checkName("message name", name),
      value: jsonValue(encodeJson(value, "the message")),
      ...(options.id === undefined ? {} : { id: checkKey("message id", options.id) }),
    },
  });
  return shared.future(call, () => undefined);
}

/** A run started by this one: its `result`, whether it is `done`, and messages to it. */
export class Child<O = unknown> {
  /** The child's chain, the id a client gives `sub.run`. */
  readonly chain: string;
  readonly #shared: Shared;

  constructor(shared: Shared, chain: string) {
    this.#shared = shared;
    this.chain = chain;
  }

  toString(): string {
    return `child ${this.chain}`;
  }

  /** Whether the child's chain ended, for a `waitUntil` over several things. */
  get done(): boolean {
    return this.#shared.ended.has(this.chain);
  }

  /** The value the child's chain ended with; `RunFailed` if it failed or was cancelled. */
  result(): Promise<O> {
    return this.#shared.future(makeCall({ case: "childResult", value: this.chain }), decodeValue);
  }

  /** Sends the child a message, delivered once per `id`. */
  send(name: string, value?: unknown, options: MessageOptions = {}): Promise<void> {
    return send(this.#shared, this.chain, name, value, options);
  }
}

export function start<I, O>(
  shared: Shared,
  workflow: Workflow<I, O>,
  input: I | undefined,
  options: ChildOptions,
): Promise<Child<O>> {
  if (!(workflow instanceof Workflow)) {
    throw new InvalidArgument(`start takes a workflow, not ${workflow}`);
  }
  onlyOptions(options, ["id"], "start");
  const w = workflow as AnyWorkflow;
  const call = makeCall({
    case: "start",
    value: {
      workflow: w.name,
      ...(w.version === undefined ? {} : { version: w.version }),
      input: jsonValue(inputObject(input)),
      ...(options.id === undefined ? {} : { id: checkKey("run id", options.id) }),
    },
  });
  return Promise.resolve(new Child<O>(shared, shared.expect(call, "run")));
}
