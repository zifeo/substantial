// Handlers bound to a run's messages (I-9).
import { Failed, InvalidArgument, messageOf, typeName } from "./errors.ts";
import { decodeValue, type run } from "./proto.ts";
import type { Handler, Shared } from "./scheduler.ts";

function decodeFailure(m: run.Message, why: string): Failed {
  return new Failed({
    type: "MessageDecode",
    message: `message ${JSON.stringify(m.name)} ${m.id}: ${why}`,
    retryable: false,
  });
}

function isThenable(value: unknown): boolean {
  return typeof (value as { then?: unknown } | null)?.then === "function";
}

function checkPlain(fn: unknown): void {
  if (typeof fn !== "function") throw new InvalidArgument(`a handler is a function, not ${fn}`);
  if (fn.constructor.name === "AsyncFunction") {
    throw new InvalidArgument("a handler is a plain function, not async");
  }
}

/** A handler calling `fn` with each value; a failure of either fails the run. */
function handler<T>(fn: (value: T) => unknown, decode?: (value: unknown) => T): Handler {
  checkPlain(fn);
  if (decode !== undefined && typeof decode !== "function") {
    throw new InvalidArgument("decode is a function");
  }
  return (m) => {
    let returned: unknown;
    try {
      const json = decodeValue(m.value);
      returned = fn(decode === undefined ? (json as T) : decode(json));
    } catch (e) {
      throw decodeFailure(m, `${typeName(e)}: ${messageOf(e)}`);
    }
    if (isThenable(returned)) throw decodeFailure(m, "a handler returned a promise");
  };
}

export interface HandleOptions<T> {
  /** Turns the message's JSON into the value the handler sees. */
  readonly decode?: (value: unknown) => T;
}

export function handle<T>(
  shared: Shared,
  name: string,
  fn: (value: T) => unknown,
  options: HandleOptions<T>,
): void {
  if (typeof name !== "string")
    throw new InvalidArgument(`a message name is a string, not ${name}`);
  const { decode, ...unknown } = options;
  if (Object.keys(unknown).length > 0) {
    throw new InvalidArgument(`unknown handle options ${Object.keys(unknown).join(", ")}`);
  }
  shared.bind([name], handler(fn, decode));
}

/** Binds every handler of `handlers` at once, so that the past messages of their names come in
 * the order they arrived. */
export function handleAll(
  shared: Shared,
  handlers: Readonly<Record<string, (value: never) => unknown>>,
): void {
  const byName = new Map<string, Handler>();
  for (const [name, fn] of Object.entries(handlers)) {
    byName.set(name, handler(fn as (value: unknown) => unknown));
  }
  shared.bind([...byName.keys()], (m) => (byName.get(m.name) as Handler)(m));
}
