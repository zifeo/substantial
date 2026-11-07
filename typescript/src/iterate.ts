// Loops and compression (I-12): in TypeScript only the loop's state crosses a compression.
import { ContinuedAsNew, InvalidArgument, messageOf, NotCompressible } from "./errors.ts";
import { capped, decodeValue, jsonText, jsonValue } from "./proto.ts";
import { makeCall, type Shared } from "./scheduler.ts";

const encoder = new TextEncoder();

function shouldContinueAsNew(shared: Shared): boolean {
  const value = decodeValue(
    shared.expect(makeCall({ case: "shouldContinueAsNew", value: {} }), "value"),
  );
  if (typeof value !== "boolean") throw shared.protocol(`shouldContinueAsNew answered ${value}`);
  return value;
}

// A class instance is its own fields; anything else but an object goes as is, for the core to
// refuse.
function fields(state: unknown): unknown {
  const object = typeof state === "object" && state !== null && !Array.isArray(state);
  return object ? { ...state } : state;
}

function encodeState(state: unknown, error: (message: string) => Error): Uint8Array {
  let json: Uint8Array;
  try {
    json = encoder.encode(jsonText(fields(state)));
  } catch (e) {
    throw error(`the state of iterate does not serialize to JSON: ${messageOf(e)}`);
  }
  return capped(json, "the state of iterate");
}

/** The chain's input, which each run of a loop starts with. */
function chainInput(shared: Shared): Uint8Array {
  const input = shared.start.input;
  return input?.kind.case === "json" ? input.kind.value : encoder.encode("{}");
}

/** `for await (const s of c.iterate(init))`: `init` itself at each iteration, its fields those
 * a previous run of the chain carried, if any. */
export class Iteration<S extends object> implements AsyncIterableIterator<S> {
  readonly #shared: Shared;
  readonly #state: S;
  #started = false;

  constructor(shared: Shared, init: S) {
    this.#shared = shared;
    this.#state = init;
  }

  [Symbol.asyncIterator](): this {
    return this;
  }

  async next(): Promise<IteratorResult<S>> {
    const shared = this.#shared;
    if (!this.#started) {
      this.#started = true;
      const initial = encodeState(this.#state, (m) => new InvalidArgument(m));
      const value = shared.expect(
        makeCall({ case: "iterate", value: jsonValue(initial) }),
        "value",
      );
      const carried = decodeValue(value);
      if (typeof carried !== "object" || carried === null || Array.isArray(carried)) {
        throw shared.protocol(`iterate answered ${JSON.stringify(carried)}`);
      }
      Object.assign(this.#state, carried);
      return { value: this.#state, done: false };
    }
    if (shouldContinueAsNew(shared)) {
      const state = encodeState(this.#state, (m) => new NotCompressible(m));
      throw new ContinuedAsNew(undefined, undefined, chainInput(shared), state);
    }
    return { value: this.#state, done: false };
  }
}
