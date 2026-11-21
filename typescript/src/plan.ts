// Client actions as plan machines (I-23): the host performs each store step until the outcome.
import type { Core } from "./core.ts";
import { CoreError, InvalidArgument, ProtocolError } from "./errors.ts";
import { PlanMachine } from "./machines.ts";
import { ObserverRaised } from "./observe.ts";
import type { plan } from "./proto.ts";
import { perform } from "./stores/open.ts";
import type { Store } from "./stores/store.ts";

type Outcome = Exclude<plan.PlanStep["step"], { case: "store" } | { case: undefined }>;

/** Runs a plan to its outcome, the first step that is not a store operation. */
export async function runPlan(core: Core, store: Store, input: plan.PlanInput): Promise<Outcome> {
  for await (const outcome of planOutcomes(core, store, input)) return outcome;
  throw new ProtocolError("a plan without outcome");
}

/** The outcomes of a plan, the store steps between them performed, for a plan answered in pages.
 * The machine goes on only when the next outcome is asked for, and is dropped once the caller
 * stops. An `INVALID` answer, or input refused, is the caller's argument, its names, ids and
 * budget, not a bug of the host. An observer that raises fails the plan with its error. */
export async function* planOutcomes(
  core: Core,
  store: Store,
  input: plan.PlanInput,
): AsyncGenerator<Outcome, void, undefined> {
  let machine: PlanMachine | undefined;
  try {
    machine = new PlanMachine(core, input);
    for (;;) {
      const step = machine.next().step;
      if (step.case === "store") {
        machine.feed(await perform(store, step.value));
        continue;
      }
      if (step.case === undefined) throw new ProtocolError("empty plan step");
      yield step;
    }
  } catch (e) {
    if (e instanceof CoreError && e.kind === "INVALID") throw new InvalidArgument(e.detail);
    if (e instanceof ObserverRaised) throw e.error;
    throw e;
  } finally {
    machine?.close();
  }
}

/** The case of a plan's outcome, which must be of kind `expected`. */
export function outcomeOf<K extends Outcome["case"]>(
  name: string,
  outcome: Outcome,
  expected: K,
): Extract<Outcome, { case: K }>["value"] {
  if (outcome.case !== expected) {
    throw new ProtocolError(`${name} plan answered ${outcome.case}`);
  }
  return outcome.value as never;
}

// § one cq034wpjp@y9wqr
const CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz";
const MAX_MS = 2n ** 48n - 1n;

function randomBits(): bigint {
  let random = 0n;
  for (const byte of crypto.getRandomValues(new Uint8Array(10))) {
    random = (random << 8n) | BigInt(byte);
  }
  return random;
}

/** ULIDs: 48 bits of milliseconds then 80 random bits from `bits`, in lowercase Crockford base32,
 * as run and message ids are lowercase (I-17). Monotonic: one made in the millisecond of the last,
 * or before it, is the last plus one, so a client's messages are consumed in the order sent. */
export function ulids(bits: (ms: number) => bigint): (ms: number) => string {
  let last = 0n;
  return (ms) => {
    if (!Number.isSafeInteger(ms) || ms < 0) throw new RangeError("a time after 1970");
    let value = ((BigInt(ms) & MAX_MS) << 80n) | bits(ms);
    // § wtwx1e769.b@ya1kf
    if (value >> 80n <= last >> 80n) {
      value = last + 1n;
    }
    if (value >> 128n !== 0n) throw new RangeError("ULIDs exhausted");
    last = value;
    // § end wtwx1e769.b
    let out = "";
    for (let i = 25; i >= 0; i--) {
      out += CROCKFORD[Number((value >> BigInt(i * 5)) & 31n)];
    }
    return out;
  };
}

/** The ULIDs of the process, monotonic across its clients. */
// § one gpzydmca3.b@95fex
export const ulid: (ms: number) => string = ulids(randomBits);
