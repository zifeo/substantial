// Retries of transient failures and resolution of ambiguous creates, shared by every adapter.
import { sleep } from "../durations.ts";
import type { StoreRetry } from "../settings.ts";
import { type Create, StoreFailure } from "./store.ts";

/** How one attempt of an operation failed. */
export class Fault extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Nothing happened; trying again may succeed. */
export class Transient extends Fault {}

/** A create that may or may not have happened: a timeout, a 5xx, a lost connection. */
export class Ambiguous extends Fault {}

/** Trying again cannot help: an invalid key, a refused permission. */
export class Refused extends Fault {}

class Backoff {
  readonly #policy: StoreRetry;
  #delay: number;
  #attempt = 1;

  constructor(policy: StoreRetry) {
    this.#policy = policy;
    this.#delay = policy.initial.ms;
  }

  /** Waits before the next attempt, or fails once every attempt is spent. */
  async next(error: string, wait: boolean): Promise<void> {
    if (this.#attempt >= this.#policy.attempts) {
      const unit = this.#attempt === 1 ? "attempt" : "attempts";
      throw new StoreFailure(`${error} (after ${this.#attempt} ${unit})`);
    }
    this.#attempt += 1;
    if (wait) {
      await sleep(this.#delay);
      this.#delay = Math.min(this.#delay * 2, this.#policy.max.ms);
    }
  }
}

function faultOf(e: unknown): Fault {
  if (e instanceof Fault) return e;
  throw e;
}

export async function withRetry<T>(policy: StoreRetry, op: () => Promise<T>): Promise<T> {
  const backoff = new Backoff(policy);
  for (;;) {
    try {
      return await op();
    } catch (e) {
      const fault = faultOf(e);
      if (fault instanceof Refused) throw new StoreFailure(fault.message);
      await backoff.next(fault.message, true);
    }
  }
}

function same(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((byte, i) => byte === b[i]);
}

/** A create that never assumes it won or lost: an ambiguous outcome is followed by a `get`, the
 * same bytes meaning "created", other bytes "exists", and a missing key creating again. */
export async function create(
  policy: StoreRetry,
  data: Uint8Array,
  attempt: () => Promise<Create>,
  get: () => Promise<Uint8Array | undefined>,
): Promise<Create> {
  const backoff = new Backoff(policy);
  let ambiguous = false;
  // An earlier attempt left unanswered may land after the get, so a later "exists" is checked.
  let wasAmbiguous = false;
  for (;;) {
    if (ambiguous) {
      let stored: Uint8Array | undefined;
      try {
        stored = await get();
      } catch (e) {
        const fault = faultOf(e);
        if (fault instanceof Refused) throw new StoreFailure(fault.message);
        await backoff.next(fault.message, true);
        continue;
      }
      if (stored !== undefined) {
        return same(stored, data) ? "created" : "exists";
      }
      ambiguous = false;
    }
    let outcome: Create;
    try {
      outcome = await attempt();
    } catch (e) {
      const fault = faultOf(e);
      if (fault instanceof Refused) throw new StoreFailure(fault.message);
      if (fault instanceof Ambiguous) {
        ambiguous = wasAmbiguous = true;
        await backoff.next(fault.message, false);
      } else {
        await backoff.next(fault.message, true);
      }
      continue;
    }
    if (outcome === "exists" && wasAmbiguous) {
      ambiguous = true;
      await backoff.next("exists after an ambiguous create", false);
      continue;
    }
    return outcome;
  }
}
