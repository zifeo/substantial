// Retries of transient failures and resolution of ambiguous creates, shared by every adapter.
import { sleep } from "../durations.ts";
import type { StoreRetry } from "../settings.ts";
import { sent } from "./attempts.ts";
import { type Cas, type Create, StoreFailure, type StoreObject } from "./store.ts";

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

/** How a backoff waits, in ms: a timer on the real clock. */
export type Sleep = (ms: number) => Promise<void>;

class Backoff {
  readonly #policy: StoreRetry;
  readonly #sleep: Sleep;
  #delay: number;
  #attempt = 1;

  constructor(policy: StoreRetry, wait: Sleep) {
    this.#policy = policy;
    this.#sleep = wait;
    this.#delay = policy.initial.ms;
  }

  /** Waits before the next attempt, or fails once every attempt is spent. */
  async next(error: string, wait: boolean): Promise<void> {
    // § zemxe46k3@dxxba
    if (this.#attempt >= this.#policy.attempts) {
      const unit = this.#attempt === 1 ? "attempt" : "attempts";
      throw new StoreFailure(`${error} (after ${this.#attempt} ${unit})`);
    }
    this.#attempt += 1;
    if (wait) {
      await this.#sleep(this.#delay);
      this.#delay = Math.min(this.#delay * 2, this.#policy.max.ms);
    }
  }
}

function faultOf(e: unknown): Fault {
  if (e instanceof Fault) return e;
  throw e;
}

export async function withRetry<T>(
  policy: StoreRetry,
  op: () => Promise<T>,
  wait: Sleep = sleep,
): Promise<T> {
  // § zemxe46k3@aehg4
  const backoff = new Backoff(policy, wait);
  for (;;) {
    sent();
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
 * same bytes meaning created, other bytes "exists", and a missing key creating again. */
export async function create(
  policy: StoreRetry,
  data: Uint8Array,
  attempt: () => Promise<Create>,
  get: () => Promise<StoreObject | undefined>,
  wait: Sleep = sleep,
): Promise<Create> {
  // § pnvmmdmz4@3zz0g
  const backoff = new Backoff(policy, wait);
  let ambiguous = false;
  // An earlier attempt left unanswered may land after the get, so a later "exists" is checked.
  let wasAmbiguous = false;
  for (;;) {
    if (ambiguous) {
      let stored: StoreObject | undefined;
      sent();
      try {
        stored = await get();
      } catch (e) {
        const fault = faultOf(e);
        if (fault instanceof Refused) throw new StoreFailure(fault.message);
        await backoff.next(fault.message, true);
        continue;
      }
      if (stored !== undefined) {
        return same(stored.data, data) ? { etag: stored.etag } : "exists";
      }
      ambiguous = false;
    }
    let outcome: Create;
    sent();
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
      // § pnvmmdmz4.a@dwh9p
      ambiguous = true;
      await backoff.next("exists after an ambiguous create", false);
      continue;
    }
    return outcome;
  }
}

/** A compare-and-set that never assumes it won or lost: an ambiguous outcome is followed by a
 * `get`, the same bytes meaning written, the ETag named still current sending it again, and
 * anything else "mismatch". */
export async function cas(
  policy: StoreRetry,
  data: Uint8Array,
  etag: string,
  attempt: () => Promise<Cas>,
  get: () => Promise<StoreObject | undefined>,
  wait: Sleep = sleep,
): Promise<Cas> {
  // § dqf0ythhp@7n7bb
  const backoff = new Backoff(policy, wait);
  let ambiguous = false;
  let wasAmbiguous = false;
  for (;;) {
    if (ambiguous) {
      let stored: StoreObject | undefined;
      sent();
      try {
        stored = await get();
      } catch (e) {
        const fault = faultOf(e);
        if (fault instanceof Refused) throw new StoreFailure(fault.message);
        await backoff.next(fault.message, true);
        continue;
      }
      if (stored !== undefined && same(stored.data, data)) return { etag: stored.etag };
      if (stored === undefined || stored.etag !== etag) return "mismatch";
      ambiguous = false;
    }
    let outcome: Cas;
    sent();
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
    if (outcome === "mismatch" && wasAmbiguous) {
      // § dqf0ythhp.a@wwf46
      ambiguous = true;
      await backoff.next("mismatch after an ambiguous compare-and-set", false);
      continue;
    }
    return outcome;
  }
}
