import { InvalidArgument } from "./errors.ts";

/** A duration in whole milliseconds, never negative. */
export class Duration {
  readonly ms: number;

  constructor(ms: number) {
    if (!Number.isFinite(ms) || ms < 0) {
      throw new InvalidArgument(
        `a duration is a finite number of milliseconds, not negative: ${ms}`,
      );
    }
    this.ms = Math.round(ms);
    if (!Number.isSafeInteger(this.ms)) {
      throw new InvalidArgument(`a duration of ${ms} ms is too long`);
    }
  }

  toString(): string {
    return `${this.ms}ms`;
  }
}

export function milliseconds(n: number): Duration {
  return new Duration(n);
}

export function seconds(n: number): Duration {
  return new Duration(n * 1000);
}

export function minutes(n: number): Duration {
  return new Duration(n * 60_000);
}

export function hours(n: number): Duration {
  return new Duration(n * 3_600_000);
}

export function days(n: number): Duration {
  return new Duration(n * 86_400_000);
}

/** `setTimeout` fires at once past this delay, so longer waits are cut and taken again. */
export const MAX_TIMER_MS = 2 ** 31 - 1;

/** A promise resolved after `ms`, and the function that cancels it early. */
export function delay(ms: number): { promise: Promise<void>; cancel: () => void } {
  let cancel = () => {};
  const promise = new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, Math.min(Math.max(0, ms), MAX_TIMER_MS));
    cancel = () => {
      clearTimeout(timer);
      resolve();
    };
  });
  return { promise, cancel };
}

export function sleep(ms: number): Promise<void> {
  return delay(ms).promise;
}
