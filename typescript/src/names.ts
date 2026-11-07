// The naming rules of I-17, checked by the host so that a refusal states the rule.
import { InvalidArgument } from "./errors.ts";

/** Run and message ids, workflow names and published keys, which go into store keys. */
export function checkKey(what: string, value: unknown): string {
  if (
    typeof value === "string" &&
    /^[a-z0-9._-]{1,100}$/.test(value) &&
    value !== "." &&
    value !== ".." &&
    !value.startsWith("substantial.")
  ) {
    return value;
  }
  throw new InvalidArgument(
    `${what} ${JSON.stringify(value)} must match [a-z0-9._-]{1,100}, . and .. excepted, and not start with substantial.`,
  );
}

/** Step and message names. */
export function checkName(what: string, value: unknown): string {
  if (typeof value === "string" && /^[A-Za-z0-9._-]{1,128}$/.test(value)) return value;
  throw new InvalidArgument(`${what} ${JSON.stringify(value)} must match [A-Za-z0-9._-]{1,128}`);
}
