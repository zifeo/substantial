// The naming rules of I-17, checked by the host so that a refusal states the rule.
import { InvalidArgument } from "./errors.ts";

/** Run and message ids, workflow names and published keys, which go into store keys. */
export function checkKey(what: string, value: unknown): string {
  // § px8dv58y7.a@mj2p5
  // § one px8dv58y7.b@1hnrn
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
  // § px8dv58y7.a@1arv1
  if (typeof value === "string" && /^[A-Za-z0-9._-]{1,128}$/.test(value)) return value;
  throw new InvalidArgument(`${what} ${JSON.stringify(value)} must match [A-Za-z0-9._-]{1,128}`);
}

/** The name of a message sent, by a client or a workflow. */
export function checkSentName(value: unknown): string {
  const name = checkName("message name", value);
  // § j69rwh8x9@kkazj
  if (name.startsWith("substantial.")) {
    throw new InvalidArgument(
      `message name ${JSON.stringify(name)} starts with substantial., which is reserved`,
    );
  }
  // § end j69rwh8x9
  return name;
}

/** The inline step `c.schedule()` records its fires in. */
export const RESERVED_STEP = "schedule.next";

/** A step name of the code: `schedule.next` is `c.schedule()`'s. */
export function checkStepName(value: unknown): string {
  // § vnykycs34.b@6k1w2
  if (value === RESERVED_STEP) {
    throw new InvalidArgument(
      `the step name ${JSON.stringify(RESERVED_STEP)} is reserved for c.schedule()`,
    );
  }
  // § end vnykycs34.b
  const name = checkName("step name", value);
  // § px8dv58y7.c@jcbsy
  if (name.startsWith("substantial.")) {
    throw new InvalidArgument(
      `step name ${JSON.stringify(name)} starts with substantial., which is reserved`,
    );
  }
  // § end px8dv58y7.c
  return name;
}
