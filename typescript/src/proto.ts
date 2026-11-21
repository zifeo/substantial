// Short names for the code generated from proto/substantial, and the JSON values of the code.
import { create } from "@bufbuild/protobuf";
import canonicalize from "canonicalize";
import {
  type Failure,
  InvalidArgument,
  makeFailure,
  ProtocolError,
  ValueTooLarge,
} from "./errors.ts";
import {
  type Failure as LayoutFailure,
  type Time,
  TimeSchema,
  type Value,
  ValueSchema,
} from "./gen/substantial/layout/v1/layout_pb.ts";

export * as agent from "./gen/substantial/boundary/v1/agent_pb.ts";
export * as boundary from "./gen/substantial/boundary/v1/boundary_pb.ts";
export * as plan from "./gen/substantial/boundary/v1/plan_pb.ts";
export * as run from "./gen/substantial/boundary/v1/run_pb.ts";
export * as layout from "./gen/substantial/layout/v1/layout_pb.ts";

export function time(ms: number): Time {
  return create(TimeSchema, { ms: BigInt(ms) });
}

export function now(): Time {
  return time(Date.now());
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

function plain(value: object): boolean {
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === Array.prototype || proto === null;
}

// JSON.stringify would turn NaN into null, a Map into {} and a Date into a string through its
// toJSON, drop undefined in an object and write a hole of a sparse array as null: values are
// checked whole first, and refused instead.
function strict(root: unknown): void {
  const seen = new Set<object>();
  const stack: unknown[] = [root];
  while (stack.length > 0) {
    const value = stack.pop();
    // § one z6zyx22ss@7axn9, aaev3rs1q.a@7gq07
    if (typeof value === "number" && !Number.isFinite(value)) {
      throw new TypeError(`${value} is not a JSON number`);
    }
    // § z6zyx22ss.b@drtxb
    if (value === undefined) throw new TypeError("undefined is not JSON");
    if (typeof value === "function" || typeof value === "symbol" || typeof value === "bigint") {
      throw new TypeError(`a ${typeof value} is not JSON`);
    }
    if (typeof value === "object" && value !== null) {
      if (!plain(value)) {
        throw new TypeError(`a ${value.constructor?.name ?? "object"} is not a plain JSON object`);
      }
      // A cycle is left to JSON.stringify, which refuses it.
      if (seen.has(value)) continue;
      seen.add(value);
      if (Array.isArray(value)) {
        for (let i = 0; i < value.length; i++) {
          if (!(i in value)) throw new TypeError(`the hole at ${i} of a sparse array is not JSON`);
        }
      }
      stack.push(...Object.values(value));
    }
    // § end z6zyx22ss.b
  }
}

/** The JSON text of a value of the code; `undefined` is `null`, as a function without result. */
export function jsonText(value: unknown): string {
  const whole = value === undefined ? null : value;
  strict(whole);
  // § one e1dc4dar4@kaeqe
  return JSON.stringify(whole);
}

// Far larger values trap the core, which then fails every run of its worker (D1).
// § one h8wg39cct@fy4q3
export const MAX_VALUE = 64 * 1024 * 1024;

/** `json` itself, or `ValueTooLarge` naming `what`. */
export function capped(json: Uint8Array, what: string): Uint8Array {
  // § h8wg39cct@kvhq6
  if (json.length > MAX_VALUE) {
    throw new ValueTooLarge(
      `${what} is ${json.length} bytes of JSON, over the limit of ${MAX_VALUE} bytes`,
    );
  }
  return json;
}

/** The JSON bytes of a value of the code, or `InvalidArgument` or `ValueTooLarge` naming
 * `what`. */
export function encodeJson(value: unknown, what: string): Uint8Array {
  // § h8wg39cct.c@p2zyk
  let json: Uint8Array;
  // § one z6zyx22ss.c@7sqhe
  try {
    json = encoder.encode(jsonText(value));
  } catch (e) {
    throw new InvalidArgument(`${what} is not JSON: ${e instanceof Error ? e.message : e}`);
  }
  return capped(json, what);
}

/** As `encodeJson`, canonical (RFC 8785): keys sorted, numbers as ECMAScript prints them. */
export function encodeCanonical(value: unknown, what: string): Uint8Array {
  // § aaev3rs1q.a@m4nps
  let json: Uint8Array;
  try {
    // The strict text first: canonicalize alone would turn a Map into {}.
    // § one aaev3rs1q@8s04r
    json = encoder.encode(canonicalize(JSON.parse(jsonText(value))));
  } catch (e) {
    throw new InvalidArgument(`${what} is not JSON: ${e instanceof Error ? e.message : e}`);
  }
  return capped(json, what);
}

export function jsonValue(json: Uint8Array): Value {
  return create(ValueSchema, { kind: { case: "json", value: json } });
}

/** A value of the code as recorded; `null` when absent. */
export function decodeValue(value: Value | undefined): unknown {
  if (value === undefined || value.kind.case === undefined) {
    return null;
  }
  if (value.kind.case !== "json") {
    throw new ProtocolError(
      `values other than inline JSON are not supported yet: ${value.kind.case}`,
    );
  }
  return parseJson(value.kind.value);
}

/** A failure of the core, its details decoded. */
export function failureFrom(f: LayoutFailure): Failure {
  return makeFailure(f.type, f.message, f.retryable, decodeValue(f.details));
}

/** The value of JSON bytes in UTF-8. */
export function parseJson(json: Uint8Array): unknown {
  return JSON.parse(decoder.decode(json));
}
