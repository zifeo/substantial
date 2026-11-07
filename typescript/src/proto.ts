// Short names for the code generated from proto/substantial, and the JSON values of the code.
import { create } from "@bufbuild/protobuf";
import { InvalidArgument, ProtocolError, ValueTooLarge } from "./errors.ts";
import {
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

// JSON.stringify would turn NaN into null and a Map into {}: values are refused instead.
function strict(_key: string, value: unknown): unknown {
  if (typeof value === "number" && !Number.isFinite(value)) {
    throw new TypeError(`${value} is not a JSON number`);
  }
  if (typeof value === "function" || typeof value === "symbol" || typeof value === "bigint") {
    throw new TypeError(`a ${typeof value} is not JSON`);
  }
  if (typeof value === "object" && value !== null && !plain(value)) {
    throw new TypeError(`a ${value.constructor?.name ?? "object"} is not a plain JSON object`);
  }
  return value;
}

/** The JSON text of a value of the code; `undefined` is `null`, as a function without result. */
export function jsonText(value: unknown): string {
  return JSON.stringify(value === undefined ? null : value, strict);
}

// Far larger values trap the core, which then fails every run of its worker (D1).
export const MAX_VALUE = 64 * 1024 * 1024;

/** `json` itself, or `ValueTooLarge` naming `what`. */
export function capped(json: Uint8Array, what: string): Uint8Array {
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
  let json: Uint8Array;
  try {
    json = encoder.encode(jsonText(value));
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
  return JSON.parse(decoder.decode(value.kind.value));
}
