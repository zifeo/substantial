// The module, its checksum and the boundary ABI (I-25), and the host's ULIDs.
import assert from "node:assert/strict";
import { test } from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { type Core, CoreModule, Kind, shippedFiles, WASM_NAME } from "../src/core.ts";
import {
  CoreError,
  LayoutError,
  ModuleError,
  NonDeterminism,
  ValueTooLarge,
} from "../src/errors.ts";
import { AgentMachine, coreError } from "../src/machines.ts";
import { ulid } from "../src/plan.ts";
import { agent, boundary, time } from "../src/proto.ts";

function nextRequest(): Uint8Array {
  return toBinary(
    agent.AgentRequestSchema,
    create(agent.AgentRequestSchema, { request: { case: "next", value: time(0) } }),
  );
}

function agentError(core: Core, machine: number, request: Uint8Array): boundary.CoreError {
  const response = fromBinary(agent.AgentResponseSchema, core.call(machine, request));
  assert.equal(response.response.case, "error");
  return response.response.value as boundary.CoreError;
}

test("the shipped module matches its checksum", async () => {
  const { wasm, sums } = await shippedFiles();
  await CoreModule.fromBytes(wasm, sums);
  await assert.rejects(CoreModule.fromBytes(wasm, `${"0".repeat(64)} ${WASM_NAME}\n`), /sha256/);
  await assert.rejects(CoreModule.fromBytes(wasm, "abc other.wasm\n"), /no substantial\.wasm line/);
  const tampered = wasm.slice();
  tampered[100] = (tampered[100] as number) ^ 1;
  await assert.rejects(CoreModule.fromBytes(tampered, sums), ModuleError);
  assert.equal(await CoreModule.shipped(), await CoreModule.shipped(), "compiled once");
});

test("invalid machines answer every call with their error", async () => {
  const core = (await CoreModule.shipped()).instantiate();
  for (const [kind, data] of [
    [9, new Uint8Array()],
    [Kind.AGENT, new Uint8Array([0xff, 0xff])],
    [Kind.AGENT, new Uint8Array()],
  ] as const) {
    const machine = core.newMachine(kind, data);
    assert.notEqual(machine, 0, "sub_new never returns 0");
    const first = agentError(core, machine, nextRequest());
    assert.deepEqual(agentError(core, machine, nextRequest()), first);
    core.dropMachine(machine);
  }
});

function config(): agent.AgentConfig {
  return create(agent.AgentConfigSchema, {
    partitions: 1,
    worker: "w",
    workflows: [{ name: "w", unversioned: true }],
    concurrency: 1,
    claimTtlMs: 30_000n,
    claimSkewMs: 5_000n,
    maxIdleMs: 1_000n,
    duePage: 100,
    duePages: 10,
    snapshotEvery: 64n,
    opRetries: 5,
    waitSeconds: 20,
    seed: new Uint8Array(32),
    tombstoneMs: 7n * 86_400_000n,
    retentionMinMs: 3_600_000n,
    recheckMs: 86_400_000n,
    settings: {
      continueAsNewAfterSegments: 1000n,
      retention: { keep: { case: "forever", value: {} } },
    },
  });
}

test("undecodable requests and unknown machines are errors", async () => {
  const core = (await CoreModule.shipped()).instantiate();
  const machine = core.newMachine(Kind.AGENT, toBinary(agent.AgentConfigSchema, config()));
  const invalid = boundary.ErrorKind.INVALID;
  assert.equal(agentError(core, machine, new Uint8Array([0xff, 0xff, 0xff])).kind, invalid);
  const response = fromBinary(agent.AgentResponseSchema, core.call(machine, nextRequest()));
  assert.equal(response.response.case, "command", "left as it was");
  assert.equal(agentError(core, 123_456, nextRequest()).kind, invalid);
  core.dropMachine(machine);
  assert.equal(agentError(core, machine, nextRequest()).kind, invalid);
});

test("core errors map to host errors", async () => {
  const machine = new AgentMachine(
    (await CoreModule.shipped()).instantiate(),
    create(agent.AgentConfigSchema),
  );
  assert.throws(
    () => machine.next(time(0)),
    (e) => e instanceof CoreError && e.kind === "INVALID",
  );
  machine.close();
  machine.close();
  const kinds = [
    [boundary.ErrorKind.NON_DETERMINISM, NonDeterminism],
    [boundary.ErrorKind.LAYOUT, LayoutError],
    [boundary.ErrorKind.CORRUPT, CoreError],
    [boundary.ErrorKind.VALUE_TOO_LARGE, ValueTooLarge],
  ] as const;
  for (const [kind, error] of kinds) {
    const e = coreError(create(boundary.CoreErrorSchema, { kind, message: "m" }));
    assert.equal(e.constructor, error);
  }
});

test("ULIDs are monotonic in the process", () => {
  const first = ulid(1_000_000);
  const same = ulid(1_000_000);
  const earlier = ulid(999_000);
  const later = ulid(2_000_000);
  assert.ok(first < same && same < earlier && earlier < later);
  const digits = "0123456789abcdefghjkmnpqrstvwxyz";
  const value = (u: string) =>
    [...u].reduce((acc, ch) => (acc << 5n) | BigInt(digits.indexOf(ch)), 0n);
  assert.equal(value(same) - value(first), 1n, "the same millisecond adds one");
  assert.equal(value(earlier) - value(same), 1n, "so does an earlier one");
  assert.equal(earlier.slice(0, 10), first.slice(0, 10), "a clock gone back keeps the last time");
  assert.equal(later.length, 26);
  assert.equal(later, later.toLowerCase());
});
