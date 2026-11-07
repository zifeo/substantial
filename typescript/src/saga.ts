// Rollbacks and sagas (I-10): the core says which steps to undo, the host runs their undos.
import { create } from "@bufbuild/protobuf";
import { ContinuedAsNew, RolledBack, StepFailed, UndoFailed } from "./errors.ts";
import { decodeValue, run } from "./proto.ts";
import { makeCall, type Shared } from "./scheduler.ts";
import { sagaScope } from "./step.ts";

/** Runs each undo as the step the core names, with the retry and timeout of the step it undoes. */
async function runUndos(shared: Shared, undos: run.Called_Undos): Promise<void> {
  for (const u of undos.undos) {
    const found = shared.undos.get(u.call);
    if (found === undefined) throw shared.protocol(`undo of call ${u.call} without an undo`);
    const options = create(run.StepOptionsSchema, {
      occurrence: u.occurrence,
      timeoutMs: found.timeoutMs,
      ...(found.retry === undefined ? {} : { retry: found.retry }),
    });
    const call = makeCall({ case: "step", value: { name: u.name, options } });
    try {
      await shared.future(call, decodeValue, { body: found.body(decodeValue(u.value)) });
    } catch (e) {
      if (e instanceof StepFailed) throw new UndoFailed(u.name, e.failure);
      throw e;
    }
  }
}

export async function rollback(shared: Shared, reason: string): Promise<never> {
  const undos = shared.expect(makeCall({ case: "rollback", value: {} }), "undos");
  await runUndos(shared, undos);
  throw new RolledBack(reason);
}

type Settled<T> = { ok: true; value: T } | { ok: false; error: unknown };

export async function saga<T>(shared: Shared, fn: () => T | PromiseLike<T>): Promise<T> {
  // An inner saga ending without error hands its steps to the enclosing one.
  const parent = sagaScope.getStore();
  const id = shared.expect(makeCall({ case: "sagaBegin", value: {} }), "saga");
  let settled: Settled<T>;
  try {
    settled = { ok: true, value: await sagaScope.run(id, fn) };
  } catch (error) {
    settled = { ok: false, error };
  }
  // Continuing as new is no failure: nothing is undone.
  const failed = !settled.ok && !(settled.error instanceof ContinuedAsNew);
  const end = makeCall({ case: "sagaEnd", value: { saga: id, failed, parent } });
  if (failed) await runUndos(shared, shared.expect(end, "undos"));
  else shared.expect(end, "done");
  if (!settled.ok) throw settled.error;
  return settled.value;
}
