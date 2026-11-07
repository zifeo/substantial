// Undo, rollback and sagas (I-10), end to end.
import assert from "node:assert/strict";
import { createOn } from "../src/client.ts";
import {
  type Context,
  type Failure,
  InvalidArgument,
  milliseconds,
  Permanent,
  RunFailed,
  StepFailed,
  type Substantial as Sub,
  step,
  type Workflow,
  workflow,
} from "../src/mod.ts";
import { sagaScope } from "../src/step.ts";
import { eachAdapter, LONG, script } from "./helpers.ts";

/** An inline step returning `name` whose undo logs the value it was given. */
function undoable(c: Context, log: unknown[], name: string): Promise<string> {
  return c.step(name, () => name, { undo: (value) => log.push(value) });
}

async function rolledBack(sub: Sub, w: Workflow): Promise<Failure> {
  await using _ = script(sub, [w]);
  const failed = await (await sub.start(w)).result({ timeout: LONG }).catch((e) => e);
  assert.ok(failed instanceof RunFailed, String(failed));
  assert.equal(failed.status, "failed");
  return failed.error;
}

eachAdapter("a rollback undoes every step most recent first", async (store) => {
  const log: unknown[] = [];
  const withArgs = step("c", (_n: number, _s: string) => "c", {
    undo: (value, n, s) => log.push([value, n, s]),
  });
  const rolling = workflow("rolling", async (c: Context) => {
    await undoable(c, log, "a");
    await c.step("plain", () => null);
    // A replay after the sleep binds the undos again from the recorded steps.
    await c.sleep(milliseconds(100));
    await withArgs(1, "x");
    await c.rollback("why");
  });
  const sub = await createOn(store, { partitions: 1 });
  const error = await rolledBack(sub, rolling);
  assert.deepEqual([error.type, error.message], ["RolledBack", "why"]);
  assert.deepEqual(log, [["c", 1, "x"], "a"]);
});

eachAdapter("a saga undoes its own steps then throws again", async (store) => {
  const log: unknown[] = [];
  const saga = workflow("saga", async (c: Context) => {
    await undoable(c, log, "a");
    const thrown = await c
      .saga(async () => {
        await undoable(c, log, "b");
        await undoable(c, log, "c");
        await c.step("boom", () => {
          throw new Permanent("no stock");
        });
      })
      .catch((e) => e);
    assert.ok(thrown instanceof StepFailed, String(thrown));
    assert.match(thrown.failure.message, /no stock$/);
    assert.equal(await c.saga(async () => c.step("fine", () => 7)), 7);
    await c.rollback("after the saga");
  });
  const sub = await createOn(store, { partitions: 1 });
  const error = await rolledBack(sub, saga);
  assert.equal(error.type, "RolledBack", error.message);
  assert.deepEqual(log, ["c", "b", "a"]);
});

eachAdapter("an undo without attempts left fails the run", async (store) => {
  const charge = step("charge", (_card: string) => 1, {
    undo: () => {
      throw new Permanent("refund refused");
    },
  });
  const stuck = workflow("stuck", async (c: Context) => {
    await charge("visa");
    await c.rollback("fraud");
  });
  const sub = await createOn(store, { partitions: 1 });
  const error = await rolledBack(sub, stuck);
  assert.equal(error.type, "UndoFailed", error.message);
  assert.match(error.message, /charge\.undo/);
});

eachAdapter(
  "a step of a saga that ended is invalid",
  async (store) => {
    const late = workflow("late", async (c: Context) => {
      let id: bigint | undefined;
      await c.saga(() => {
        id = sagaScope.getStore();
      });
      try {
        await sagaScope.run(id as bigint, () => c.step("late", () => 1));
      } catch (e) {
        if (e instanceof InvalidArgument) return "refused";
        throw e;
      }
      return "accepted";
    });
    const sub = await createOn(store, { partitions: 1 });
    await using _ = script(sub, [late]);
    assert.equal(await (await sub.start(late)).result({ timeout: LONG }), "refused");
  },
  ["memory", "file"],
);

eachAdapter("an inner saga ended hands its steps to the outer one", async (store) => {
  const log: unknown[] = [];
  const nested = workflow("nested", async (c: Context) => {
    await undoable(c, log, "z");
    const thrown = await c
      .saga(async () => {
        await undoable(c, log, "a");
        await c.saga(async () => undoable(c, log, "b"));
        await undoable(c, log, "c");
        await c.step("boom", () => {
          throw new Permanent("no stock");
        });
      })
      .catch((e) => e);
    assert.ok(thrown instanceof StepFailed, String(thrown));
    await c.rollback("after");
  });
  const sub = await createOn(store, { partitions: 1 });
  const error = await rolledBack(sub, nested);
  assert.equal(error.type, "RolledBack", error.message);
  assert.deepEqual(log, ["c", "b", "a", "z"]);
});
