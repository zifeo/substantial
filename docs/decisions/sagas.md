# Undo, rollback and sagas

A step declares how it is undone, and a rollback or a saga runs the undos of the steps that ran ([Interfaces](../interfaces.md#i-10-undo-rollback-and-sagas)). An undo receives the step's value then its arguments, "so a step that returns nothing can be undone too" (same section). The work is split: "the core says which steps to undo, the host runs their undos" (`rust/src/workflow/saga.rs`). The core lists them most recent first and records each undo as a step of its own; the host runs them one at a time, with the step's retry and timeout, and raises the error the code sees.

`RolledBack` and `UndoFailed` reach the code as errors. A run ends as the error that leaves its workflow, so code that catches one goes on ([g039jvy6k](workflows.md#g039jvy6k)). A run whose failure ran undos is never retried ([vb6krfmej](core/lifecycle.md#vb6krfmej)).

## Undos

**§ <a id="a5c1mfs8d">a5c1mfs8d</a> An undo receives the step's value then the step's arguments.**

- (a) An undo runs with the retry and the timeout of the step it undoes.

## Rollback

**§ <a id="zjw5cess8">zjw5cess8</a> `c.rollback(reason)` runs, one after another, the undos the core lists for the steps of the run so far, then raises `RolledBack(reason)`.**

## Sagas

**§ <a id="ctp0q370t">ctp0q370t</a> On an error that leaves `c.saga()`, the saga runs, one after another, the undos the core lists for its steps, then raises the error again.**

- (a) A `Cancelled` that leaves a saga runs its undos as any other error does.

- (b) A `ContinuedAsNew` that leaves a saga ends it as a return does.

- (c) In Python a saga left by `asyncio.CancelledError`, as when a replay closes its code, lets the error through at once.

## An undo that fails

**§ <a id="j5gf2r47g">j5gf2r47g</a> An undo with no attempt left stops the rollback or the saga, which raises `UndoFailed` naming the undo's step.**
