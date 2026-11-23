# The replay drive

The core decides what a replay does next; each host runs the workflow code, hands it the core's answers one at a time on an executor of its own, evaluates the `wait_until` predicates and writes the code's logs only while the replay is live ([k3zma9hjs](hosts.md#k3zma9hjs)). What is left to each host is what it does with the code where a replay stops: at a suspension and at the end of the function.

## Suspension

When every branch of the function waits, the run is suspended, and nothing runs until the next wakeup (I-1). In Python the code sees it: the host cancels the code's task, so "`finally` blocks and `async with` exits run at every suspension", while "TypeScript leaves the code's promises pending, and Rust drops its future: nothing reaches the code" (I-20).

**§ <a id="d9818h4m4">d9818h4m4</a> When the core ends a replay idle, the host drops the code where it waits, so no process holds a suspended run.**

- (a) Python cancels the code's tasks on the replay's loop, which raises `asyncio.CancelledError` at the `await` each is blocked on.

Python code that catches that `CancelledError` and goes on gets a `ProtocolError` at its next call (I-20); what the other control flow looks like in each host is in [errors](errors.md#b7wsejcvb).

## Ends

**§ <a id="g039jvy6k">g039jvy6k</a> ~~The host ends a run with what leaves its workflow function: a value completes it, `Cancelled` cancels it, `ContinuedAsNew` continues it, and any other error fails it with that error's type.~~**

- (a) ~~A value that does not encode as JSON fails the run with `Json`.~~

- (b) ~~In Python, a code task cancelled from outside the code ends the run failed with `CancelledError`.~~

- (c) ~~The `Cancelled` of the run's own `c.cancel` and the `RolledBack` of `c.rollback` record no end: code that catches one ends the run as it then ends it, completed if it returns, while a cancel from outside ends the run cancelled whatever its code does.~~

A run's end is recorded, so every replay must reach it the same way: an error of a Substantial verb is such an outcome, the same at every replay, while any other error is a bug of the code that the next deploy fixes. Failing the run on a bug would end it for good, when a fix could let it finish, so a bug blocks the run instead: nothing is recorded, and the run is tried again until code that runs it is deployed (I-20).

**§ <a id="dxgjekysz">dxgjekysz</a> The host ends a run with what leaves its workflow function: a value completes it, `Cancelled` cancels it, `ContinuedAsNew` continues it, an error that is a recorded outcome fails it with that error, and any other error blocks the run, recording nothing.**

- (a) A value that does not encode as JSON blocks the run with `Json`, and one past the cap of a value with `ValueTooLarge` ([h8wg39cct.e](values.md#h8wg39cct)).

- (b) In Python, a code task cancelled from outside the code blocks the run with `CancelledError`.

- (c) The `Cancelled` of the run's own `c.cancel` and the `RolledBack` of `c.rollback` record no end: code that catches one ends the run as it then ends it, completed if it returns, while a cancel from outside ends the run cancelled whatever its code does.

- (d) The errors that fail a run are `Fail`, which the code raises to fail its run on purpose with a type and a message of its own, a `StepFailed`, `BudgetExceeded`, `RolledBack`, `UndoFailed`, `HistoryTooLarge`, `RunFailed`, `WaitTimeout`, `RunNotFound` and `RunEnded`.

- (e) A replay that blocks stops the code and its step attempts as a dropped replay does, and logs a warning naming the run and the error.

An `UndoFailed` the code catches is no different from any error it catches. A result over the size cap fails the run ([h8wg39cct](values.md#h8wg39cct)), and a `StepFailed` the code lets through keeps the core's details ([r3sd261h4](errors.md#r3sd261h4)).

## Cancellation

A run can be cancelled by a client, by its parent or by itself ([Interfaces](../interfaces.md#i-13-cancellation)). An outside cancel is the message `substantial.cancel`: the run raises `Cancelled` at every call it is blocked on and ends cancelled whatever its code does after ([vmk2ydwta](core/lifecycle.md#vmk2ydwta)). "The code may catch it and still run steps or a rollback", and "Undos are never cancelled" ([Interfaces](../interfaces.md#i-13-cancellation)). A run that ends, cancelled, completed or failed, cancels its children still running but those started with `abandon` ([fca4h4ker](core/lifecycle.md#fca4h4ker)). A step attempt already running when the cancel lands is not interrupted ([vmk2ydwta.d](core/lifecycle.md#vmk2ydwta)).

A parent's `child.cancel(reason)` cancels a child "as a client's `run.cancel` does", and "It is recorded like a workflow's send" ([Interfaces](../interfaces.md#i-11-starting-other-workflows)). A client's `run.cancel` is in [Clients](clients.md). A reason is a string in every host, and where a host refuses another is in the reference (I-13).

The run's own `c.cancel(reason)` raises `Cancelled(reason)`, in Rust returns it (I-13). That `Cancelled` ends the run as any error that leaves the workflow ([dxgjekysz](#dxgjekysz)): cancelled if it leaves, and as the code ends if the code catches it ([dxgjekysz.c](#dxgjekysz)).

## Divergence

At the first call the history does not answer, the run blocks with `NonDeterminism` ([rbwstdetg](core/history.md#rbwstdetg)): "the host stops the code and the run is blocked" (I-5), until code that matches its history is deployed or the change gets a new version. The replay is dropped there, so no attempt in flight has its outcome recorded, and every host stops the code and its attempts there as it stops them at a dropped replay ([v4n6ja42e.a](steps.md#v4n6ja42e)): "dropping the future and the attempts in flight stops the code where it is" (`rust/src/worker/replay.rs`). A blocked run's error and its next try are kept in a hint ([nmpjmw8mf.d](core/commit.md#nmpjmw8mf)), and its status reads `blocked` ([hzew2nw4v](clients.md#hzew2nw4v)).

## Versions served

A worker given only versions of a name leaves the runs of that name without a version to other workers (I-5): which runs a worker takes is the agent's ([d9vmk6y6y.d](core/lifecycle/queue.md#d9vmk6y6y)), and which code replays a run is in [versions](promises.md#pjrbw31h4).
