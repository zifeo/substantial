# Steps

A step is a function whose value is recorded (I-6). Everything that touches the outside world runs in one, because the workflow function runs again on every wakeup and a step is the only call that does not (I-2). A step may run more than once, so it is idempotent or keyed (I-2), and an interrupted step "may leave its effect half done: keep it idempotent, or keyed with `s.step_key()`" (I-6). Which attempt runs when, and how a step's outcome is resolved, is the core's; the host names the step, builds its key, runs its attempts with their timeouts, and decides what an attempt's outcome is. Where each misuse of the step API raises, such as `NotInWorkflow` outside a step, is in the reference (I-6, I-20).

## Identity and key

The core identifies a step by its name and its occurrence, the count of earlier calls of that name in the run ([twravbm6m](core/history.md#twravbm6m)). So a step's name is permanent: "renaming a step function breaks the runs in flight, so pin the old name with `name=` before a refactor" (I-2), and "two step functions with the same name in different modules collide: give one a `name=`" (I-6).

**§ <a id="wv67rmrjm">wv67rmrjm</a> A step's occurrence is taken when the code calls the step.**

- (a) In Rust, whose futures are lazy, the occurrence is taken when the step future is first polled.

How each host names a declared step, its function's bare name or, in TypeScript, the first argument of `s.step` "since bundlers rename functions" ([README](../../README.md#example)), is in the reference (I-6).

A step may run more than once, so it gives the API it calls a key that is the same for every attempt and replay: "pass it to a payment provider, an `Idempotency-Key` header, a database unique key" (I-6). A retry keeps the keys of the run it resumes, "so that a half-done effect keeps its idempotency key" (`rust/src/workflow/scheduler.rs`).

**§ <a id="b1cj6wtk9">b1cj6wtk9</a> `s.step_key()` called inside a step returns `<run>/<step>/<occurrence>`, the same for every attempt and every replay of that step.**

- (a) A retry of a failed chain builds its step keys from the first run of its line of retries, so a step whose effect was half done keeps its key.

An undo's key follows the same rule, the core naming the undo `<step>.undo` with the occurrence of the step it undoes (I-6).

How each host binds an inline step's arguments and where it runs a blocking step function is in the reference (I-6): Python binds them at the call and runs a synchronous function in a thread, "so a blocking call never stops the worker", while a TypeScript step runs on the event loop, which a blocking call stops.

## Retries and failure

A step without `retry` runs one attempt, and its first error fails the step (I-2): no retry is the default so that a card is never charged by surprise, a retry being something the code asks for (the maintainer's reason). With `retry`, its attempts back off exponentially (I-6). The core schedules the attempts from the step's policy. A `Permanent` error is for "a 404, an invalid input" (I-6). An undo runs with its step's retry ([a5c1mfs8d.a](sagas.md#a5c1mfs8d)).

**§ <a id="epxcjcz4s">epxcjcz4s</a> A step out of attempts, or failed by an error that is not retryable, raises `StepFailed`, whose `cause` is the last attempt's failure: `StepTimeout`, `ValueTooLarge`, `Json` for a value that does not encode, or the type of the error the step raised, `Error` in Rust.**

- (a) A `Permanent` error is recorded as not retryable, so the step fails at once, attempts left or not.

That every field of `Retry` is required, and what each means, is in the reference (I-6).

A step's value over the size cap fails the step at once, as "another attempt would return it again" (`rust/src/worker/replay.rs`), and a value that does not encode as JSON fails it at once too, with cause `Json` ([values](values.md#what-json-cannot-hold)).

## Timeouts

A worker renews its claim while it lives, not while a step makes progress, "so a hung step would keep the run on that worker" (I-18). So every attempt has a timeout, the worker's `step_timeout` of [the presets](workers.md#presets) unless something sets one. An undo has its step's timeout ([a5c1mfs8d.a](sagas.md#a5c1mfs8d)).

**§ <a id="aym52h8ch">aym52h8ch</a> Every step attempt has a timeout: the step's own, else its workflow's `step_timeout`, else the worker's, and an attempt past it fails with a retryable `StepTimeout`.**

A timed-out attempt runs again only as the next attempt of a step with `retry`: without one, the step fails with `StepFailed`, its cause `StepTimeout` (I-6, I-20).

## Abandoned attempts

An attempt is abandoned past its timeout, past its worker's `shutdown_grace` ([dh7y75pvn](workers.md#dh7y75pvn)), or as its replay is dropped: by a lost commit, a takeover, a divergence, a stop or the worker's death. The runtimes differ in what they can stop: "JavaScript cannot stop a function", and "a thread cannot be interrupted either" (I-6).

**§ <a id="v4n6ja42e">v4n6ja42e</a> An attempt the worker abandons, past its timeout or its worker's `shutdown_grace`, or as its replay is dropped, has its outcome dropped.**

- (a) Python cancels an `async` step at its `await`, Rust drops the step's future at its `.await`, and TypeScript aborts `s.stepSignal()` and stops awaiting the body.

- (b) A thread cannot be interrupted: a Python plain `def` and a Rust `step_blocking` run to their end, their outcome dropped, maybe while the next attempt runs.

- (c) A TypeScript body that does not pass on `s.stepSignal()` runs to its end, its outcome dropped.

A step attempt whose outcome was not committed, because its worker crashed, paused past its claim or shut down past its grace, runs again on the worker that takes the run, after its effect may already have happened (I-1, [Integration](../integration.md#what-start-and-send-guarantee), "Effects once").

A cancel is not an abandonment: "a step attempt already running is not interrupted: its outcome is recorded and returned" (I-13, [vmk2ydwta.d](core/lifecycle.md#vmk2ydwta)).

## Values

TypeScript's types are erased, so `decode` turns the recorded JSON into the value the code sees (I-2); without it the code gets the value as JSON reads it, the first time too. A value JSON cannot hold as it is, such as a `Date`, is refused rather than written as a string ([z6zyx22ss](values.md#z6zyx22ss)).

**§ <a id="vjy5p3exq">vjy5p3exq</a> A step's value is encoded to JSON and decoded before the code sees it, the first time too, so the first run and every replay see the same value of the same type.**
