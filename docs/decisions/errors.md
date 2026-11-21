# Errors

A failure is read across hosts: a client or a parent reads the failure of a run, whatever host failed it, and "matches `cause.type` (Rust `cause.kind`) without reading messages" (I-20). Control flow travels as exceptions too, and code must never swallow it (I-2, "Never swallow Substantial's control flow"). Each host gives it a type of its own idiom.

## Kinds

**§ <a id="rabbgjpb2">rabbgjpb2</a> Every host raises the same error kinds.**

How each host roots them, `SubstantialError` in Python and TypeScript, one `substantial::Error` enum in Rust, is in the reference (I-20).

Rust's `substantial::Error` also has variants for the host's own failures, such as `Trap` and `Protocol` ([the boundary](hosts.md#the-boundary); I-20).

## Control flow

**§ <a id="b7wsejcvb">b7wsejcvb</a> `Cancelled`, `RolledBack` and `ContinuedAsNew` are control flow, each host giving them a type of its own idiom.**

- (a) In Python they are `BaseException`s, which `except Exception` never catches.

- (b) In TypeScript they are subclasses of `Control`, which extends `Error`, so a `catch` catches them, and code that catches rethrows them when `isControl(e)`.

A suspension is not an error, and only Python's code sees it, so that "`finally` blocks and `async with` exits run at every suspension" (I-20).

## Failures

**§ <a id="r3sd261h4">r3sd261h4</a> A failure as a history records it is a `Failure` with `type` (Rust `kind`), `message`, `retryable` and `details`, the same in every host.**

- (a) `RunFailed` holds the run's `error` and its `cause`: for a run a step failed, `error` is the step's `StepFailed` and `cause` the last attempt's failure, so a client or a parent matches `cause.type` without reading messages.

- (b) A `StepFailed` the code lets through ends the run with the failure the core resolved the step with, its details `{"step", "attempts", "error"}` unchanged, so `retry` finds the step to run again.

- (c) A workflow's own failure keeps its type and message, and in Rust an `anyhow::Error` holding a `substantial::Error` gives it back.
