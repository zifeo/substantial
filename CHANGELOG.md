# Changelog

All notable changes to [Substantial](https://github.com/zifeo/substantial) are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Release notes are also published on [GitHub Releases](https://github.com/zifeo/substantial/releases).

## [Unreleased]

### Fixed

- Opening a Redis store during a Sentinel or Cluster failover no longer fails with `ConfigError` "has no WAITAOF" when the primary is demoted between the connection's checks: the node is checked again, and a replica is a failover in progress that the store retries.

## [0.4.0] - 2026-11-23

Substantial 0.4.0 keeps each chain in one object, its head, rewritten at every commit, and lets the store expire ended runs. It does not read the namespaces of 0.3: start it on a new namespace.

### Added

- Blocked runs: a run whose code lets out an error that is not a recorded outcome, or whose history no longer matches its code, is blocked instead of failed. Nothing is recorded, the worker logs a warning and tries the run again with a backoff, 1 s to 1 min in `script` and 10 s to 10 min in `service`, until a deploy fixes or rolls back the code. `status` reads `blocked`, and `runs` lists the run as waiting on `blocked` with its error.
- `s.Fail(type, message)` (Rust `s::Error::fail`, TypeScript `s.Fail`) ends a run failed on purpose.
- History limits: a run whose history passes 128 MiB or 100,000 events, whose replay consumes more than 10,000 messages, or whose continuation would carry more than 100,000 message ids, ends failed with the new `HistoryTooLarge`, and a commit past 80 % of a limit logs a warning once.
- `c.patched(id)` tells a run that reaches a change with the new code from one that replays a history recorded before it, in every host and in Wasm workflows.
- `c.iterate(..., upgrade=True)` and `c.schedule(upgrade=True)` continue a loop at its next boundary into the highest version of its workflow the worker serves, and `sub.schedule` with a newer version moves the running schedule instead of raising `RunExists`.
- `c.start(..., abandon=True)` (TypeScript `{ abandon: true }`, Rust `.abandon()`) starts a child that goes on after its parent ends.
- `sub.start(..., message=)` sends a message to the chain once the run is created or found.
- `run.retry(keep=n)` resets an ended run, completed, failed or cancelled, to its first `n` events and runs the code live from there.
- `Retry(deadline=)`: no attempt of a step starts once its deadline passed since the first attempt started.
- A worker whose clock is behind a run's last segment by more than `claim_skew` leaves the run to another worker and logs an error.
- The observe events `Blocked`, `NearLimit` and `Skewed`, and the `Replay` end `blocked`. `Skewed` names the chain in `run` and its current run in `current`.
- Heartbeats: `s.heartbeat()` inside a step, with a `heartbeat_timeout` on the step or its workflow, past which the attempt times out with `StepTimeout`.
- `s.testing.replay` replays recorded runs of a store against new code without writing to it, and reports each run as matched, diverged or ended.
- `history()` holds `patched` events, and a `step_retry` event says `deadline` when the deadline stopped the retries.
- The store contract has six operations in every adapter of every host: an exclusive create, a compare-and-set naming an ETag, a get answering the ETag, a list, a delete, and an expiry in whole days that every write carries.
- `Substantial.create` and `open` probe the store: a second create of a key must answer `exists`, a compare-and-set naming an ETag the key never had must answer `mismatch`, and so must one naming the ETag of a deleted key. A store failing a probe is refused with `ConfigError`. Each `create` and `open` probes keys of its own, `probe/<id>/` under a random lowercase ULID, written with an expiry of 1 day and deleted after the probes, so processes opening one namespace at once never refuse a healthy store.
- Every run stays readable by its id while it is kept: `status`, `result` and `history` of a run its chain continued read its archive, and those of a later run go through its pointer to its chain.
- A child's end or a cancel that reaches a chain past its retention, or a run that took its id since, is ignored without harm: a chain consumes the end only of a child it started and whose end it has not consumed, and a parent's cancel reaches only a live child whose head names it. A run that took an expired parent's id and starts the same child adopts it and gets its end, even when the child ended before.
- A divergence blocks the run whatever the code does with it: code that catches it, an `except Exception` or a `catch` that then raises `Fail`, still leaves the run blocked, not failed.
- On `s3://` and `h3://`, a worker checks at start that the bucket has a lifecycle rule for its retention, for each of its workflows' and for 1 day, the expiry of the probe keys a crash during `create` or `open` leaves, and dies with a `ConfigError` holding the JSON of the missing rule. The check needs `s3:GetLifecycleConfiguration`, and adding the rule `s3:PutLifecycleConfiguration`.
- The price `cas`, 5,000,000 nano-USD per 1,000 requests in the AWS table as a PUT, required with the other prices; the budget meter `cas`; and `cas` in `Cost` and its meters.

### Changed

- Errors that are not a recorded outcome block the run instead of failing it: `NonDeterminism`, a user exception, a result, message or loop state JSON cannot hold (`InvalidArgument`, `MessageDecode`, `NotCompressible`), a result or a continuation's input or state past 64 MiB (`ValueTooLarge`), a `ConfigError` of a schedule, and a Wasm workflow that traps. A run fails only with `Fail`, `StepFailed`, `BudgetExceeded`, `RolledBack`, `UndoFailed`, `HistoryTooLarge`, `RunFailed`, `WaitTimeout`, `RunNotFound` or `RunEnded`.
- Rust: an `anyhow` error a workflow returns blocks its run, `Error::failed` is renamed `Error::fail`, and `Retry` gains the field `deadline`, so a struct literal of it needs `deadline: None`.
- A Wasm workflow's unmatched failure of type `Json`, `InvalidArgument` or `NonDeterminism` blocks its run, and any other ends it failed as `Fail`.
- A continuation that names its own workflow with a version continues into that version.
- A run's end, completed or failed as well as cancelled, cancels its children still running but those started with `abandon`, with the reason `the parent run <run> completed` or `the parent run <run> failed`.
- A worker leases a run only as its replay is about to start its first step attempt: a wakeup that runs no step writes no lease, and a run in such a replay reads `suspended` or `pending`, not `running`.
- A commit sends its independent creates and deletes together, which saves round trips, not operations.
- `Substantial.create` and `open` refuse an S3 directory bucket with `ConfigError`, as its listings are not in byte order. Bucket versioning is optional on `s3://` and `h3://`: the adapters read no versioning, and the reference gives the lifecycle rules that bound what a versioned bucket keeps.
- **Breaking:** the store layout 3, the head layout. A chain's current run lives whole in one object, its head, rewritten by a compare-and-set at every commit; a continuation archives the run before and points the next run's id to its chain; a lease lives in the head; a message is an entry of its chain's queue holding its body, due at once whatever its sender's clock; published values live in the head, readable once the commit that recorded them lands. A namespace of Substantial 0.3, of layout 2, is refused with `ConfigError` and never read: there is no migration.
- **Breaking:** the store removes an ended run by expiry, at least its retention after its own end, whatever its children do, and at most 2 days after that, and no worker collects anything. On `s3://` and `h3://` each retention in use needs a lifecycle `Expiration` rule filtered by the namespace's prefix and the tag `substantial-retention=<d>d`.
- **Breaking:** `retention` is whole days from 1 to 36,500, 30 in both presets: Python `retention=30`, TypeScript `retention: 30`, Rust `.set("retention", "30d")` and `Workflow::with_retention(30)`. Any other value raises `ConfigError`.
- **Breaking:** the tombstone is gone. An id is refused to a new start exactly while its run is kept, and starts a new run once the run was removed. `RunExists` and `RunNotFound` lose `collected` and `until`, and a `RunNotFound` failure's details hold only `run`.
- **Breaking:** the settings `snapshot_every`, `tombstone`, `retention_min` and `recheck` are gone.
- **Breaking:** the file store keeps a key as a directory of numbered versions, each with a 24-byte header holding its expiry and its key's incarnation, written by `link()` alone in every host, so the three hosts share one directory and a directory written by 0.3 is not read.
- **Breaking:** `h3://` sends `x-herald-wait-seconds` and `x-herald-wait-etag`, and reads `x-herald-listing-etag`: a server that answers only the `x-substantial-*` names fails every waiting listing.
- Runs make fewer requests: a commit is one compare-and-set of the head, a wakeup reads one object or none on the worker that wrote the head last, and an end leaves no entry to collect. A budget in requests counts the `cas` requests among them.
- The Redis failover tests drive the writers by counts of acknowledged creates, not durations, and check that creates are acknowledged again after the promotion, on a cluster once the killed node rejoins.
- A waiting `result` on `h3://` and Redis holds a waiting listing of its chain's head, which returns as soon as the head is rewritten: a Redis page's etag covers each key's ETag, kept in the namespace's hash `{<ns>}:t`.

### Removed

- The coverage workflow.

[0.4.0]: https://github.com/zifeo/substantial/compare/v0.3.0...v0.4.0

## [0.3.0] - 2026-11-21

Wasm workflows on the Rust worker, and every host refusing what another would read otherwise. See the [README](https://github.com/zifeo/substantial/blob/v0.3.0/README.md).

### Added

- Wasm workflows: a workflow compiled to a WebAssembly component of the world `substantial:wasm/module` (`wit/workflow.wit`) runs on a Rust worker, which loads it from its bytes, when it starts or later, so its code ships without a rebuild of the worker. The Rust crate's `wasm-runner` feature loads and replays it with wasmtime, under `substantial::wasm`, and the crate `substantial` itself, built for `wasm32-unknown-unknown`, writes it with the API of the Rust host: `#[s::workflow]`, `#[s::step]`, `s::export!`, `c.sleep`, `c.receive`, `c.send`, so one source builds both as a native workflow and as a Wasm workflow. A Wasm workflow has no network, clock or filesystem: its workflow code calls only Substantial's verbs, and its steps only the interfaces the embedder adds, sync or async. An embedder may also call exports of its own beside the workflow, run a module its own workflow chooses with `Module::run`, and gives each module its bounds: a memory per instance and a time for its workflow code between two waits, past which it traps with `WasmTrap`.

### Changed

- Every host refuses a value JSON cannot hold as it is instead of writing it as another value: Python a dict key that is not a string and a tuple, which it wrote as a list, TypeScript a `Date`, a `Map` or any object that is not plain, `undefined` inside an object or an array and a hole of a sparse array, before any `toJSON`, and Rust a NaN or an infinity.
- A client's `send` refuses a message name starting with `substantial.`, as a workflow's does, and every host refuses a step name starting with `substantial.`.
- `sub.schedule` sends its config with the id `schedule-<revision>-<digest>`, and a run that receives two configs of one revision fails with `ConfigError` instead of dropping the second.
- Rust fails a run whose message handler panics with `MessageDecode`, as Python and TypeScript do for a handler that raises.
- TypeScript stops a diverged replay's attempts, as Rust and Python do, and prints its end warning without a `substantial: ` prefix.
- Rust's ULIDs carry past the random bits of a millisecond into its time, as Python's and TypeScript's do, instead of panicking.
- Substantial runs on Linux and macOS, and on Windows through WSL. The Rust crate no longer compiles for native Windows, as a worker listens to SIGTERM.
- Python and Rust refuse an integer past 2^53 - 1 in every value, as in inputs, since TypeScript would read it rounded.
- Rust raises `InvalidArgument` for a message or a published value JSON cannot hold, and fails a run whose loop state does not serialize with the new `NotCompressible` variant, as Python and TypeScript do.
- `receive` and `handle` refuse a message name outside the naming rules with `InvalidArgument` stating the rule, in every host, and Rust checks an inline step's name against the full rule.
- A Python message handler that returns an awaitable fails the run with `MessageDecode`, as a TypeScript one returning a promise does.
- Every host refuses a duration past 2^53 - 1 milliseconds with `InvalidArgument`, as TypeScript did: in Rust, a longer sleep or timeout stopped the worker instead of failing its run.

[0.3.0]: https://github.com/zifeo/substantial/compare/v0.2.0...v0.3.0

## [0.2.0] - 2026-11-17

A new version, not compatible with 0.1.0: a Rust core in WebAssembly with Rust, Python and TypeScript hosts. See the [README](https://github.com/zifeo/substantial/blob/v0.2.0/README.md).

### Added

- Durable workflows over memory, file, S3 and Redis stores.
- Live runs, history and retry.
- Cost, charges and budgets.
- Observe events.
- Awaiting any chain, child cancel, receive with timeout, and polling.
- Schedules with backfill, and draining workers.

### Changed

- The store layout 2 is not compatible with the namespaces of 0.2.0-beta.1 and 0.2.0-beta.2: drain them with the old version and start on a new namespace (see [docs/interfaces.md](https://github.com/zifeo/substantial/blob/v0.2.0/docs/interfaces.md)).

[0.2.0]: https://github.com/zifeo/substantial/compare/v0.1.0...v0.2.0

## [0.2.0-beta.1] - 2026-11-02

A new version, not compatible with 0.1.0, for Rust, Python and TypeScript. See the [README](https://github.com/zifeo/substantial/blob/v0.2.0-beta.1/README.md).

[0.2.0-beta.1]: https://github.com/zifeo/substantial/releases/tag/v0.2.0-beta.1

## [0.1.0] - 2025-01-05

### Added

- Utilities on the `Context` object ([#10](https://github.com/zifeo/substantial/pull/10)).
- Advanced filters ([#11](https://github.com/zifeo/substantial/pull/11)).

[0.1.0]: https://github.com/zifeo/substantial/compare/v0.0.1...v0.1.0

## [0.0.1] - 2024-10-26

[0.0.1]: https://github.com/zifeo/substantial/releases/tag/v0.0.1
