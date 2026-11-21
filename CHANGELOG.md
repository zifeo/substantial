# Changelog

All notable changes to [Substantial](https://github.com/zifeo/substantial) are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Release notes are also published on [GitHub Releases](https://github.com/zifeo/substantial/releases).

## [Unreleased]

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
