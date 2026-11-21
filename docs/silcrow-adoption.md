# Silcrow adoption

This tree adopts [Silcrow](https://github.com/zifeo/silcrow): the decisions of Substantial are laws in note files, and a marker ties each law to the code that decides it, or, for the core, to the test that checks it. This page records the layout, the rule that splits the laws, the decisions taken while adopting and their reasons, and the open questions. The notes themselves hold no remark about Silcrow.

## Layout

The root Silcrow project, declared in `AGENTS.md` with `decisions="docs/decisions/**/*.md"`, `code="**/*"` and `docs=""`.

The Rust guest SDK, `rust/guest`, is a project of its own with the same block and no notes. A pin reads every declaration of its project, so the SDK, which offers the Rust host's API under the same names (`Context`, `seconds`), would otherwise make the host's pins drift. Its code marks the root's laws qualified with `.:`, which pays no duty: the root's own markers, at the guest macros of `rust/macros` and at the runner, pay it.

| Notes | Laws | Carried by |
|---|---|---|
| `docs/decisions/*.md`, one note per topic from `promises.md` to `testing.md`, `guests.md` aside | 68 | the three hosts' code, `redis/*.lua` |
| `docs/decisions/guests.md`: the guest world, its runner and its Rust SDK | 14 | `wit/guest.wit`, the runner in `rust/src/guest`, the guest macros of `rust/macros`, and `rust/tests/guests.rs` |
| `docs/decisions/core/*.md`: `commit`, `lifecycle`, `history`, `steps`, `messages`, `sagas`, `children`, `cost` | 58 | the conformance tests (`rust/tests`, `python/tests`, `typescript/test`, `dev/cross`) and the layout schema in `proto/` |

`promises` states the headline promises in the users' words, each with its source, and names the law whose sentence states it: a finished step never runs again, steps run at least once, durable when it returns, idempotent under the caller's ids, ordered messages, waiting holds no process, no broker, deterministic replay, versions through a deploy, the platforms, one core in three languages, a known cost, every run ends and is collected, tests run the real protocol. A note is named by its topic, and the reading order is the one the last section of `promises` links: the architecture (`store`, `hosts`, then the core's notes), a run's life (`lifecycle`), the programming model (`clients` to `schedules`), operating (`observing` to `errors`) and testing (`testing`). Cancellation, which held no law, is a section of `workflows`, beside the ends it shares.

The documentation carries no marker: `README.md` and `docs/*.md` name laws in prose where it helps, and stay true by review.

## The split rule

A clause is a host law when host code decides it, and then every host that decides it has a marker at the deciding code. What the core decides, the host only passing a value in or reading an answer back, is a core law. A core law is carried by the conformance tests that exercise `wasm/substantial.wasm` as shipped, and the object layout by its schema. A host law that only restates a core law is cut in favour of the core law, and no marker sits on a line that only passes a value through.

A marker sits on the block that holds its clause. `§ one` marks the one statement or member that carries a clause whole, and a list of addresses when that statement carries several; `§ end` closes a stretch when the rest of the block is not governed by the clause. A test marker sits on the act and the assert that fail if its clause breaks.

## Decisions

The owner's rule for every gap: hosts are symmetric, a documented promise wins over code that breaks it, a silent change becomes an explicit failure, a value or reason with no source stays out of the laws, a law states what holds rather than what does not happen, and breaking changes are accepted.

### Values

- Every host refuses a value JSON cannot hold rather than write it as another value (`z6zyx22ss`): a NaN or an infinity anywhere, an integer past 2^53 - 1 in any value, as TypeScript's `JSON.parse` would read it rounded, and what each host's encoder would otherwise change. Python refuses a dict key that is not a string and a tuple, which `json` writes as a list; TypeScript refuses an object that is not plain before any `toJSON`, `undefined` inside an object or an array, a hole of a sparse array and a `bigint`. One encoder per host serves every value, so the refusal is the same at every call site. Every host raises `InvalidArgument` for a refused message or published value.
- Amounts are integers in every host's API; the decimal strings are the core's JSON only, as they may pass 2^53.

### Platforms

- Every host runs on Linux and macOS (`k6m3smdpk`), as the README promises, and Windows through WSL. A worker listens to SIGTERM in every host, so the Rust crate does not compile for native Windows. Python and TypeScript no longer refuse native Windows at import: the refusal was a law of absence, and a positive law has no code to carry it.

### Names and messages

- Every host refuses a message name outside the rules at `receive` and `handle`, with `InvalidArgument` stating the rule, and a client's `send` refuses a name starting with `substantial.`, as the reference promised. A step name starting with `substantial.` is reserved in every host, refused where the step is declared (the Rust macro as it compiles) or at its first call.
- A handler that raises, panics or returns an awaitable fails the run with `MessageDecode`, in every host.
- `sub.schedule` sends its config with the id `schedule-<revision>-<digest>`, so the same config from every pod is delivered once while another config of the same revision reaches the run, which fails with `ConfigError` naming both. A run keeps only the config it applied last: remembering every applied config would grow the loop's state with each revision for no case the reference names.

### Replay and control flow

- Every host stops a diverged replay's attempts, as at a dropped replay.
- The `Cancelled` of `c.cancel` and the `RolledBack` of `c.rollback` record no end, so code that catches one ends the run as it then ends it, while an outside cancel ends it cancelled.
- Rust's ULIDs carry past the random bits of a millisecond into its time, as Python's and TypeScript's do.

### What left the laws

- A client call that raises `StoreError` may have written its keys; the caller calls it again with the same ids. This is a statement of what is not guaranteed, kept in the reference, not a law.
- `result` waits in a process that runs no worker, as it reads the end from the store (`y025f8qen.b`). The test environment's paths carry no marker of it: the law is decided by the client's read.
- Clauses that said what a moment "may" do, or that no host test can observe (a replay stopped by a takeover, a message of a moment, a listing of a moment), are prose of their notes.
- Per-host API (how each host names a step, roots its errors, binds arguments), tables of fixed values and error messages are in the reference, `docs/interfaces.md`, not in the laws.
- A message sent within a collected chain's tombstone reaching no new run is prose of `core/messages.md`: what holds is that the send raises `RunNotFound`, which `ja67c3az7.a` states.

### Carriers

- `kwvhb7ehw`, no overwrite, is carried by each host's store interface at its one write, `create`; the interface as a whole is `w89qkqkaj`'s, and two markers cannot share one span. An adapter that overwrote would break `dfd6vt90h`, carried by each adapter's create.
- The file store's durable create is `c7pz5rk26`'s in every host, so `dfd6vt90h.e` is carried by the Redis adapters' `WAITAOF` alone.
- `sqbjc1adw.a`, layout 2 keeping the objects of layout 1, is carried by the layout schema in `proto/`: no object of layout 1 is in the tree for a test to read.
- A core clause's test checks what a host can observe of it; where a clause has parts no host test reaches, the test carries the parts it checks.

## Open questions

- S3 has supported conditional writes (`If-Match`, `If-None-Match`) since 2024, so "S3 cannot make an overwrite conditional" is false. The no-overwrite design (`kwvhb7ehw`) does not rest on it today, but a design that assumed S3 could not compare and swap deserves another look.
- A worker facing a due run of a version it does not serve lists that workflow every half `claim_ttl` and gets the run every `claim_ttl`, whatever its `max_idle`. The core decides it; whether that cost during a rolling deploy is intended is still to be decided.
- No CI job runs `silcrow check --all`: Silcrow is not public yet, so a runner cannot install it. The gate is the local hooks, `silcrow install` and the `silcrow` hook of `.pre-commit-config.yaml`, which a contributor without Silcrow skips. A CI job comes once a runner can install it.
- No CI job runs on macOS, so the platforms law is carried by the signal code alone and the promise for macOS is tested only on maintainers' machines.
