# One core, three hosts

The protocol lives in a core compiled to WebAssembly, with no I/O, clock or randomness of its own: every host loads the same module and only runs the code and talks to the store, so a namespace can be shared by workers and clients in all three languages (README, "One core"). The core decides each store operation; the host performs it, runs the workflow code and steps, and keeps time ([Architecture](../architecture.md#how-it-works), "One core"). The decisions of the core are those of [its own project](core/lifecycle.md). How a call crosses the boundary, one protobuf message in and one out, is in the reference (I-23).

## The split

**§ <a id="fsvns3j6x">fsvns3j6x</a> Every host loads the same core module, which decides every store operation, while the host performs them, runs the workflow code and its steps, and keeps time.**

- (a) The core holds three kinds of machine, each driven by one request in and one response out: a run machine per replay, an agent machine per worker, and a plan machine per client action.

**§ <a id="gpzydmca3">gpzydmca3</a> The core has no I/O, no clock and no randomness of its own and imports nothing: the host passes it the time, random bytes and the answers of the store.**

- (a) A worker seeds its agent from its OS CSPRNG, from which each replay gets a seed of its own and the worker its phase.

- (b) The ULIDs of `start` and `send` come from the host.

The checksum is checked before the module is loaded (I-25).

**§ <a id="hs5x9cc61">hs5x9cc61</a> A host loads the core only if the module's SHA-256 matches its line in `wasm/SHA256SUMS`, and a mismatch, a failed compilation or a missing export is a `Module` error.**

## The boundary

**§ <a id="b80k2s0e4">b80k2s0e4</a> A trap is a bug of the core: the host drops the instance and its machines, and reports it as a `Trap` error.**

- (a) The core answering what the host does not expect at that point is a `Protocol` error.

## The scheduler

Parallel branches are deterministic only if their outcomes reach the code in the same order on every replay. The core records that order; each host makes it observable by running the code on an executor of its own and delivering one outcome at a time (I-24: "That one rule makes branches deterministic"). Which executor each host uses, and what quiescent means on it, is in the reference (I-24).

**§ <a id="k3zma9hjs">k3zma9hjs</a> The host delivers one `Next` of the core at a time, runs the workflow code to quiescence, and only then asks the core for the next one.**

- (a) After `deliver` and `child_ended` the host evaluates the `wait_until` predicates before the next `next`.

- (b) The host writes the code's logs only while the replay is live, as the core says with `live`, so a log line is written once and never at a replay.
