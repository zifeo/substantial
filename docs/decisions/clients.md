# Clients

A client starts runs, sends them messages and follows their outcome (I-17). Most callers are web routes answering a provider that retries until it gets a 2xx, so a start and a send are idempotent under ids the caller picks, and durable when they return: the route is idempotent "as long as the provider retries until it gets a 2xx and the ids are stable" ([Integration](../integration.md#what-start-and-send-guarantee)). What a start, a send or a cancel writes, and what it refuses, is planned by the core ([ct862686r](core/lifecycle.md#ct862686r), [vcgwgaf4n](core/messages.md#vcgwgaf4n)); the host checks names, draws the ids that order messages, and runs the loop of a `result` wait.

## Ids and names

Ids and names become store keys. Keys are lowercase, so a file system that folds case holds them as they are ([Interfaces](../interfaces.md#the-store-contract), "a file system that folds case (macOS by default) holds them as they are"). Many external ids are case-sensitive, and lowercasing one would merge it with another, so an external id is hashed into a run or message id instead (I-17, "lowercasing `evt_1AbC` would merge it with `evt_1abc`").

**§ <a id="px8dv58y7">px8dv58y7</a> Run and message ids, workflow names and published keys match `[a-z0-9._-]{1,100}`, `.` and `..` excepted, and step and message names match `[A-Za-z0-9._-]{1,128}`.**

- (a) Anything else raises `InvalidArgument`, whose message states the rule, where the name is given or declared.

- (b) A run id, message id, workflow name or published key starting with `substantial.` raises `InvalidArgument`.

- (c) A step name starting with `substantial.` raises `InvalidArgument` in every host, where the step is declared, else at its first call, and a Rust declared step of such a name fails to compile.

**§ <a id="cq034wpjp">cq034wpjp</a> A `start` names its run by the `id` the caller gives, else by a ULID the host draws, in lowercase.**

A start with an id already started for the same workflow, version, input and budget returns that run, and with another one raises `RunExists` ([ct862686r](core/lifecycle.md#ct862686r)). The input is compared in its canonical encoding ([values](values.md#canonical-inputs)).

## Sending and cancelling

A run consumes its messages in the order of their `ord`, the sender's ULID ([a1qvxtaj6](core/messages.md#a1qvxtaj6)). A send or a cancel to a chain whose last run ended raises `RunEnded`, to a chain that does not exist `RunNotFound`, and once it returns, the message's inbox key and the run's due entry exist in the store (I-17; [Integration](../integration.md#what-start-and-send-guarantee), "Durable"). The host's ULIDs are written in lowercase Crockford base32, "as run and message ids are lowercase (I-17)", and each one sorts after the last, "so a client's messages are consumed in the order sent" (`rust/src/plan.rs`). A cancel is the message `substantial.cancel` ([vmk2ydwta](core/lifecycle.md#vmk2ydwta)).

**§ <a id="wtwx1e769">wtwx1e769</a> A client's `send` draws one ULID per call, its `ord`, which orders its message, an `id` given or not.**

- (a) A `send` without an `id` takes that ULID as its id, so it is a new message every time.

- (b) The host's ULIDs are monotonic in the process: one drawn in the same millisecond as the last, or earlier after the clock went back, is the last plus one.

- (c) A client's `cancel` draws one ULID per call, which orders it and is its id, so every cancel is a new message.

## Waiting for the end

A `result` wait may last hours or days. The interval of its polls trades requests for latency: "a longer cap costs fewer GET on a long wait, and sees the end up to `max_interval` after it is written" ([Architecture](../architecture.md#clients)), and a short interval first sees a quick end soon. The curve and its default cap, and what a wait costs by the hour and by the day, are in the architecture document. Which key each poll reads, and how a chain is followed to its last run, is the core's ([hegd7d4bp](core/lifecycle.md#hegd7d4bp)). Polling one run's `result` key never sees that run collected (`rust/src/client/run.rs`, "as polling one run's `result` never sees that run collected (I-17)"), so a wait follows its chain again from its first run now and then, and a wait on a chain that gets collected raises `RunNotFound` (I-17).

**§ <a id="y025f8qen">y025f8qen</a> Outside a test environment, `result` polls at an interval that starts short and doubles up to a cap, `max_interval` when given, so a quick end is seen soon and a long wait costs few reads.**

- (a) On a store whose listings wait, it holds a waiting listing of the last run's `result` key instead of polling.

- (b) `result` reads the end from the store, so it waits as well in a process that runs no worker.

- (c) A `result` wait follows its chain again from its first run at least once per `retention_min`, so a wait on a chain collected meanwhile ends with `RunNotFound` instead of polling forever.

- (d) A waiting listing it holds lasts at most until the next follow.

## Status, listing, history and retry

`status` reads the run's state from the store ([gt35c42xy](core/lifecycle.md#gt35c42xy)), `history` gives the events as the core renders them ([bq8tt3bh7](core/history.md#bq8tt3bh7)), and `retry` runs a failed chain again, its recorded steps kept ([vb6krfmej](core/lifecycle.md#vb6krfmej)). `runs` reads a workflow's queue, never the histories ([j08c2ntyv](core/lifecycle.md#j08c2ntyv)), and "its pages come as the iteration goes, so stopping early costs less" ([Architecture](../architecture.md#clients)).
