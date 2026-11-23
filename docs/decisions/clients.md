# Clients

A client starts runs, sends them messages and follows their outcome (I-17). Most callers are web routes answering a provider that retries until it gets a 2xx, so a start and a send are idempotent under ids the caller picks, and durable when they return: the route is idempotent "as long as the provider retries until it gets a 2xx and the ids are stable" ([Integration](../integration.md#what-start-and-send-guarantee)). What a start, a send or a cancel writes, and what it refuses, is planned by the core ([jbkv4v0we](core/lifecycle.md#jbkv4v0we), [dnxwdv0ac](core/messages.md#dnxwdv0ac)); the host checks names, draws the ids that order messages, and runs the loop of a `result` wait.

## Ids and names

Ids and names become store keys. Keys are lowercase, so a file system that folds case holds them as they are ([Interfaces](../interfaces.md#the-store-contract), "a file system that folds case (macOS by default) holds them as they are"). Many external ids are case-sensitive, and lowercasing one would merge it with another, so an external id is hashed into a run or message id instead (I-17, "lowercasing `evt_1AbC` would merge it with `evt_1abc`").

**§ <a id="px8dv58y7">px8dv58y7</a> Run and message ids, workflow names and published keys match `[a-z0-9._-]{1,100}`, `.` and `..` excepted, and step and message names match `[A-Za-z0-9._-]{1,128}`.**

- (a) Anything else raises `InvalidArgument`, whose message states the rule, where the name is given or declared.

- (b) A run id, message id, workflow name or published key starting with `substantial.` raises `InvalidArgument`.

- (c) A step name starting with `substantial.` raises `InvalidArgument` in every host, where the step is declared, else at its first call, and a Rust declared step of such a name fails to compile.

**§ <a id="cq034wpjp">cq034wpjp</a> A `start` names its run by the `id` the caller gives, else by a ULID the host draws, in lowercase.**

A start with an id already started for the same workflow, version, input and budget returns that run, and with another one raises `RunExists` ([jbkv4v0we](core/lifecycle.md#jbkv4v0we)). The input is compared in its canonical encoding ([values](values.md#canonical-inputs)).

## Sending and cancelling

A run consumes its messages in the order of their `ord`, the sender's ULID ([a1qvxtaj6](core/messages.md#a1qvxtaj6)). A send or a cancel to a chain whose last run ended raises `RunEnded`, to a chain that does not exist `RunNotFound`, and once it returns, the message's entry exists in the store (I-17; [Integration](../integration.md#what-start-and-send-guarantee), "Durable"). The host's ULIDs are written in lowercase Crockford base32, "as run and message ids are lowercase (I-17)", and each one sorts after the last, "so a client's messages are consumed in the order sent" (`rust/src/plan.rs`). A cancel is the message `substantial.cancel` ([vmk2ydwta](core/lifecycle.md#vmk2ydwta)).

**§ <a id="wtwx1e769">wtwx1e769</a> A client's `send` draws one ULID per call, its `ord`, which orders its message, an `id` given or not.**

- (a) A `send` without an `id` takes that ULID as its id, so it is a new message every time.

- (b) The host's ULIDs are monotonic in the process: one drawn in the same millisecond as the last, or earlier after the clock went back, is the last plus one.

- (c) A client's `cancel` draws one ULID per call, which orders it and is its id, so every cancel is a new message.

A run that waits for the first of many messages, a cart that the first item creates, needs a start that also sends: two calls would race another client's start and send.

**§ <a id="fqv5ts8vg">fqv5ts8vg</a> A start given a message sends it to the chain once the start created the run or found it, as `send` would, and sends nothing when the start is refused.**

- (a) A message given to a start whose chain already ended raises `RunEnded`, and its name and id follow the rules of `send`.

## Waiting for the end

A `result` wait may last hours or days. The interval of its polls trades requests for latency: "a longer cap costs fewer GET on a long wait, and sees the end up to `max_interval` after it is written" ([Architecture](../architecture.md#clients)), and a short interval first sees a quick end soon. The curve and its default cap, and what a wait costs by the hour and by the day, are in the architecture document. Which key each poll reads is the core's: the chain's head, which every continuation rewrites and the chain's expiry removes, so a wait sees its chain's end and its removal alike, and a wait on a chain its retention removed raises `RunNotFound` (I-17).

**§ <a id="y025f8qen">y025f8qen</a> Outside a test environment, `result` polls at an interval that starts short and doubles up to a cap, `max_interval` when given, so a quick end is seen soon and a long wait costs few reads.**

- (a) ~~On a store whose listings wait, it holds a waiting listing of the last run's `result` key instead of polling.~~

- (b) `result` reads the end from the store, so it waits as well in a process that runs no worker.

- (c) ~~A `result` wait follows its chain again from its first run at least once per `retention_min`, so a wait on a chain collected meanwhile ends with `RunNotFound` instead of polling forever.~~

- (d) ~~A waiting listing it holds lasts at most until the next follow.~~

- (e) On a store whose listings wait, it holds a waiting listing of its chain's head instead of polling, and reads the head again each time the listing returns, which it does as soon as the head is rewritten.

## Status, listing, history and retry

`status` reads the run's state from the store ([f91jrjyjy](core/lifecycle.md#f91jrjyjy)), `history` gives the events as the core renders them ([bq8tt3bh7](core/history.md#bq8tt3bh7)), and `retry` runs a failed chain again, its recorded steps kept ([vb6krfmej](core/lifecycle.md#vb6krfmej)). `runs` reads a workflow's queue and the head of each chain it finds there, and "its pages come as the iteration goes, so stopping early costs less" ([Architecture](../architecture.md#clients)).

A blocked run waits for a fix of its code, which only a person deploys, so it must be found without reading logs.

**§ <a id="hzew2nw4v">hzew2nw4v</a> A blocked run's `status` is `blocked`, and `runs` lists it as waiting on a fix, with the error of its latest block and when it is tried again.**

A run that ended may need its later work done again, after a bug in a step that completed: a reset gives the chain a new run that keeps the run's history up to a point and runs the code live from there.

**§ <a id="grzez328c">grzez328c</a> `retry` given `keep` starts a new run of a chain whose last run ended completed, failed or cancelled, that replays the first `keep` events of that run's history and runs the code live from there.**

- (a) A run that has not ended or ended by a rollback, a point past the end, and a point that drops a child start or an undo made after it raise `InvalidArgument`.
