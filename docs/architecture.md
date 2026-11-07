# Substantial architecture: store operations and costs

What every verb does to the store, how many round trips it waits for, what happens when it goes wrong, and what that costs on S3. The guarantees are the ones of the [interfaces](interfaces.md) (`I-n`); this document counts. Every count below is measured by `rust/tests/costs.rs` against the core in `wasm/`, or read from the code where a test cannot reach (failures, pages past 1,000 keys). The core decides every store operation, so the Python and TypeScript hosts perform the same ones.

- [How it works](#how-it-works)
- [Reading the tables](#reading-the-tables)
- [Clients](#clients)
- [Workers](#workers)
- [Workflow verbs](#workflow-verbs)
- [Collection](#collection)
- [Measured runs](#measured-runs)
- [When something goes wrong](#when-something-goes-wrong)
- [Settings that drive cost](#settings-that-drive-cost)
- [Monthly cost on S3](#monthly-cost-on-s3)
- [Storage](#storage)
- [Stores whose listings wait](#stores-whose-listings-wait)
- [Redis](#redis)
- [Keeping this document true](#keeping-this-document-true)

## How it works

- **The store is the only shared thing.** No broker, no server, no queue service. Clients and workers use four operations (I-26), each one request on S3:

  | Operation | S3 request | S3 price class |
  |---|---|---|
  | create, exclusive | `PutObject` with `If-None-Match: *` | PUT, COPY, POST, LIST |
  | get | `GetObject` | GET and all other |
  | list, at most 1,000 keys | `ListObjectsV2` | PUT, COPY, POST, LIST |
  | delete | `DeleteObject` | free |

- **Keys.** Every object is written once and never overwritten. A namespace holds:

  | Key | What |
  |---|---|
  | `substantial.json` | the layout marker, with `partitions` |
  | `runs/<run>/start` | the workflow, version and input of a run |
  | `runs/<run>/log/<n>` | segment `n` of its history |
  | `runs/<run>/snap/<n>` | a snapshot of segments 0 to `n` |
  | `runs/<run>/effects/<n>` | empty: the effects of segment `n` exist |
  | `runs/<run>/inbox/<ord>-<id>` | a message not yet consumed, `ord` the sender's ULID |
  | `runs/<run>/claim/<n>-<attempt>` | a worker's claim on writing segment `n` |
  | `runs/<run>/result` | the outcome, and `written_at`, when it was written |
  | `runs/<run>/gone` | the tombstone of a collected chain: until when its id is taken |
  | `pub/<chain>/<key>/...` | a published value |
  | `queues/<workflow>/<partition>/due/<time>~<run>~<tag>` | empty: a wakeup; tagged `c`, a collection; `g`, a tombstone's end |
  | `index/children/<parent>/<child>` | empty: the children of a run |
  | `index/chains/<chain>/<seq>-<run>` | empty: the runs a chain continued into; its first run is `runs/<chain>/` |
  | `probe/exclusive` | empty: the probe of `create` and `open`, kept |

- **The commit point is an exclusive create.** A replay records what happened in a segment and commits it by creating `log/<n>`. Of two workers racing on a run, the first create wins and the other drops its work. Everything else is a hint that can be rebuilt from the histories: due entries, claims, indexes.
- **Segments.** A wakeup lists the run's keys once, reads its `start`, its latest snapshot, the segments after it and its new messages, replays the code against them, then commits a new segment. A snapshot is written every `snapshot_every` segments, so a wakeup reads at most one snapshot and 63 segments. A worker keeps what it read and committed of a run, up to 16 MiB in all: its next wakeup of the run reads only what is new.
- **Due entries.** A wakeup is an empty key sorting by time under its workflow and partition. A worker lists the first page of each partition of each workflow it serves, takes the entries due, and sleeps until the earliest entry it saw or `max_idle`, whichever comes first.
- **Claims.** Before a replay a worker creates `claim/<n>-<attempt>`, and a new attempt every half `claim_ttl` while the replay runs; a commit in the middle of a replay keeps it. A claim only avoids duplicate work; the segment decides.
- **Collection.** A run's end creates a due entry tagged `c` at its end plus its `retention`, none with `forever`. A worker serving the workflow, any version, that finds it due checks the run can go and deletes its keys. A `result` younger than twice `retention_min` (2 hours) creates the entry again at the instant it is that old; a check that fails, `recheck` (1 day) later. Collecting the first run of a chain creates `runs/<chain>/gone` first, holding until when the id is taken, and an entry tagged `g` at that time, which deletes it.
- **Partitions** split the due entries of a workflow by a hash of the run. More partitions list more due runs per round and cost more listings when idle.
- **A store whose listings wait** (`h3://`) answers a listing only once its page changes, up to 20 s: workers wait instead of polling.
- **One core.** The protocol is a WebAssembly module every host loads. It decides each store operation; the host performs it, runs the workflow code and steps, and keeps time.

## Reading the tables

- **PUT, GET, LIST, DELETE** count creates, gets, listings and deletes. On S3, PUT and LIST are billed at the same rate, GET at a lower one, DELETE is free.
- **Round trips** are the operations on the critical path, each waiting for the previous answer. A client action performs its operations one after the other, and a worker one at a time per run: their round trips equal their operation counts. The listings of a scan are sent together: one round trip per scan. At a store latency `L`, a wakeup of 20 operations spends about `20 L` in the store, plus its steps.
- **Pages.** A listing returns at most 1,000 keys; past that, one more listing per 1,000 keys. The tables assume fewer, and give the formula where a count grows.
- Retries of failed operations are not counted (see [When something goes wrong](#when-something-goes-wrong)).

## Clients

| Verb | PUT | GET | LIST | DELETE | Round trips | Operations |
|---|---|---|---|---|---|---|
| `create`, new namespace | 3 | 0 | 0 | 0 | 3 | probe twice, marker |
| `create`, existing namespace | 2 | 1 | 0 | 0 | 3 | probe (exists), marker (exists), get marker |
| `open` | 1 | 1 | 0 | 0 | 2 | get marker, probe (exists) |
| `start` | 2 | 1 | 0 | 0 | 3 | `start`, get `gone` (missing), due entry |
| `start`, same id and input again | 2 | 2 | 0 | 0 | 4 | `start` (exists), get it, get `gone`, due entry again |
| `start`, id taken: `RunExists` | 1 | 1 | 0 | 0 | 2 | `start` (exists), get it |
| `start`, during a tombstone: `RunExists` | 1 | 1 | 0 | 1 | 3 | `start`, get `gone`, delete the `start` it created |
| `send`, `cancel` | 2 | 2 | 0 | 0 | 4 | get `result` and `start`, inbox key, due entry |
| `send`, run ended: `RunEnded` | 0 | 1 | 0 | 0 | 1 | get `result` |
| `send`, no chain: `RunNotFound` | 0 | 3 | 0 | 0 | 3 | get `result`, `start` and `gone` |
| `status`, `result`, `send`, `cancel`, collected: `RunNotFound` | 0 | 3 | 0 | 0 | 3 | as no chain, `gone` found |
| `status`, pending | 0 | 2 | 3 | 0 | 5 | get `result` and `start`, list claims, segments, snapshots |
| `status`, running | 0 | 3 | 1 | 0 | 4 | 2 gets as above, list claims, get the latest |
| `status`, suspended | 0 | 2 | 2 | 0 | 4 | as pending, the segment listing finds one |
| `status`, ended | 0 | 1 | 0 | 0 | 1 | get `result` |
| `result`, ended | 0 | 1 | 0 | 0 | 1 | get `result` |
| `result`, a chain of 2 runs ended | 0 | 2 | 1 | 0 | 3 | get the first run's `result`, list the chain, get the last run's `result` |
| `result`, a poll while not ended | 0 | 1 | 0 | 0 | 1 | get the `result` of the run the poll before named; the first poll also gets the `start` |
| `read`, a value | 0 | 1 | 1 | 0 | 2 | list `pub/<chain>/<key>/`, get the last |
| `read`, nothing published | 0 | 0 | 1 | 0 | 1 | list `pub/<chain>/<key>/` |

- **Chains.** `send`, `cancel`, `status` and `result` follow a chain from its first run's `result`: a chain that never continued costs no listing. One that continued lists `index/chains/<chain>/`, one listing per 1,000 runs, and gets the `result` of the last run it finds; the rows above are for a chain of one run.
- **`result` polls.** The first poll follows the chain; each later one gets one key, the `result` of the run the poll before answered. The interval doubles from 10 ms up to 30 s, in every host: polls at 0, 10, 30, 70, 150, 310, 630, 1,270, 2,550, 5,110, 10,230, 20,470 and 40,950 ms, then every 30 s, `13 + floor((T - 40.95) / 30)` polls in `T` seconds past 41 s (measured: 10 in 6 s, 11 GET). Every 30 minutes a poll follows the chain again from its first run, 1 GET more, so a wait on a chain that gets collected ends. **Waiting an hour costs about 134 GET, $0.000054 on S3; a day about 2,940 GET, $0.0012.** On a store whose listings wait, `result` does not poll (see [Stores whose listings wait](#stores-whose-listings-wait)).
- **`read`** lists every value ever published under the key by the chain, kept as long as the chain: one listing per 1,000 values, then one get.
- **A client in another process** than the worker is seen at the worker's next listing, within `max_idle`; in the same process it wakes the worker at once, without a request.

## Workers

### Scans

A scan lists the first page of `due/` of every partition of every workflow the worker serves: `W x P` LIST for `W` workflows and `P` partitions, sent together, one round trip. Measured: 2 workflows on 3 partitions with `max_idle` 200 ms, 10 scans of 6 listings in 2 s.

A worker scans:

| When | Scans |
|---|---|
| idle | one every `max_idle` |
| a due entry it saw comes due (a timer, a retry) | one at that time |
| a replay ends | only if a scan saw an entry of the run the replay did not serve, the worker was full on a store that waits, or the replay created an entry due now (a child, a message, a continuation); otherwise the timer comes forward to the entry its suspension created |
| a start or send in its own process | one at once |

A partition is listed past a full page (`due_page` keys) of entries all due that the worker cannot take (runs held by other workers, versions it does not serve), in rounds of up to `due_pages` pages, to the end of the partition or its first entry not yet due. A page is walked past only when all its entries were due when it was listed. With `S` skipped entries at the head of a partition, a scan of it costs about `S / due_page` listings, and each skipped run is read again (its `start`, and its claims if held) about once per `claim_ttl`. Runs of a version no worker serves cause this: give every version with runs left a worker.

Measured per wakeup with nothing else going on: 1 scan, the one that finds it; a wakeup that starts a child or sends a message adds one. On a busy worker scans are shared between runs, so this is an upper bound.

### One wakeup

Every wakeup is built from these blocks, one operation at a time:

| Block | PUT | GET | LIST | DELETE | Operations |
|---|---|---|---|---|---|
| open | 1 | `G + H + M` | 1 | 0 | get `start` unless the worker holds it (`G`, 0 or 1); list `runs/<run>/`, which shows the segments, claims, effects markers, inbox and `result`; create the claim; get the snapshot and segments it does not hold (`H`); get each new message (`M`) |
| progress commit | 1 | 0 | 1 | C | `log/<n>`, list the inbox; delete the `C` messages it consumed |
| suspension commit | 1 + T | 0 | 0 | 2 + C + S | due entry at the next timer if any (`T`), `log/<n>`, delete the due entry it was woken by, its claim, and a timer entry the run no longer needs (`S`, see `wait_until`) |
| end commit | 3 | 0 | 0 | 2 + C + S | `log/<n>`, the collection entry (none with `forever`), `result`, delete due entry and claim |
| nothing new | 0 or 1 | 1 | 0 | 2 | get `log/<n>` to check no one committed meanwhile, due entry at the next timer, delete due entry and claim |
| renewal | 1 | 0 | 0 | 1 | a new claim attempt every `claim_ttl / 2` while the replay runs, deleted with the others at the commit |
| snapshot | 1 | 0 | 0 | 0 | `snap/<n>` every `snapshot_every` segments |
| effects marker | 1 | 0 | 0 | 0 | `effects/<n>` after a segment with effects that does not end the run; the next open sees it in its listing |

- `H` is the number of history objects the worker does not hold: 0 when it served the run's last wakeup and still holds it (16 MiB of histories per worker, the run used longest ago leaving first); otherwise 1 for the latest snapshot if any, plus the segments after it, at most `snapshot_every - 1`. For a run with `S` segments and `snapshot_every` 64, `H = (S >= 64 ? 1 : 0) + S mod 64`.
- With `commit_every` 0, each step is committed before the next step starts or the run waits on steps: a progress commit, 2 operations. The step before a suspension or the end lands in that commit. Measured: a run of 0 or 1 step costs 8 operations, each further step 2.
- A run whose first replay records nothing (it waits on a message at once) commits an empty segment, `log/0`, without a due entry: its first wakeup is 6 operations, the open, `log/0`, and the deletes of its due entry and claim, and `status` reads it `suspended`. The segment counts toward `continue_as_new_after` and is deleted at the collection like any other.
- A wakeup of a run that already ended (a leftover due entry) sees its `result` in the listing and only deletes the due entry; if the listing shows inbox keys, it reads the history first, to forward them if the run continued. Measured: 5 operations for 2 segments and a message.
- A wakeup of a collected id (a due entry created late, under `gone`) gets the `start`, missing, lists the run and deletes its entry: 3 operations, measured.

## Workflow verbs

What each call adds, at `commit_every` 0, on top of the open and the commit of its wakeup.

| Verb | PUT | GET | LIST | DELETE | Notes |
|---|---|---|---|---|---|
| step | 1 | 0 | 1 | 0 | a progress commit as the next step starts; none for the step before a suspension or the end |
| step, `commit_every` > 0 | 0 | 0 | 0 | 0 | steps within the span share a commit; measured 3 steps at `1h`: 7 operations, as 0 steps |
| step retry | 1 | 0 | 0 | 0 | the failed attempt and its backoff land in one suspension commit, its due entry at the backoff; then a wakeup at its time |
| step running long | `t / (claim_ttl / 2)` | 0 | 0 | as many | renewals; measured 4 to 6 in 1.2 s at `claim_ttl` 400 ms |
| `now`, `random`, `uuid` | 0 | 0 | 0 | 0 | recorded with the next commit; measured |
| `sleep`, `sleep_until` | 1 | 0 | 0 | 0 | the due entry of the suspension; then a scan and a wakeup at its time |
| `receive`, `wait_until` without timeout | 0 | 0 | 0 | 0 | no due entry; the message brings one |
| a message consumed | 0 | 1 | 0 | 1 | its get at open, the delete of its inbox key; its due entry wakes the run and is deleted as any; plus the sender's operations |
| `handle` | 0 | 0 | 0 | 0 | messages as above |
| `wait_until` with timeout | 1 | 0 | 0 | 1 | the timer's due entry; when a message wins, the commit after it deletes the entry |
| `start` of a child | 3 + 1 | 1 | 0 | 0 | child `start`, get its `gone`, `index/children`, due entry, effects marker |
| child `result` | 2 | 1 + 1 | 0 | 1 | the child's end: get the parent's `start`, create its inbox key and due entry; the parent's wakeup reads the message and deletes its key |
| `send` from a workflow | 2 + 1 | 2 | 0 | 0 | before the commit, get the chain's `result` and `start`, as a client's `send` follows the chain; after it, the inbox key and due entry, the marker if the run goes on. To a chain that ended or does not exist, `RunEnded` (1 GET) or `RunNotFound` (3 GET) at the call, and nothing created |
| `publish` | 1 | 0 | 0 | 0 | `pub/<chain>/<key>/...`, plus the marker if the run goes on |
| `iterate` | 0 | 0 | 0 | 0 | no operation per iteration; a continuation when the history passes `continue_as_new_after` |
| `continue_as_new` | 3 | 0 | 0 | 0 | the next run's `start`, its `index/chains` entry, due entry, on top of the end commit and its collection entry; then the next run's first wakeup |
| `cancel`, from inside or a client | 2 per running child | 1 or 2 per child | 1 per run of the chain | 0 | a cancelled end lists the children of each run of its chain (and the chain itself past its first run), gets each child's `result`, and sends each running child `substantial.cancel`: get its `start`, create its inbox key and due entry |
| `rollback`, saga, undo | 0 | 0 | 0 | 0 | an undo is a step; the undos of a rollback ride in its end commit |

## Measured runs

Each line is one wakeup, a worker serving 1 workflow on 1 partition.

| Run | Wakeup | PUT | GET | LIST | DELETE | Ops, round trips |
|---|---|---|---|---|---|---|
| 0 or 1 step | start to end | 4 | 1 | 1 | 2 | 8 |
| 3 steps | start to end | 6 | 1 | 3 | 2 | 12 |
| 3 steps, sleep, end | start: 3 steps, suspend | 5 | 1 | 3 | 2 | 11 |
| | timer: end | 4 | 0 | 1 | 2 | 7 |
| receive, 2 steps, end | start: an empty segment | 2 | 1 | 1 | 2 | 6 |
| | message: 2 steps, end | 5 | 1 | 2 | 3 | 11 |
| `wait_until` with timeout | start: suspend | 3 | 1 | 1 | 2 | 7 |
| | message: end, the timer's entry deleted | 4 | 1 | 1 | 4 | 10 |
| parent with a child | parent: start the child, suspend | 6 | 2 | 1 | 2 | 11 |
| | child: end, tell the parent | 6 | 2 | 1 | 2 | 11 |
| | parent: read the child's end, end | 4 | 1 | 1 | 3 | 9 |
| a run sends to a waiting one | sender: send, end | 6 | 3 | 1 | 2 | 12 |
| | target: message, end | 4 | 1 | 1 | 3 | 9 |
| publish, end | start to end | 5 | 1 | 1 | 2 | 9 |
| `continue_as_new`, then end | first run: continue | 7 | 1 | 1 | 2 | 11 |
| | next run: end | 4 | 1 | 1 | 2 | 8 |
| a step failing once, retried | start: attempt, backoff | 3 | 1 | 1 | 2 | 7 |
| | retry: end | 4 | 0 | 1 | 2 | 7 |
| a step, then `rollback` | start to failed | 5 | 1 | 2 | 2 | 10 |
| cancel a parent with a running child | parent: cancelled, cancel the child | 6 | 3 | 2 | 3 | 14 |
| | child: cancelled, tell the parent | 6 | 2 | 2 | 3 | 13 |
| | parent, ended: delete hints | 0 | 3 | 1 | 1 | 5 |

A wakeup on the worker that served the run's last one gets nothing it holds: a GET of `start` appears only at a run's first wakeup there. A polling loop, one step then a sleep per iteration, one segment per iteration: the wakeup of iteration `k` costs PUT 3, LIST 1, DELETE 2, 6 round trips, plus a PUT when it writes a snapshot; on a worker that does not hold the run, also GET `1 + H(k)`. When the history reaches `continue_as_new_after` segments, the next wakeup continues instead: PUT 7, LIST 1, DELETE 2. The test runs it with `snapshot_every` 4 and a threshold of 8 segments, and checks every wakeup against this formula.

## Collection

A collection is one task per due entry tagged `c` or `g`, found by a scan like any wakeup, with no claim: every operation is a delete, or a create of a key a second collector would create the same, and the served entry is deleted last, so a crash leaves it for the next scan. Measured on runs with no published value, no child and no message left, at a hidden `retention` under a second:

| Collection | PUT | GET | LIST | DELETE | Ops, round trips | Operations |
|---|---|---|---|---|---|---|
| a chain of one run, 1 segment | 2 | 3 | 2 | 4 | 11 | list the run, get `start`, the last segment and `result` (its age), create `gone`, list `pub/<run>/`, delete the history, `start` and `result`, create the `g` entry, delete the served one |
| the first run of a chain that continued, retired, 2 segments | 0 | 4 | 3 | 3 | 10 | list the run, get `start`, the last segment, `result` and the next run's `start` (the messages it carried), list `index/chains/<chain>/` and `pub/<chain>/`, delete the history and the entry; its `start` and `result` stay |
| a continued run of a live chain, 2 segments | 0 | 5 | 2 | 6 | 13 | list the run, get `start`, the last segment, `result`, the previous run's `result` and the next run's `start`, list `pub/<chain>/`; delete its `index/chains` entry, the values it published that later ones replaced, the history, `start`, `result`, the entry |
| the last run at the chain's end, 2 segments | 2 | 6 | 3 | 6 | 17 | list the run, get `start`, the last segment, `result`, the previous run's `result` and `start`, list `index/chains/<chain>/` and the first run's `index/children/`, get the first run's `start`, create its `gone` and its `c` entry due now; delete its `index/chains` entry, the history, `start`, `result`, the entry |
| the first run's delete phase, under `gone` | 1 | 2 | 2 | 3 | 8 | list the run, get `gone` and `start`, list `pub/<chain>/`, delete what is left, `start` and `result`, create the `g` entry, delete the served one |
| the tombstone's end | 0 | 1 | 1 | 2 | 4 | list the run, get `gone`, delete it and the entry |
| a chain of one run, its `result` younger than twice `retention_min` | 1 | 3 | 1 | 1 | 6 | list the run, get `start`, the last segment and `result`, create the `c` entry again at the result's `written_at` plus twice `retention_min`, delete the served one |

- **More keys.** Each segment, snapshot, effects marker, consumed message left, claim and published value adds a DELETE, free on S3; a run or a value prefix past 1,000 keys adds a LIST per 1,000. A run that started children adds a LIST of `index/children/<run>/`; a child's first run adds 1 to 3 GET, its parent's progress.
- **Deferral.** With a `retention` of at most 2 hours, the `result` may be younger than twice `retention_min` when the entry comes due, as it is written just after the end fixes the entry's time: the collection is deferred to the instant the `result` is that old, so a `retention` of 1 hour collects at about 2 hours, for the row above and a scan more. A check that fails, a child still kept, a parent still starting it or an earlier run of the chain not yet collected, creates the entry again `recheck` (1 day) later and deletes the served one: PUT 1, DELETE 1, plus the reads up to the check. A run kept by a child is checked again every day until the child is collected.
- **Per run.** A chain of one run costs a collection and a tombstone end, each found by a scan: PUT 2, GET 4, LIST 3 and 2 scan listings, so 7 PUT-class requests and 4 GET, $0.000037 on S3, $0.37 per 10,000 runs. With `forever`, none of it, and the end commit creates no entry.

## When something goes wrong

Every create is exclusive, so doing an operation twice does it once. What a failure costs:

| Where | What happens | Guarantee |
|---|---|---|
| any operation fails | the adapter retries it, `store_retry`: 5 attempts, backoff from 100 ms to 5 s; S3 requests time out after 5 s to connect and 60 s per attempt | I-26 |
| a create with no answer (timeout, 5xx, lost connection) | the adapter gets the key: the same bytes mean created, other bytes exist, missing means create again; 1 more GET per ambiguous attempt | I-26 |
| an operation still failing in a worker | the core reissues it up to 5 times (`OP_RETRIES`), each with the adapter's retries, then drops the replay and deletes its claims; the due entry stays, the run is tried again after `claim_ttl` | I-20 `StoreError` |
| an operation still failing in a client | `StoreError`; a `start` or `send` retried with the same id creates once | I-17, I-9 |
| a client crashes after `start` before its due entry | the run waits; starting it again with the same id completes it (the 3-operation row) | I-17 |
| a client crashes after a message's inbox key, before its due entry | the message waits for the run's next wakeup, which reads the whole inbox; resent with the same id, it is delivered once | I-9 |
| a client send races the run's end | accepted, never consumed | I-17 |
| a worker crashes mid-replay | its claim expires; the due entries that started the replay are still there, so another worker takes the run over within `claim_ttl + claim_skew` (35 s) of the last renewal: open, get the stale claim, create the next attempt; steps not committed run again | I-1, I-18 |
| a worker finds a live claim | open up to the claim (GET `start`, LIST the run, GET claim) and leave the run until the claim expires | I-18 |
| a worker crashes after `log/<n>`, before the effects | the next wakeup's listing shows `effects/<n>` missing: it creates the effects again, then the marker; due entries were not deleted yet, so that wakeup is at once | I-9, I-11 |
| a worker crashes after the effects, before its deletes | leftover due entries and claims: one wakeup that finds nothing new (6 operations and up) and deletes them | I-26 |
| two workers commit the same segment | one create of `log/<n>` wins; the other deletes its claims, reads the run again, creates the winner's effects if missing, and replays; its steps ran once more than recorded | I-1, I-2 |
| a step hangs | renewals keep the claim, 1 PUT and 1 DELETE every 15 s, until its attempt times out: the step's `timeout`, else `step_timeout`, 5 min by default | I-18 |
| a worker crashes mid-collection | the served entry is deleted last: the next scan collects again, finding fewer keys; a first run's `gone` holds its `until`, so the tombstone keeps its time | I-16 |
| shutdown | no new replay; steps in flight may commit within `shutdown_grace`; runs left keep their due entries | I-18 |

## Settings that drive cost

Values from the presets (I-18):

| Setting | `script` | `service` | Cost |
|---|---|---|---|
| `max_idle` | 1 s | 30 s; 5 min on a waiting store | idle: `W x P` LIST per `max_idle` per worker; latency of work from another process |
| partitions | at `create` | at `create` | idle LIST scale with `P`, and so does every scan |
| `due_page` | 100 | 100 | keys per scan listing; more due runs at once than this per partition need more scans |
| `due_pages` | 10 | 10 | pages listed past entries the worker skips |
| `claim_ttl` | 30 s | 30 s | a renewal, 1 PUT and 1 DELETE, every 15 s of a replay; takeover after `claim_ttl + claim_skew` |
| `claim_skew` | 5 s | 5 s | added to takeover time |
| `commit_every` | 0 s | 0 s | 0: a progress commit (2 operations) as each further step starts; more: steps of the span share one, more rerun after a crash |
| `snapshot_every` | 64 | 64 | bounds history reads to 64 GET per wakeup; each snapshot repeats the history (see [Storage](#storage)) |
| `continue_as_new_after` | 1,000 segments or 8 MiB | 1,000 segments or 8 MiB | how often a loop continues: one extra wakeup and 3 PUT per continuation |
| `wait` | 20 s | 20 s | on `h3://`: one waiting listing per partition per `wait` |
| `store_retry` | 5 attempts, 100 ms to 5 s | same | requests per failing operation |
| `retention` | 30 days | 30 days | a collection and a tombstone end per chain, see [Collection](#collection); `forever` saves them and the end commit's PUT, and keeps every run's storage |

## Monthly cost on S3

**Prices.** AWS S3 Standard, US East (N. Virginia), read on 2026-11-01 from the [S3 pricing page](https://aws.amazon.com/s3/pricing/) and the price list it is built from:

| Item | Price |
|---|---|
| PUT, COPY, POST, LIST requests | $0.005 per 1,000 |
| GET and all other requests | $0.0004 per 1,000 |
| DELETE requests | free |
| storage, first 50 TB | $0.023 per GB-month |

A month is 30 days, 2,592,000 s. Requests and storage only. The examples use 1 workflow on 1 partition and one worker, which holds the histories of the runs it serves, unless said, and count the 1 scan per wakeup measured on a lone run, an upper bound.

### An idle worker fleet

`N` workers, `W` workflows, `P` partitions, `service` preset (`max_idle` 30 s):

`N x W x P x 86,400` LIST a month, **$0.432 per worker, workflow and partition**.

| Fleet | LIST a month | Cost a month |
|---|---|---|
| 1 worker, 1 workflow, 1 partition | 86,400 | $0.43 |
| 2 workers, 1 workflow, 4 partitions | 691,200 | $3.46 |
| 3 workers, 2 workflows, 16 partitions | 8,294,400 | $41.47 |
| 1 worker, 1 workflow, 1 partition, `script` (`max_idle` 1 s) | 2,592,000 | $12.96 |

This is paid whatever runs; the examples below add to it.

### A simple workflow

Start, 3 steps, a 1-day sleep, complete; 10,000 runs a month, each collected 30 days after its end; the client starts and does not wait for the result.

| Per run | PUT | GET | LIST | DELETE |
|---|---|---|---|---|
| client `start` | 2 | 1 | 0 | 0 |
| wakeup 1: 3 steps, suspend | 5 | 1 | 3 | 2 |
| wakeup 2: timer, end | 4 | 0 | 1 | 2 |
| collection of its 4 segments | 2 | 3 | 2 | 7 |
| tombstone end | 0 | 1 | 1 | 2 |
| scans, 4 | 0 | 0 | 4 | 0 |
| **total** | **13** | **6** | **11** | **13** |

10,000 runs: 240,000 PUT and LIST, $1.20; 60,000 GET, $0.024. **$1.22 a month**, plus the fleet. With `forever`, 10 PUT, 2 GET and 6 LIST a run: $0.81 a month. If another worker serves the second wakeup, it gets the `start` and 3 segments: 4 GET more per run, $0.016 a month. A client waiting on `result()` through the day would add about $0.0012 per run, $12 a month.

### A polling cron

A loop calling an API every 5 minutes in a step, with `iterate`, one chain all month: 8,640 iterations. Each iteration is one wakeup and one segment; the chain continues every 1,000 segments, about 8.6 times a month.

Per 1,000 iterations (one run of the chain), from the formula the test checks:

| | PUT | GET | LIST | DELETE |
|---|---|---|---|---|
| 1,000 iteration wakeups: 3 PUT, 1 LIST, 2 DELETE | 3,000 | 0 | 1,000 | 2,000 |
| the run's `start`, at its first wakeup | 0 | 1 | 0 | 0 |
| 15 snapshots | 15 | 0 | 0 | 0 |
| the continuation wakeup | 7 | 0 | 1 | 2 |
| the run's collection, 30 days later: its 1,017 keys listed in 2 pages, its values | 0 | 5 | 3 | 1,019 |
| scans, 1 per wakeup and collection | 0 | 0 | 1,002 | 0 |
| **total** | **3,022** | **6** | **2,006** | **3,021** |

A month: 43,442 PUT and LIST, $0.22; 52 GET. **$0.22 a month**, plus the fleet. On a worker that does not hold the history (several workers serving the chain, or a restart), each wakeup gets the `start` and `H(k)` history objects, each wakeup reading 32 on average: about 34,000 GET more per 1,000 iterations, $0.12 a month.

### A message-driven order

Start, wait for a payment message, 2 steps, done; 100,000 orders a month, each collected 30 days after its end.

| Per order | PUT | GET | LIST | DELETE |
|---|---|---|---|---|
| client `start` | 2 | 1 | 0 | 0 |
| wakeup 1: an empty segment | 2 | 1 | 1 | 2 |
| client `send` of the payment | 2 | 2 | 0 | 0 |
| wakeup 2: payment, 2 steps, end | 5 | 1 | 2 | 3 |
| collection of its 3 segments | 2 | 3 | 2 | 6 |
| tombstone end | 0 | 1 | 1 | 2 |
| scans, 4 | 0 | 0 | 4 | 0 |
| **total** | **13** | **9** | **10** | **13** |

100,000 orders: 2,300,000 PUT and LIST, $11.50; 900,000 GET, $0.36. **$11.86 a month**, plus the fleet. With `forever`, 10 PUT, 5 GET and 5 LIST an order: $7.70 a month.

## Storage

A run's keys are deleted at its collection, its `retention` after its chain ends, 30 days by default (I-16); a collected chain leaves `gone`, 9 bytes, for 7 more days. With `forever`, every key below stays. Never delete keys under a namespace, by hand or with an S3 lifecycle rule: runs still going read keys of runs that ended (I-16). Hints (due entries, claims, consumed inbox keys) are deleted as they are used; the probe is one empty key per namespace. Values are stored inline in the history (I-15), so their size adds directly.

Measured with small values (an integer per step, an empty input):

| Run | Keys until collected | Bytes |
|---|---|---|
| 3 steps, sleep, end | 6: `start`, 4 segments, `result` | 263 |
| receive, 2 steps, end | 5: `start`, 3 segments, `result` | 355 |
| polling loop | about 75 bytes per segment, 40 per segment repeated in a snapshot | |

- A value is stored in the segment that records it, once more in every later snapshot of the run, and a run's input in its `start`. A result is in the last segment and in `result`.
- **Snapshots repeat the history.** Snapshot `n` holds every event of segments 0 to `n`. In a run of 1,000 segments with `snapshot_every` 64, the 15 snapshots hold 7,680 segments' worth: each value is stored about 8.7 times. `continue_as_new_after` bounds this per run.
- The examples above, at S3 Standard: 10,000 simple runs add 2.6 MB a month; the cron, about 0.38 MB per 1,000 iterations, 3.3 MB a month, plus `8.7 x V` per poll for a poll value of `V` bytes (1 KiB a poll adds about 73 MiB a month); 100,000 orders add 36 MB a month. Each GB kept costs $0.023 a month, so storage stays below a cent a month for these examples. With the default `retention` a namespace holds about the runs of the last 30 days and stops growing after the first month; with `forever` it grows by that much every month.

## Stores whose listings wait

`h3://` opens the same adapter as `s3://`, with listings that wait (I-16). The code changes when work is seen, not what a run costs:

- **Per run, the same operations.** Measured: a 1-step run on a fake h3 server, 7 operations, as on memory.
- **Idle, more listing requests, each held.** Per worker, workflow and partition: one waiting listing per `wait` (20 s) and one plain listing per `max_idle` (5 min on a waiting store in `service`), 138,240 requests a month against 86,400 on S3 at 30 s. Measured with `wait` 1 s and `max_idle` 3 s on 2 partitions: 12 waiting listings and 4 listings in 6 s.
- **Latency.** A start or message from another process is seen as soon as it lands, instead of within `max_idle`. Measured: with `max_idle` 1 h, a run started by another client ran at once.
- **`result` waits instead of polling.** Each time the run's end is read and it has none, the client lists the `result` key the core names with a wait of 0 s for its etag, then holds a waiting listing of it, sent again every 20 s, until the key exists; then it gets that `result`. A run's end is seen as soon as it is written, instead of up to 30 s later. Every 30 minutes it follows the chain again, 2 GET and a listing for the etag, so a wait on a chain that gets collected ends. An hour waited on a run that does not continue: 5 GET (each follow's 2, the last's 1) and 182 listings, 2 for the etag and 180 held.
- **A connection per held listing.** A worker holds one per workflow and partition it serves, and a client one per `result` waited on. The adapter's connections are not capped in any host, so its other requests never queue behind them.
- An h3 server is self-hosted: its requests are not billed per request by AWS. What reaches its backends depends on the server.

## Redis

A Redis store performs the operations of every table above, each answer waiting for fsyncs on the primary and its replicas: its commands, latency, memory and failovers are in [Redis stores](redis.md).

## Keeping this document true

`rust/tests/costs.rs` runs every row of this document against a store that counts operations by kind and measures round trips from their timing, and asserts the numbers above. A change of the core or of a host that changes a count fails it.

- `cargo test --test costs` checks.
- `COSTS_MEASURE=1 cargo test --test costs -- --nocapture --test-threads 1` prints every measured row without asserting, to update this document and the test together.
- `COSTS_TRACE=1` adds each worker operation with its key, to explain a row.
- Time-dependent counts (idle scans, `result` polls, renewals, waiting listings) are measured over a controlled window and checked within bounds; their formulas are above. The monthly examples are arithmetic on the measured rows and the prices above.
