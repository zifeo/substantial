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
- [What a run records of its cost](#what-a-run-records-of-its-cost)
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
  | `substantial.json` | the layout marker: the layout, 2, and the encoding |
  | `runs/<run>/start` | the workflow, version and input of a run |
  | `runs/<run>/log/<n>` | segment `n` of its history |
  | `runs/<run>/snap/<n>` | a snapshot of segments 0 to `n` |
  | `runs/<run>/effects/<n>` | empty: the effects of segment `n` exist |
  | `runs/<run>/inbox/<ord>-<id>` | a message not yet consumed, `ord` the sender's ULID |
  | `runs/<run>/claim/<n>-<attempt>` | a worker's claim on writing segment `n` |
  | `runs/<run>/result` | the outcome, and `written_at`, when it was written |
  | `runs/<run>/watch/<watcher>` | empty: a run awaiting the chain's end with `c.result`, told at the end |
  | `runs/<run>/gone` | the tombstone of a collected chain: until when its id is taken |
  | `pub/<chain>/<key>/...` | a published value |
  | `queues/<workflow>/due/<time>~<run>~<tag>` | empty: a wakeup; tagged `c`, a collection; `g`, a tombstone's end |
  | `queues/<workflow>/rest/<run>` | empty: the run waits on messages alone, for `runs`; no worker lists it |
  | `index/children/<parent>/<child>` | empty: the children of a run |
  | `index/chains/<chain>/<seq>-<run>` | empty: the runs a chain continued into; its first run is `runs/<chain>/` |
  | `probe/exclusive` | empty: the probe of `create` and `open`, kept |

- **The commit point is an exclusive create.** A replay records what happened in a segment and commits it by creating `log/<n>`. Of two workers racing on a run, the first create wins and the other drops its work. Everything else is a hint that can be rebuilt from the histories: due entries, rest markers, claims, indexes.
- **Segments.** A wakeup lists the run's keys once, reads its `start`, its latest snapshot, the segments after it and its new messages, replays the code against them, then commits a new segment. A snapshot is written every `snapshot_every` segments, so a wakeup reads at most one snapshot and 63 segments. A worker keeps what it read and committed of a run, up to 16 MiB in all: its next wakeup of the run reads only what is new.
- **Due entries.** A wakeup is an empty key sorting by time under its workflow. A worker lists the first page of each workflow it serves, takes the entries due, and sleeps until the earliest wakeup it saw, a timer, a retry or a message, or `max_idle`, whichever comes first; after its first scan, the worker's phase, drawn below `max_idle`, stands for `max_idle`.
- **Rest markers.** A run that waits on messages alone has no due entry: its first such wait creates `queues/<workflow>/rest/<run>`, which only `runs` lists, and its segment records it, so no later wakeup creates it again. Its end deletes it, free on S3; its collection deletes one a crash left.
- **Claims.** Before a replay a worker creates `claim/<n>-<attempt>`, and a new attempt every half `claim_ttl` while the replay runs; a commit in the middle of a replay keeps it. A claim only avoids duplicate work; the segment decides.
- **Collection.** A run's end creates a due entry tagged `c` at its end plus its `retention`. A worker serving the workflow, any version, that finds it due checks the run can go and deletes its keys. A `result` younger than twice `retention_min` (2 hours) creates the entry again at the instant it is that old; a check that fails, `recheck` (1 day) later. Collecting the first run of a chain creates `runs/<chain>/gone` first, holding until when the id is taken, and an entry tagged `g` at that time, which deletes it. These entries set no timer: the worker's next scan after they are due serves them.
- **Several workers** serving a workflow list the same entries: a claim decides which one replays a run, and the others skip it (see [Several workers](#several-workers)).
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
| `status`, pending | 0 | 2 | 1 | 0 | 3 | get `result` and `start`, list `runs/<run>/` |
| `status`, running | 0 | 3 | 1 | 0 | 4 | as pending, then get the latest claim the listing shows |
| `status`, suspended | 0 | 2 | 1 | 0 | 3 | as pending, the listing shows a segment |
| `status`, ended | 0 | 1 | 0 | 0 | 1 | get `result` |
| `status`, `result`, failed | 0 | 1 | 1 | 0 | 2 | get `result`, list `index/chains/<chain>/` for a retry of it |
| `result`, ended | 0 | 1 | 0 | 0 | 1 | get `result` |
| `result`, a chain of 2 runs ended | 0 | 2 | 1 | 0 | 3 | get the first run's `result`, list the chain, get the last run's `result` |
| `result`, a poll while not ended | 0 | 1 | 0 | 0 | 1 | get the `result` of the run the poll before named; the first poll also gets the `start` |
| `read`, a value | 0 | 1 | 1 | 0 | 2 | list `pub/<chain>/<key>/`, get the last |
| `read`, nothing published | 0 | 0 | 1 | 0 | 1 | list `pub/<chain>/<key>/` |
| `runs`, 2 pending | 0 | 2 | 2 | 0 | 4 | list the workflow's `due/` and `rest/`, get each run's `start` |
| `runs`, 2 waiting on messages | 0 | 4 | 2 | 0 | 6 | as pending; a run found by its rest marker alone also gets its `result`, missing |
| `history`, a run of 1 segment | 0 | 3 | 1 | 0 | 4 | get `result` and `start`, list the run, get its segment |
| `history`, a retry | 0 | 3 | 2 | 0 | 5 | get the failed run's `result`, list the chain, get the retry's `result`, list it, get its snapshot |
| `retry` | 3 | 4 | 2 | 0 | 9 | get the failed run's `result`, list the chain, list the failed run's segments and get the last, which must hold no undo, get its `start`, create the retry's, get the chain's `gone`, create its `index/chains` entry and due entry |
| `retry`, a failure that ran undos: `InvalidArgument` | 0 | 2 | 2 | 0 | 4 | get the failed run's `result`, list the chain, list its segments, get the last, which holds an undo; a rollback's `result` alone refuses it, 1 GET and 1 LIST |

- **Chains.** `send`, `cancel`, `status`, `result`, `history` and `retry` follow a chain from its first run's `result`: a chain that never continued and did not fail costs no listing; a failed run's `result` adds a listing of `index/chains/<chain>/`, which finds its retry if any. One that continued lists `index/chains/<chain>/`, one listing per 1,000 runs, and gets the `result` of the last run it finds; the rows above are for a chain of one run.
- **`result` polls.** The first poll follows the chain; each later one gets one key, the `result` of the run the poll before answered. The interval doubles from 10 ms up to 30 s, in every host: polls at 0, 10, 30, 70, 150, 310, 630, 1,270, 2,550, 5,110, 10,230, 20,470 and 40,950 ms, then every 30 s, `13 + floor((T - 40.95) / 30)` polls in `T` seconds past 41 s (measured: 10 in 6 s, 11 GET). Every 30 minutes a poll follows the chain again from its first run, 1 GET more, so a wait on a chain that gets collected ends. **Waiting an hour costs about 134 GET, $0.000054 on S3; a day about 2,940 GET, $0.0012.** On a store whose listings wait, `result` does not poll (see [Stores whose listings wait](#stores-whose-listings-wait)).
- **`result(max_interval=)`** caps the interval at `max_interval` instead of 30 s: a longer cap costs fewer GET on a long wait, and sees the end up to `max_interval` after it is written. With the follow every 30 minutes and the first poll's GET of `start`:

  | `max_interval` | GET in an hour | GET in a day | A day on S3 |
  |---|---|---|---|
  | 30 s, the default | 134 | 2,940 | $0.0012 |
  | 2 min | 46 | 782 | $0.00031 |
  | 5 min | 29 | 351 | $0.00014 |
  | 30 min | 22 | 114 | $0.000046 |

- **`status`** lists the run once, one listing per 1,000 of its keys (a long inbox adds some).
- **`runs`** lists the workflow's `due/` and `rest/` prefixes, one listing per 1,000 keys each, and gets the `start` of each run once; a run waiting on messages alone, found by its marker, gets its `result` first. Its pages come as the iteration goes, so stopping early costs less.
- **`history`** reads what a wakeup of the last run reads: its latest snapshot and the segments after it, at most 63 with `snapshot_every` 64, plus 1 GET of its `start` when it has no snapshot; a retry without a snapshot of its own reads the failed run too.
- **`read`** lists every value ever published under the key by the chain, kept as long as the chain: one listing per 1,000 values, then one get.
- **A client in another process** than the worker is seen at the worker's next listing, within `max_idle`; in the same process it wakes the workers serving the run's workflow at once, without a request, and they list that workflow alone.

## Workers

### Scans

A scan lists the first page of `due/` of every workflow the worker serves: `W` LIST for `W` workflows, sent together, one round trip. Measured: 2 workflows with `max_idle` 200 ms, 10 scans of 2 listings in 2 s.

A worker scans:

| When | Scans |
|---|---|
| idle | one every `max_idle`, the first after the worker's phase, drawn once below `max_idle` from its seed: workers started together list at times of their own |
| a wakeup it saw comes due (a timer, a retry) | one at that time; a collection entry sets no timer, the next scan serves it |
| a replay ends | only if a scan saw an entry of the run the replay did not serve, the worker was full on a store that waits, or the replay created an entry due now (a child, a message, a continuation); otherwise the timer comes forward to the entry its suspension created |
| it skipped a run held by another worker's live claim | one more at once, once it let the run go |
| a start or send in its own process | a listing of the run's workflow alone, at once |

A workflow's entries are listed past a full page (`due_page` keys) of entries all due that the worker cannot take (runs held by other workers, versions it does not serve), in rounds of up to `due_pages` pages, to their end or their first entry not yet due. A page is walked past only when all its entries were due when it was listed. With `S` skipped entries at the head of a workflow's entries, a scan of it costs about `S / due_page` listings, and each skipped run is read again (its `start`, and its claims if held) about once per `claim_ttl`. Runs of a version no worker serves cause this: give every version with runs left a worker.

Measured per wakeup with nothing else going on: 1 scan, the one that finds it; a wakeup that starts a child or sends a message adds one. On a busy worker scans are shared between runs, so this is an upper bound.

### Several workers

Workers serving a workflow share its due entries: nothing splits them ahead, each lists all of them, and a claim decides which worker replays a run. What each worker adds:

- **Idle.** Its own scans, `W` LIST per `max_idle`: the fleet's idle cost grows with workers times workflows ([An idle worker fleet](#an-idle-worker-fleet)).
- **A timer.** Every worker that listed a run's timer scans when it comes due. One replays the run; each other lists the run, finds its claim live (it gets the claim, or its own create of it answers that it exists) and skips the run, then, once it let it go, scans again at once. Measured with 3 workers serving 1 workflow, at a timer: the one that replayed the run, PUT 4, LIST 2, DELETE 2 and 1 scan, as alone; each of the two others, PUT 0 or 1, GET 1 or 2, LIST 1, 3 operations, and 2 scans. Per timer, each worker more costs 2 GET, or a GET and a PUT, and `1 + 2W` LIST.
- **A start or a message from another process.** The first worker whose scan comes after it takes it; the others' scans no longer list its entry once that replay committed, a few round trips later, so they rarely meet its claim.
- **A collection.** The first worker whose scan comes after the entry is due collects the run; the others' scans find nothing. Measured with 3 workers: one collection, PUT 2, GET 3, LIST 1, DELETE 4, and one scan of each worker. Two scans within the few round trips of a collection both collect: duplicate work, harmless.

**Fewer, larger workers.** Raise `concurrency` before adding workers: a worker more serves no more runs on its own, and adds its idle listings for every workflow it serves and, at every timer of the workflows it serves, a scan, a read of the run and a scan again.

### One wakeup

Every wakeup is built from these blocks, one operation at a time:

| Block | PUT | GET | LIST | DELETE | Operations |
|---|---|---|---|---|---|
| open | 1 | `G + H + M` | 1 | 0 | get `start` unless the worker holds it (`G`, 0 or 1); list `runs/<run>/`, which shows the segments, claims, effects markers, inbox and `result`; create the claim; get the snapshot and segments it does not hold (`H`); get each new message (`M`) |
| progress commit | 1 | 0 | 1 | C | `log/<n>`, list the inbox; delete the `C` messages it consumed |
| suspension commit | 1 + T | 0 | 0 | 2 + C + S | due entry at the next timer if any (`T`), or the run's rest marker at its first wait on messages alone, `log/<n>`, delete the due entry it was woken by, its claim, and a timer entry the run no longer needs (`S`, see `wait_until`) |
| end commit | 3 + 2 V | V | 1 | 2 + C + S + R | `log/<n>`, the collection entry, delete the rest marker of a run that waited on messages alone (`R`, 0 or 1), `result`, list `runs/<run>/watch/` and tell each of the `V` runs awaiting the chain (get its `start`, create its inbox key and due entry), delete due entry and claim; a `continue_as_new` end carries each watcher instead (create its key under the next run, get that run's `result`) |
| nothing new | 0 or 1 | 1 | 0 | 2 | get `log/<n>` to check no one committed meanwhile, due entry at the next timer, delete due entry and claim |
| renewal | 1 | 0 | 0 | 1 | a new claim attempt every `claim_ttl / 2` while the replay runs, deleted with the others at the commit |
| snapshot | 1 | 0 | 0 | 0 | `snap/<n>` every `snapshot_every` segments |
| effects marker | 1 | 0 | 0 | 0 | `effects/<n>` after a segment with effects that does not end the run; the next open sees it in its listing |

- `H` is the number of history objects the worker does not hold: 0 when it served the run's last wakeup and still holds it (16 MiB of histories per worker, the run used longest ago leaving first); otherwise 1 for the latest snapshot if any, plus the segments after it, at most `snapshot_every - 1`. For a run with `S` segments and `snapshot_every` 64, `H = (S >= 64 ? 1 : 0) + S mod 64`.
- With `commit_every` 0, each step is committed before the next step starts or the run waits on steps: a progress commit, 2 operations. The step before a suspension or the end lands in that commit. Measured: a run of 0 or 1 step costs 9 operations, each further step 2.
- A run whose first replay records nothing (it waits on a message at once) commits an empty segment, `log/0`, without a due entry: its first wakeup is 7 operations, the open, its rest marker, `log/0`, and the deletes of its due entry and claim, and `status` reads it `suspended`. The segment counts toward `continue_as_new_after` and is deleted at the collection like any other.
- A wakeup of a run that already ended (a leftover due entry) sees its `result` in the listing and only deletes the due entry; if the listing shows inbox keys, it reads the history first, to forward them if the run continued. Measured: 5 operations for 2 segments and a message.
- A wakeup of a collected id (a due entry created late, under `gone`) gets the `start`, missing, lists the run and deletes its entry: 3 operations, measured.

### A worker run on a schedule

`drain(within=)` (I-18) scans every workflow it serves once when it starts, then only at the wakeups due within its horizon that it listed or created and at the end of a crashed worker's claim, and stops once none is left: no idle listing and no waiting listing. Measured with 1 workflow: nothing due, LIST 1; a run of one step due now, its wakeup (PUT 4, GET 1, LIST 2, DELETE 2) and 1 scan; a run whose sleep ends within the horizon, its two wakeups and 2 scans.

A worker a job starts every `P` seconds, serving `W` workflows, costs `2,592,000 / P x W` LIST a month when nothing is due, against `2,592,000 / max_idle x W` for a worker that runs all the time: every 10 minutes, 4,320 LIST, $0.022 a month per workflow, against $0.43 at a `max_idle` of 30 s. Work due between two jobs waits for the next one: up to `P` of latency.

## Workflow verbs

What each call adds, at `commit_every` 0, on top of the open and the commit of its wakeup.

| Verb | PUT | GET | LIST | DELETE | Notes |
|---|---|---|---|---|---|
| step | 1 | 0 | 1 | 0 | a progress commit as the next step starts; none for the step before a suspension or the end |
| step, `commit_every` > 0 | 0 | 0 | 0 | 0 | steps within the span share a commit; measured 3 steps at `1h`: 9 operations, as 0 steps |
| step retry | 1 | 0 | 0 | 0 | the failed attempt and its backoff land in one suspension commit, its due entry at the backoff; then a wakeup at its time |
| step running long | `t / (claim_ttl / 2)` | 0 | 0 | as many | renewals; measured 4 to 6 in 1.2 s at `claim_ttl` 400 ms |
| `now`, `random`, `uuid` | 0 | 0 | 0 | 0 | recorded with the next commit; measured |
| `sleep`, `sleep_until` | 1 | 0 | 0 | 0 | the due entry of the suspension; then a scan and a wakeup at its time |
| `receive`, `wait_until` without timeout | 0 or 1 | 0 | 0 | 0 or 1 | no due entry, the message brings one; the run's first wait on messages alone creates its rest marker, for `runs`, and its end deletes it: 1 PUT and 1 free DELETE per run that ever waits so, including a parent awaiting a child, and per continuation of an actor |
| a message consumed | 0 | 1 | 0 | 1 | its get at open, the delete of its inbox key; its due entry wakes the run and is deleted as any; plus the sender's operations |
| `handle` | 0 | 0 | 0 | 0 | messages as above |
| `wait_until` with timeout | 1 | 0 | 0 | 1 | the timer's due entry; when a message wins, the commit after it deletes the entry |
| `start` of a child | 3 + 1 | 1 | 0 | 0 | child `start`, get its `gone`, `index/children`, due entry, effects marker |
| child `result` | 2 | 1 + 1 | 0 | 1 | the child's end: get the parent's `start`, create its inbox key and due entry; the parent's wakeup reads the message and deletes its key |
| `send` from a workflow | 2 + 1 | 2 | 0 | 0 | before the commit, get the chain's `result` and `start`, as a client's `send` follows the chain; after it, the inbox key and due entry, the marker if the run goes on. To a chain that ended or does not exist, `RunEnded` (1 GET) or `RunNotFound` (3 GET) at the call, and nothing created |
| `publish` | 1 | 0 | 0 | 0 | `pub/<chain>/<key>/...`, plus the marker if the run goes on |
| `poll`, a try not ready | 1 | 0 | 0 | 0 | a step and a sleep: each try is a wakeup of its own, PUT 3, LIST 1, DELETE 2, measured; the try that is ready ends it as a step does |
| `schedule`, a fire | 4 | 0 | 2 | 0 | the plan's step and the fire's published value, each landing in a progress commit, and the effects marker; measured from the fire's timer to the next suspension, the body running one step: PUT 8, LIST 4, DELETE 2, a wakeup, 3 progress commits (the plan's step, the fire, the body's step), the published value and its effects marker, the next timer's due entry |
| `iterate` | 0 | 0 | 0 | 0 | no operation per iteration; a continuation when the history passes `continue_as_new_after` |
| `continue_as_new` | 3 | 0 | 0 | 0 | the next run's `start`, its `index/chains` entry, due entry, on top of the end commit and its collection entry; then the next run's first wakeup |
| `c.result(chain)` | 1 | 3 | 0 | 0 | before the commit: get the chain's `result` (missing) and `start`, create `runs/<chain>/watch/<run>`, get `result` again, which catches an end written meanwhile; a chain that continued adds LIST 1 and GET 1, to follow it to its last run. A chain that ended answers at once, 1 GET; no chain or a collected one raises `RunNotFound`, 3 GET. The end's message then wakes the run, as a child's does. The same chain again in the run costs nothing |
| `child.cancel` | 2 + 1 | 2 | 0 | 0 | as a `send` from a workflow to the child: before the commit, get the child's `result` and `start`; after it, the `substantial.cancel` inbox key and due entry, the marker if the run goes on. A child that ended raises `RunEnded` at the call, 1 GET. Then the child pays a cancelled end |
| `cancel`, from inside or a client | 2 per running child | 1 or 2 per child | 1 per run of the chain | 0 | a cancelled end lists the children of each run of its chain (and the chain itself past its first run), gets each child's `result`, and sends each running child `substantial.cancel`: get its `start`, create its inbox key and due entry |
| `rollback`, saga, undo | 0 | 0 | 0 | 0 | an undo is a step; the undos of a rollback ride in its end commit |

## Measured runs

Each line is one wakeup, a worker serving 1 workflow.

| Run | Wakeup | PUT | GET | LIST | DELETE | Ops, round trips |
|---|---|---|---|---|---|---|
| 0 or 1 step | start to end | 4 | 1 | 2 | 2 | 9 |
| 3 steps | start to end | 6 | 1 | 4 | 2 | 13 |
| 3 steps, sleep, end | start: 3 steps, suspend | 5 | 1 | 3 | 2 | 11 |
| | timer: end | 4 | 0 | 2 | 2 | 8 |
| receive, 2 steps, end | start: an empty segment, the rest marker | 3 | 1 | 1 | 2 | 7 |
| | message: 2 steps, end | 5 | 1 | 3 | 4 | 13 |
| `wait_until` with timeout | start: suspend | 3 | 1 | 1 | 2 | 7 |
| | message: end, the timer's entry deleted | 4 | 1 | 2 | 4 | 11 |
| parent with a child | parent: start the child, suspend | 7 | 2 | 1 | 2 | 12 |
| | child: end, tell the parent | 6 | 2 | 2 | 2 | 12 |
| | parent: read the child's end, end | 4 | 1 | 2 | 4 | 11 |
| a run sends to a waiting one | sender: send, end | 6 | 3 | 2 | 2 | 13 |
| | target: message, end | 4 | 1 | 2 | 4 | 11 |
| publish, end | start to end | 5 | 1 | 2 | 2 | 10 |
| `continue_as_new`, then end | first run: continue | 7 | 1 | 2 | 2 | 12 |
| | next run: end | 4 | 1 | 2 | 2 | 9 |
| a step failing once, retried | start: attempt, backoff | 3 | 1 | 1 | 2 | 7 |
| | retry: end | 4 | 0 | 2 | 2 | 8 |
| a step, then `rollback` | start to failed | 5 | 1 | 3 | 2 | 11 |
| a step, then another failing for good, retried | the retry's first wakeup: get its `start`, list the failed run, get its `start` and 2 segments, run the failed step, end, its first snapshot | 5 | 4 | 3 | 2 | 14 |
| cancel a parent with a running child | parent: cancelled, cancel the child | 6 | 3 | 3 | 4 | 16 |
| | child: cancelled, tell the parent | 6 | 2 | 3 | 4 | 15 |
| | parent, ended: delete hints | 0 | 3 | 1 | 1 | 5 |
| a parent cancels the child it started | parent: start the child, cancel it, suspend | 9 | 5 | 1 | 2 | 17 |
| | child: cancelled, tell the parent | 6 | 3 | 3 | 4 | 16 |
| | parent: read the child's end, end | 4 | 1 | 2 | 4 | 11 |
| a run awaits a chain with `c.result` | waiter: register, suspend | 4 | 4 | 1 | 2 | 11 |
| | watched run: end, tell 1 watcher | 6 | 2 | 2 | 4 | 14 |
| | watched run: end, tell 2 watchers | 8 | 3 | 2 | 4 | 17 |
| | waiter: read the end, end | 4 | 1 | 2 | 4 | 11 |
| | a chain already ended: register, end | 4 | 2 | 2 | 2 | 10 |
| a watched run continues as new | continue, carry the watcher | 8 | 2 | 2 | 4 | 16 |
| | next run: end, tell the watcher | 6 | 2 | 2 | 4 | 14 |

A wakeup on the worker that served the run's last one gets nothing it holds: a GET of `start` appears only at a run's first wakeup there. A polling loop, one step then a sleep per iteration, one segment per iteration: the wakeup of iteration `k` costs PUT 3, LIST 1, DELETE 2, 6 round trips, plus a PUT when it writes a snapshot; on a worker that does not hold the run, also GET `1 + H(k)`. When the history reaches `continue_as_new_after` segments, the next wakeup continues instead: PUT 7, LIST 2, DELETE 2. The test runs it with `snapshot_every` 4 and a threshold of 8 segments, and checks every wakeup against this formula.

## Collection

A collection is one task per due entry tagged `c` or `g`, found by a scan like any wakeup, with no claim: every operation is a delete, or a create of a key a second collector would create the same, and the served entry is deleted last, so a crash leaves it for the next scan. Measured on runs with no published value, no child and no message left, at a hidden `retention` under a second:

| Collection | PUT | GET | LIST | DELETE | Ops, round trips | Operations |
|---|---|---|---|---|---|---|
| a chain of one run, 1 segment | 2 | 3 | 1 | 5 | 11 | list the run, get `start`, the last segment and `result` (its age), create `gone`, delete its rest marker, the history, `start` and `result`, create the `g` entry, delete the served one |
| the first run of a chain that continued, retired, 2 segments | 0 | 4 | 2 | 3 | 9 | list the run, get `start`, the last segment, `result` and the next run's `start` (the messages it carried), list `index/chains/<chain>/`, delete the history and the entry; its `start` and `result` stay |
| a continued run of a live chain, 2 segments | 0 | 5 | 1 | 7 | 13 | list the run, get `start`, the last segment, `result`, the previous run's `result` and the next run's `start`; delete its `index/chains` entry, its rest marker, the history, `start`, `result`, the entry |
| the last run at the chain's end, 2 segments | 2 | 6 | 3 | 7 | 18 | list the run, get `start`, the last segment, `result`, the previous run's `result` and `start`, list `index/chains/<chain>/` and the first run's `index/children/`, get the first run's `start`, create its `gone` and its `c` entry due now, which the next scan serves; delete its `index/chains` entry, its rest marker, the history, `start`, `result`, the entry |
| the first run's delete phase, under `gone` | 1 | 2 | 2 | 4 | 9 | list the run, get `gone` and `start`, list `pub/<chain>/` (the chain continued), delete its rest marker and what is left, `start` and `result`, create the `g` entry, delete the served one |
| the tombstone's end | 0 | 1 | 1 | 2 | 4 | list the run, get `gone`, delete it and the entry |
| a chain of one run, its `result` younger than twice `retention_min` | 1 | 3 | 1 | 1 | 6 | list the run, get `start`, the last segment and `result`, create the `c` entry again at the result's `written_at` plus twice `retention_min`, delete the served one |

- **When.** A collection entry sets no worker's timer: the first scan after it is due serves it, an idle one at the latest, up to `max_idle` after its time. So does the end of a tombstone, and the entry a chain's end creates for its first run, due at once.
- **Watcher keys.** A collection deletes a run's `watch/` keys with its other keys, 1 DELETE each, free on S3.
- **Rest markers.** A collection that deletes a run deletes its rest marker too, whether the run ever waited on messages alone or not, 1 DELETE, free on S3: its end deleted it already unless a crash or a lost race left one.
- **A failed run** gets the `start` of its retry, 1 GET: none, it is collected as above; one, it is kept until the retry's first snapshot holds its history, then collected as a run that continued, the first run of a chain keeping its `start` and `result`.
- **Published values.** A collection lists `pub/<chain>/` only if its run may have published: an `effects/` marker in its listing, which every segment with an effect leaves but the last, or a `published` event in its last segment. It then deletes the values later runs of the chain replaced, or, for the chain's first run, every value of the chain. The first run's delete phase lists them whenever the chain continued, and when it is resumed after a crash.
- **More keys.** Each segment, snapshot, effects marker, consumed message left, claim and published value adds a DELETE, free on S3; a run or a value prefix past 1,000 keys adds a LIST per 1,000. A run that started children adds a LIST of `index/children/<run>/`; a child's first run adds 1 to 3 GET, its parent's progress.
- **Deferral.** With a `retention` of at most 2 hours, the `result` may be younger than twice `retention_min` when the entry comes due, as it is written just after the end fixes the entry's time: the collection is deferred to the instant the `result` is that old, so a `retention` of 1 hour collects at about 2 hours, for the row above and a scan more. A check that fails, a child still kept, a parent still starting it or an earlier run of the chain not yet collected, creates the entry again `recheck` (1 day) later and deletes the served one: PUT 1, DELETE 1, plus the reads up to the check. A run kept by a child is checked again every day until the child is collected.
- **Per run.** A chain of one run that published nothing costs a collection and a tombstone end, each served by a scan the worker makes anyway: PUT 2, GET 4, LIST 2, so 4 PUT-class requests and 4 GET, $0.000022 on S3, $0.22 per 10,000 runs; one that published lists its values too, LIST 1 more.

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
| `max_idle` | 1 s | 30 s; 5 min on a waiting store | idle: `W` LIST per `max_idle` per worker; latency of work from another process and of collections |
| `due_page` | 1,000 | 1,000 | keys per scan listing; more due runs at once than this per workflow need more scans |
| `due_pages` | 10 | 10 | pages listed past entries the worker skips |
| `claim_ttl` | 30 s | 30 s | a renewal, 1 PUT and 1 DELETE, every 15 s of a replay; takeover after `claim_ttl + claim_skew` |
| `claim_skew` | 5 s | 5 s | added to takeover time |
| `commit_every` | 0 s | 0 s | 0: a progress commit (2 operations) as each further step starts; more: steps of the span share one, more rerun after a crash |
| `snapshot_every` | 64 | 64 | bounds history reads to 64 GET per wakeup; each snapshot repeats the history (see [Storage](#storage)) |
| `continue_as_new_after` | 1,000 segments or 8 MiB | 1,000 segments or 8 MiB | how often a loop continues: one extra wakeup and 3 PUT per continuation |
| `wait` | 20 s | 20 s | on `h3://`: one waiting listing per workflow per `wait` |
| `store_retry` | 5 attempts, 100 ms to 5 s | same | requests per failing operation |
| `retention` | 30 days | 30 days | a collection and a tombstone end per chain, see [Collection](#collection) |

## Monthly cost on S3

**Prices.** AWS S3 Standard, US East (N. Virginia), from the [AWS price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json) of version 20261109154310, published 2026-11-09, read on 2026-11-15, the table `s.Prices("aws-s3-standard-us-east-1-2026-11-09")` (I-27); DELETE as the [S3 pricing page](https://aws.amazon.com/s3/pricing/) states it, free:

| Item | Price |
|---|---|
| PUT, COPY, POST, LIST requests | $0.005 per 1,000 |
| GET and all other requests | $0.0004 per 1,000 |
| DELETE requests | free |
| storage, first 50 TB | $0.023 per GB-month |

A month is 30 days, 2,592,000 s. Requests and storage only. The examples use 1 workflow and one worker, which holds the histories of the runs it serves, unless said, and count the 1 scan per wakeup measured on a lone run, an upper bound. Collections and tombstone ends are served by the idle scans the fleet pays for.

### An idle worker fleet

An idle worker scans every workflow it serves once per `max_idle`, its first scan after its phase, so `2,592,000 / max_idle` scans a month. Per worker and workflow:

| `max_idle` | LIST a month | Cost a month |
|---|---|---|
| 1 s (`script`) | 2,592,000 | $12.96 |
| 30 s (`service`) | 86,400 | $0.432 |
| 2 min | 21,600 | $0.108 |
| 5 min | 8,640 | $0.043 |

`N` workers serving `W` workflows each pay `N x W` times the row:

| Fleet, `service` | LIST a month | Cost a month |
|---|---|---|
| 1 worker, 1 workflow | 86,400 | $0.43 |
| 2 workers, 1 workflow | 172,800 | $0.86 |
| 3 workers, 2 workflows | 518,400 | $2.59 |

This is paid whatever runs; the examples below add to it. A longer `max_idle` costs less and delays work from another process and collections more; fewer, larger workers cost less than many small ones (see [Several workers](#several-workers)).

### A simple workflow

Start, 3 steps, a 1-day sleep, complete; 10,000 runs a month, each collected 30 days after its end; the client starts and does not wait for the result.

| Per run | PUT | GET | LIST | DELETE |
|---|---|---|---|---|
| client `start` | 2 | 1 | 0 | 0 |
| wakeup 1: 3 steps, suspend | 5 | 1 | 3 | 2 |
| wakeup 2: timer, end | 4 | 0 | 2 | 2 |
| collection of its 4 segments | 2 | 3 | 1 | 7 |
| tombstone end | 0 | 1 | 1 | 2 |
| scans, 2, one per wakeup | 0 | 0 | 2 | 0 |
| **total** | **13** | **6** | **9** | **13** |

10,000 runs: 220,000 PUT and LIST, $1.10; 60,000 GET, $0.024. **$1.12 a month**, plus the fleet. With 3 workers serving the workflow, the two that do not replay the run at its timer each read it, 2 GET or a GET and a PUT, and list 3 times: up to PUT 2, GET 4 and LIST 6 more per run, $0.32 to $0.41 a month, plus the fleet of 3. If another worker serves the second wakeup, it gets the `start` and 3 segments: 4 GET more per run, $0.016 a month. A client waiting on `result()` through the day would add about $0.0012 per run, $12 a month.

### A polling cron

A loop calling an API every 5 minutes in a step, with `iterate`, one chain all month: 8,640 iterations. Each iteration is one wakeup and one segment; the chain continues every 1,000 segments, about 8.6 times a month.

Per 1,000 iterations (one run of the chain), from the formula the test checks:

| | PUT | GET | LIST | DELETE |
|---|---|---|---|---|
| 1,000 iteration wakeups: 3 PUT, 1 LIST, 2 DELETE | 3,000 | 0 | 1,000 | 2,000 |
| the run's `start`, at its first wakeup | 0 | 1 | 0 | 0 |
| 15 snapshots | 15 | 0 | 0 | 0 |
| the continuation wakeup | 7 | 0 | 2 | 2 |
| the run's collection, 30 days later: its 1,017 keys listed in 2 pages | 0 | 5 | 2 | 1,019 |
| scans, 1 per wakeup | 0 | 0 | 1,001 | 0 |
| **total** | **3,022** | **6** | **2,005** | **3,021** |

A month: 43,433 PUT and LIST, $0.22; 52 GET. **$0.22 a month**, plus the fleet. On a worker that does not hold the history (several workers serving the chain, or a restart), each wakeup gets the `start` and `H(k)` history objects, each wakeup reading 32 on average: about 34,000 GET more per 1,000 iterations, $0.12 a month.

### A message-driven order

Start, wait for a payment message, 2 steps, done; 100,000 orders a month, each collected 30 days after its end.

| Per order | PUT | GET | LIST | DELETE |
|---|---|---|---|---|
| client `start` | 2 | 1 | 0 | 0 |
| wakeup 1: an empty segment, the rest marker | 3 | 1 | 1 | 2 |
| client `send` of the payment | 2 | 2 | 0 | 0 |
| wakeup 2: payment, 2 steps, end | 5 | 1 | 3 | 4 |
| collection of its 3 segments | 2 | 3 | 1 | 7 |
| tombstone end | 0 | 1 | 1 | 2 |
| scans, 2, one per wakeup | 0 | 0 | 2 | 0 |
| **total** | **14** | **9** | **8** | **15** |

100,000 orders: 2,200,000 PUT and LIST, $11.00; 900,000 GET, $0.36. **$11.36 a month**, plus the fleet. The rest marker that lets `runs` list an order waiting for its payment is 100,000 of those PUT, $0.50.

## What a run records of its cost

`c.cost()` (I-27) reads what the core counted; counting sends no request.

- **Zero requests added.** The counts come from the answers of the operations a run makes anyway: every table above is unchanged by them, and so is every count of `rust/tests/costs.rs`, which also checks that each run's last segment records the requests the counting store saw for its wakeups, and, with the table `aws-s3-standard-us-east-1-2026-11-09`, their money: 5,000 nano-dollars per PUT or LIST, 400 per GET.
- **Bytes added.** Each segment holds the chain's cost so far: about 18 bytes without prices, 30 with them, plus about 20 per step name that charged; snapshots repeat their segment's. A continued run's and a retried run's `start` and a child's end message carry the total. Measured, a run of 3 steps and a sleep, 4 segments, grows by 83 bytes and one waiting on a message, 3 segments, by 60: 0.8 MB a month for 10,000 simple runs, 6 MB for 100,000 orders (see [Storage](#storage)).
- **What a segment holds.** The requests made up to it, those of its own commit included as the core plans them: `log/<n>`, the effects (a child's `start`, its `gone`, index and due entry; a message's inbox key and due entry; a published value; a continuation's three creates; an end's collection entry, rest marker delete, `result`, the listing of its watchers and the messages to those its wakeup's opening listing showed), the effects marker, the snapshot, the deletes, and after a progress commit the inbox listing.
- **What it misses.** A retry by the adapter, an ambiguous create's read-back or a read a race adds, during a commit, is recorded by the run's next segment only if the same worker writes it, through what the worker holds of the run; so is a wakeup that wrote no segment (a leftover entry, a run left at a shutdown). A cancelled run's messages to its children are not counted, nor, after an end, any retry of its last operations, nor a wakeup of the run once it ended, which redoes or settles its end's effects and belongs to the fleet. So a run's cost is a lower bound by those, exact otherwise.
- **Not a run's.** Scans, the collection of ended runs and client calls are the fleet's: count them with this document's tables or with the `StoreOp` events of `observe` (I-29), which also give each operation's money with `prices`.
- **Prices per worker.** Each worker prices the requests it counts with its own `prices`; a worker without them adds counts and no money. A total never goes down.

## Storage

A run's keys are deleted at its collection, its `retention` after its chain ends, 30 days by default (I-16); a collected chain leaves `gone`, 9 bytes, for 7 more days. Never delete keys under a namespace, by hand or with an S3 lifecycle rule: runs still going read keys of runs that ended (I-16). Hints (due entries, claims, consumed inbox keys) are deleted as they are used; the probe is one empty key per namespace. Values are stored inline in the history (I-15), so their size adds directly.

Measured with small values (an integer per step, an empty input):

| Run | Keys until collected | Bytes |
|---|---|---|
| 3 steps, sleep, end | 6: `start`, 4 segments, `result` | 346 |
| receive, 2 steps, end | 5: `start`, 3 segments, `result` | 415 |
| polling loop | about 94 bytes per segment, 85 per segment repeated in a snapshot, with its origin | |

- A value is stored in the segment that records it, once more in every later snapshot of the run, and a run's input in its `start`. A result is in the last segment and in `result`.
- **Snapshots repeat the history.** Snapshot `n` holds every event of segments 0 to `n`. In a run of 1,000 segments with `snapshot_every` 64, the 15 snapshots hold 7,680 segments' worth: each value is stored about 8.7 times. `continue_as_new_after` bounds this per run.
- The examples above, at S3 Standard: 10,000 simple runs add 3.5 MB a month; the cron, about 0.75 MB per 1,000 iterations, 6.5 MB a month, plus `8.7 x V` per poll for a poll value of `V` bytes (1 KiB a poll adds about 73 MiB a month); 100,000 orders add 42 MB a month. Each GB kept costs $0.023 a month, so storage stays below a cent a month for these examples. With the default `retention` a namespace holds about the runs of the last 30 days and stops growing after the first month.

## Stores whose listings wait

`h3://` opens the same adapter as `s3://`, with listings that wait (I-16). The code changes when work is seen, not what a run costs:

- **Per run, the same operations.** Measured: a 1-step run on a fake h3 server, 9 operations, as on memory.
- **Idle, more listing requests, each held.** Per worker and workflow: one waiting listing per `wait` (20 s) and one plain listing per `max_idle` (5 min on a waiting store in `service`), 138,240 requests a month against 86,400 on S3 at 30 s. Measured with `wait` 1 s and `max_idle` 3 s: 6 waiting listings and 2 listings in 6 s.
- **Latency.** A start or message from another process is seen as soon as it lands, instead of within `max_idle`. Measured: with `max_idle` 1 h, a run started by another client ran at once.
- **`result` waits instead of polling.** Each time the run's end is read and it has none, the client lists the `result` key the core names with a wait of 0 s for its etag, then holds a waiting listing of it, sent again every 20 s, until the key exists; then it gets that `result`. A run's end is seen as soon as it is written, instead of up to 30 s later. Every 30 minutes it follows the chain again, 2 GET and a listing for the etag, so a wait on a chain that gets collected ends. An hour waited on a run that does not continue: 5 GET (each follow's 2, the last's 1) and 182 listings, 2 for the etag and 180 held.
- **A connection per held listing.** A worker holds one per workflow it serves, and a client one per `result` waited on. The adapter's connections are not capped in any host, so its other requests never queue behind them.
- An h3 server is self-hosted: its requests are not billed per request by AWS. What reaches its backends depends on the server.

## Redis

A Redis store performs the operations of every table above, each answer waiting for fsyncs on the primary and its replicas: its commands, latency, memory and failovers are in [Redis stores](redis.md).

## Keeping this document true

`rust/tests/costs.rs` runs every row of this document against a store that counts operations by kind and measures round trips from their timing, and asserts the numbers above. A change of the core or of a host that changes a count fails it.

- `cargo test --test costs` checks.
- `COSTS_MEASURE=1 cargo test --test costs -- --nocapture --test-threads 1` prints every measured row without asserting, to update this document and the test together.
- `COSTS_TRACE=1` adds each worker operation with its key, to explain a row.
- Time-dependent counts (idle scans, `result` polls, renewals, waiting listings) are measured over a controlled window and checked within bounds; their formulas are above. The monthly examples are arithmetic on the measured rows and the prices above.
