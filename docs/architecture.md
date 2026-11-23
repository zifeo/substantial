# Substantial architecture: store operations and costs

What every verb does to the store, how many round trips it waits for, what happens when it goes wrong, and what that costs on S3. The guarantees are the ones of the [interfaces](interfaces.md) (`I-n`); this document counts. Every count below is measured by `rust/tests/costs.rs` against the core in `wasm/`, or read from the code where a test cannot reach (failures, pages past 1,000 keys). The core decides every store operation, so the Python and TypeScript hosts perform the same ones.

- [How it works](#how-it-works)
- [Reading the tables](#reading-the-tables)
- [Clients](#clients)
- [Workers](#workers)
- [Workflow verbs](#workflow-verbs)
- [Retention and expiry](#retention-and-expiry)
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

- **The store is the only shared thing.** No broker, no server, no queue service. Clients and workers use six operations (I-26), each one request on S3, and an expiry in whole days that every write carries on the same request:

  | Operation | S3 request | S3 price class |
  |---|---|---|
  | create, exclusive | `PutObject` with `If-None-Match: *` | PUT, COPY, POST, LIST |
  | cas, compare-and-set | `PutObject` with `If-Match` on the ETag the writer read | PUT, COPY, POST, LIST |
  | get, with the ETag | `GetObject` | GET and all other |
  | list, at most 1,000 keys | `ListObjectsV2` | PUT, COPY, POST, LIST |
  | delete | `DeleteObject` | free |
  | expiry | the tag `substantial-retention=<d>d` (`x-amz-tagging`) on the write's own `PutObject`, which a lifecycle rule expires | none of its own |

- **Keys.** A namespace holds:

  | Key | What |
  |---|---|
  | `substantial.json` | the layout marker: the layout, 3, and the encoding |
  | `probe/<probe>/exclusive`, `probe/<probe>/deleted` | the probes of one `create` or `open`, `<probe>` an id it draws and holds as their bytes, deleted after them; written with an expiry of 1 day, which removes those a crash leaves |
  | `runs/<chain>` | the chain's head: its current run's start, segments, lease, block, inbox and published values, rewritten by a cas at every commit |
  | `runs/<run>` | a later run of a chain: a pointer to `runs/<chain>` while it is the chain's current run, then its archive |
  | `archives/<chain>` | the archive of a chain's first run, once a later run holds the head |
  | `queues/<workflow>/due/<time>~<chain>~s<n>` | empty: a wakeup |
  | `queues/<workflow>/due/19700101t000000000z~<chain>~m<ord>-<id>` | a message to the chain, its body inside, `ord` the sender's ULID: due for every worker, whatever its clock |
  | `queues/<workflow>/rest/<chain>` | empty: the chain waits on messages alone, for `runs`; no worker lists it |
  | `queues/<workflow>/blocked/<chain>` | empty: the chain is blocked, for `runs`; no worker lists it |
  | `watch/<chain>/<watcher>` | empty: a chain awaiting this chain's end with `c.result`, told at the end |

- **The commit point is a cas of the head.** A replay records what happened in a segment and commits it by a cas of `runs/<chain>` naming the ETag it read. Of two workers racing on a chain, the first cas wins, the other answers mismatch and drops its work. Due entries, rest and blocked markers and watcher keys are hints that can be rebuilt from the head.
- **Effects.** A commit with effects (a child's head and due entry, a message, a pointer, an archive) writes the head pending first, then the effects, then a cas that clears the pending; a crash between them leaves the head pending, and the next wakeup does the effects again.
- **The head.** A wakeup reads the chain's head once, 1 GET, replays the code against its segments, then commits. A worker keeps what it read and committed of a chain, its head and its ETag: its next wakeup of the chain reads nothing. A head grows with its history, bounded by `continue_as_new_after` (1,000 segments or 8 MiB), and every commit uploads it whole.
- **Continuation.** A continuation is 4 writes: the next run's wakeup, the head holding the next run, pending with the run before as its archive, the archive (`archives/<chain>` for a chain's first run, else `runs/<run>`), and the pointer `runs/<next run>`, so every run stays readable by its id. The next run's first write clears the pending.
- **Due entries.** A wakeup is an empty key sorting by time under its workflow; a message is a due entry at the epoch, which sorts before every timer. A worker lists the first page of each workflow it serves, takes the entries due, and sleeps until the earliest wakeup it saw, a timer, a retry or a message, or `max_idle`, whichever comes first; after its first scan, the worker's phase, drawn below `max_idle`, stands for `max_idle`.
- **Rest markers.** A chain that waits on messages alone has no due entry: its first such wait creates `queues/<workflow>/rest/<chain>`, which only `runs` lists, and its segment records it, so no later wakeup creates it again. Its end deletes it, free on S3.
- **Leases.** As its replay is about to start its first step attempt, a worker writes its lease into the head by a cas, and again every half `claim_ttl` while the replay runs; a commit in the middle of a replay keeps it. A replay that starts no step takes no lease. A lease only avoids running a step twice; the cas of the commit decides.
- **Expiry.** An ended run is written with its retention as its expiry, on the same write as its end: the store removes it after its retention, at most 2 days later. Nothing collects, nothing is listed or deleted for it (see [Retention and expiry](#retention-and-expiry)).
- **Several workers** serving a workflow list the same entries: a lease decides which one runs a chain's steps, and the others skip it; of two replays that start no step, the second's cas answers mismatch (see [Several workers](#several-workers)).
- **A store whose listings wait** (`h3://`) answers a listing only once its page changes, up to 20 s: workers wait instead of polling.
- **One core.** The protocol is a WebAssembly module every host loads. It decides each store operation; the host performs it, runs the workflow code and steps, and keeps time.

## Reading the tables

- **PUT, GET, LIST, DELETE** count writes, gets, listings and deletes. PUT counts creates and cas together: both are S3 PUTs, billed alike; the `cas` column, where a table has one, gives how many of the PUT are cas. On S3, PUT and LIST are billed at the same rate, GET at a lower one, DELETE is free.
- **Round trips** are the operations on the critical path, each waiting for the previous answer. A client action performs its operations one after the other, and a worker one at a time per chain but for the groups a commit sends at once: the effects of a segment, then their deletes. A client's round trips equal its operation counts. The listings of a scan are sent together: one round trip per scan. At a store latency `L`, a wakeup of 10 operations spends about `10 L` in the store, plus its steps.
- **Pages.** A listing returns at most 1,000 keys; past that, one more listing per 1,000 keys. The tables assume fewer, and give the formula where a count grows.
- Retries of failed operations are not counted (see [When something goes wrong](#when-something-goes-wrong)).

## Clients

| Verb | PUT | GET | LIST | DELETE | Round trips | Operations |
|---|---|---|---|---|---|---|
| `create`, new namespace | 6 | 0 | 0 | 2 | 8 | the opener's own `probe/<probe>/exclusive` twice (created, then exists), a cas of it naming a wrong ETag (mismatch), `probe/<probe>/deleted` created, deleted, then a cas naming its ETag (mismatch), `probe/<probe>/exclusive` deleted, the marker; 2 of the PUT are cas |
| `create`, existing namespace | 6 | 1 | 0 | 2 | 9 | the marker (exists), get it, then the probes as `open` does; 2 of the PUT are cas |
| `open` | 5 | 1 | 0 | 2 | 8 | get the marker, then the probes as a new namespace's `create` does: its own keys, so two openers at once never meet; 2 of the PUT are cas |
| `start` | 2 | 0 | 0 | 0 | 2 | the chain's head `runs/<chain>`, its due entry |
| `start`, same id and input again | 2 | 1 | 0 | 0 | 3 | the head (exists), get it to compare its start, the due entry again |
| `start`, id taken: `RunExists` | 1 | 1 | 0 | 0 | 2 | the head (exists), get it; the same for an id whose run ended and is kept |
| `start`, after its run was removed | 2 | 0 | 0 | 0 | 2 | as a new id |
| `send`, `cancel` | 1 | 1 | 0 | 0 | 2 | get the head, then the message's due entry, which holds the message |
| `send`, run ended: `RunEnded` | 0 | 1 | 0 | 0 | 1 | get the head |
| `send`, no chain: `RunNotFound` | 0 | 1 | 0 | 0 | 1 | get the head, missing |
| `status`, `result`, `send`, `cancel`, removed: `RunNotFound` | 0 | 1 | 0 | 0 | 1 | as no chain |
| `status`: pending, running, suspended, blocked, ended or failed | 0 | 1 | 0 | 0 | 1 | get the head, which holds the segments, the lease, the block and the end |
| `result`, ended | 0 | 1 | 0 | 0 | 1 | get the head; the same for a chain of several runs, whose head is the chain's |
| `result`, a poll while not ended | 0 | 1 | 0 | 0 | 1 | get the head |
| `read`, a value or nothing published | 0 | 1 | 0 | 0 | 1 | get the head, which holds the latest value published under each key |
| `runs`, 2 pending | 0 | 2 | 3 | 0 | 5 | list the workflow's `blocked/`, `due/` and `rest/`, get each chain's head |
| `runs`, 2 waiting on messages | 0 | 2 | 3 | 0 | 5 | as pending; a chain found by its rest marker alone, its head |
| `runs`, 1 blocked | 0 | 1 | 3 | 0 | 4 | as pending; a chain found by its blocked marker is not read again under `due/` |
| `history`, a run or a retry | 0 | 1 | 0 | 0 | 1 | get the head, which holds a retry's resumed events with its own |
| `retry` | 5 | 1 | 0 | 0 | 6 | get the head, its cas pending with the failed run's archive, the archive (`archives/<chain>` for a first run), the retry's pointer and its due entry, the cas that clears the pending; 2 of the PUT are cas |
| `retry(keep=)`, a reset of a completed run, the second of its chain | 5 | 1 | 0 | 0 | 6 | get the head, its cas pending, the reset run's archive written over its pointer (a cas), the new run's pointer and its due entry, the cas that clears the pending; 3 of the PUT are cas |
| `retry`, a failure that ran undos: `InvalidArgument` | 0 | 1 | 0 | 0 | 1 | get the head, whose last segment holds an undo |

- **Reading by id.** Every verb reads one key, `runs/<id>`: the chain's head for a chain's id; for a later run's id, its pointer then the chain's head, 1 GET more, or its archive once the chain went past it. No listing follows a chain.
- **`result` polls.** Each poll gets the head, one key. The interval doubles from 10 ms up to 30 s, in every host: polls at 0, 10, 30, 70, 150, 310, 630, 1,270, 2,550, 5,110, 10,230, 20,470 and 40,950 ms, then every 30 s, `13 + floor((T - 40.95) / 30)` polls in `T` seconds past 41 s (measured: 10 in 6 s). A chain removed by its expiry ends the wait with `RunNotFound` at the next poll. **Waiting an hour costs 131 GET, $0.000052 on S3; a day 2,891 GET, $0.0012.** On a store whose listings wait, `result` does not poll (see [Stores whose listings wait](#stores-whose-listings-wait)).
- **`result(max_interval=)`** caps the interval at `max_interval` instead of 30 s: a longer cap costs fewer GET on a long wait, and sees the end up to `max_interval` after it is written:

  | `max_interval` | GET in an hour | GET in a day | A day on S3 |
  |---|---|---|---|
  | 30 s, the default | 131 | 2,891 | $0.0012 |
  | 2 min | 43 | 733 | $0.00029 |
  | 5 min | 26 | 302 | $0.00012 |
  | 30 min | 19 | 65 | $0.000026 |

- **`runs`** lists the workflow's `blocked/`, `due/` and `rest/` prefixes, one listing per 1,000 keys each, and gets the head of each chain once. Its pages come as the iteration goes, so stopping early costs less.
- **`history`**, **`status`** and **`read`** get the head alone, whatever the length of the history: the head holds every segment of the chain's current run.
- **A client in another process** than the worker, or through another `Substantial`, is seen at the worker's next listing, within `max_idle`; through the worker's own `Substantial` it wakes the workers serving the run's workflow at once, without a request, and they list that workflow alone.

## Workers

### Scans

A scan lists the first page of `due/` of every workflow the worker serves: `W` LIST for `W` workflows, sent together, one round trip. Measured: 2 workflows with `max_idle` 200 ms, 9 to 11 scans of 2 listings in 2 s.

A worker scans:

| When | Scans |
|---|---|
| idle | one every `max_idle`, the first after the worker's phase, drawn once below `max_idle` from its seed: workers started together list at times of their own |
| a wakeup it saw comes due (a timer, a retry) | one at that time |
| a replay ends | only if a scan saw an entry of the chain the replay did not serve, the worker was full on a store that waits, or the replay created an entry due now (a child, a message, a continuation); otherwise the timer comes forward to the entry its suspension created |
| it skipped a chain held by another worker's live lease | one more at once, once it let the chain go |
| a due run of a version it does not serve stays due | one every half `claim_ttl`, and a get of the head every `claim_ttl`, whatever `max_idle`, while it stays due: measured on the test environment, 240 listings and 120 gets an hour at the default `claim_ttl` |
| a start or send through its own `Substantial` | a listing of the run's workflow alone, at once |

A workflow's entries are listed past a full page (`due_page` keys) of entries all due that the worker cannot take (chains held by other workers, versions it does not serve), in rounds of up to `due_pages` pages, to their end or their first entry not yet due. A page is walked past only when all its entries were due when it was listed. With `S` skipped entries at the head of a workflow's entries, a scan of it costs about `S / due_page` listings, and each skipped chain's head is read again about once per `claim_ttl`. Runs of a version no worker serves cause this: give every version with runs left a worker.

Measured per wakeup with nothing else going on: 1 scan, the one that finds it; a wakeup that starts a child or sends a message adds one. On a busy worker scans are shared between chains, so this is an upper bound.

### Several workers

Workers serving a workflow share its due entries: nothing splits them ahead, each lists all of them, and a lease decides which worker runs a chain's steps. What each worker adds:

- **Idle.** Its own scans, `W` LIST per `max_idle`: the fleet's idle cost grows with workers times workflows ([An idle worker fleet](#an-idle-worker-fleet)).
- **A timer.** Every worker that listed a chain's timer scans when it comes due. When the wakeup runs a step, one replays the chain; each other reads the head, and its cas of the lease may find the winner's, so it reads the head again: under a live lease it skips the chain and, once it let it go, scans again; if the winner already ended the run, it redoes the end's last steps, the listing of the watchers and the delete of the entry. Measured with 3 workers serving 1 workflow, at a timer followed by a step: the one that replayed the chain, PUT 2 (2 cas), GET 0 or 1, LIST 1, DELETE 1 and 1 scan, as alone; each of the two others, PUT 0 or 1, GET 1 or 2, LIST 0 or 1, DELETE 0 or 1, 1 to 5 operations, and 1 or 2 scans. Per such timer, each worker more costs up to 2 GET and a PUT, and `1 + 2W` LIST. When the wakeup runs no step, there is no lease to meet: each worker that read the head before the first commit replays it and its cas answers mismatch. The outcome is the same, only the cost of the lost races differs.
- **A start or a message from another process.** The first worker whose scan comes after it takes it; the others' scans no longer list its entry once that replay committed, a few round trips later, so they rarely meet its lease.
- **An end.** It leaves no entry: no worker's timer is set by it, and the scans after it find nothing. Measured with 3 workers: no operation after the end, then one scan of each worker when woken.

**Fewer, larger workers.** Raise `concurrency` before adding workers: a worker more serves no more chains on its own, and adds its idle listings for every workflow it serves and, at every timer of the workflows it serves, a scan, a read of the head and a scan again.

### One wakeup

Every wakeup is built from these blocks, one operation at a time:

| Block | PUT | cas | GET | LIST | DELETE | Operations |
|---|---|---|---|---|---|---|
| open | 0 | 0 | `G + M` | 0 | 0 | get the head unless the worker holds it (`G`, 0 or 1); get each message entry it serves (`M`), which holds the message; list the chain's messages too only when the scan's page stopped inside them |
| lease | 1 | 1 | 0 | 0 | 0 | the head's cas with the worker's lease, as the replay is about to start its first step attempt; none for a wakeup whose replay starts no step (`K`, 0 or 1, below) |
| progress commit | 1 | 1 | 0 | 1 | 0 | the head's cas, then a listing of the chain's messages, `queues/<workflow>/due/19700101t000000000z~<chain>~m` |
| suspension commit | 1 + T | 1 | 0 | 0 | 1 + C + S | due entry at the next timer if any (`T`), or the chain's rest marker at its first wait on messages alone, then the head's cas; delete the due entry it was woken by, the `C` message entries it consumed, and a timer entry the run no longer needs (`S`, see `wait_until`) |
| end commit | 1 + E | 1 + E | 0 | 1 | 1 + C + S + R | the head's cas with the run's own retention as its expiry, whatever its children do; for a later run of its chain the end is two-phase, the head pending, the pointer's expiry, the head's (`E`, 0 or 2 cas); list `watch/<chain>/`; delete the rest marker of a chain that waited on messages alone (`R`, 0 or 1), the due entry and the messages it consumed |
| watchers told | `V` | 0 | `V` | 0 | `V` | after the end's last write, for each of the `V` chains awaiting it: get its head, create its message, delete its key; a continuation carries each watcher instead |
| effects | `F` | 1 | 0 | 0 | 0 | a commit with effects writes the head pending, then its `F` effects (a child's head and due entry, a message's due entry), then the cas that clears the pending |
| renewal | 1 | 1 | 0 | 0 | 0 | the lease written again every `claim_ttl / 2` while the replay runs |

- With `commit_every` 0, each step is committed before the next step starts or the run waits on steps: a progress commit, 2 operations. The step before a suspension or the end lands in that commit. Measured: a run of 0 steps costs 4 operations, of 1 step 5, each further step 2.
- A run whose first replay records nothing (it waits on a message at once) commits an empty segment without a due entry: its first wakeup is 4 operations, the head's GET, its rest marker, the cas, and the delete of its due entry, and `status` reads it `suspended`. The segment counts toward `continue_as_new_after` like any other.
- A wakeup of a chain whose run already ended (a leftover due entry) reads the head and redoes its end's last steps: it lists the watchers and deletes the entry, 3 operations.
- A wakeup of a removed id (a due entry created late, after the head's expiry) gets the head, missing, and deletes its entry: 2 operations, measured.

### A worker run on a schedule

`drain(within=)` (I-18) scans every workflow it serves once when it starts, then only at the wakeups due within its horizon that it listed or created and at the end of a crashed worker's lease, and stops once none is left: no idle listing and no waiting listing. Measured with 1 workflow: nothing due, LIST 1; a run of one step due now, its wakeup (PUT 2, all cas, GET 1, LIST 1, DELETE 1) and 1 scan; a run whose sleep ends within the horizon, its two wakeups and 2 scans.

A worker a job starts every `P` seconds, serving `W` workflows, costs `2,592,000 / P x W` LIST a month when nothing is due, against `2,592,000 / max_idle x W` for a worker that runs all the time: every 10 minutes, 4,320 LIST, $0.022 a month per workflow, against $0.43 at a `max_idle` of 30 s. Work due between two jobs waits for the next one: up to `P` of latency.

## Workflow verbs

What each call adds, at `commit_every` 0, on top of the open and the commit of its wakeup.

| Verb | PUT | GET | LIST | DELETE | Notes |
|---|---|---|---|---|---|
| step | 1 | 0 | 1 | 0 | a progress commit, the head's cas and a message listing, as the next step starts; none for the step before a suspension or the end |
| step, `commit_every` > 0 | 0 | 0 | 0 | 0 | steps within the span share a commit; measured 3 steps at `1h`: 5 operations, as 1 step |
| step retry | 1 | 0 | 0 | 0 | the failed attempt and its backoff land in one suspension commit, its due entry at the backoff; then a wakeup at its time |
| step running long | `t / (claim_ttl / 2)` | 0 | 0 | 0 | renewals of the lease, each a cas; measured 4 to 6 in 1.2 s at `claim_ttl` 400 ms |
| `now`, `random`, `uuid` | 0 | 0 | 0 | 0 | recorded with the next commit; measured |
| `sleep`, `sleep_until` | 1 | 0 | 0 | 0 | the due entry of the suspension; then a scan and a wakeup at its time |
| `receive`, `wait_until` without timeout | 0 or 1 | 0 | 0 | 0 or 1 | no due entry, the message brings one; the chain's first wait on messages alone creates its rest marker, for `runs`, and its end deletes it: 1 PUT and 1 free DELETE per chain that ever waits so, including a parent awaiting a child, and per continuation of an actor |
| a message consumed | 0 | 1 | 0 | 1 | the get of its due entry, which holds it, and the entry's delete at the wakeup's last commit; plus the sender's operations |
| `handle` | 0 | 0 | 0 | 0 | messages as above |
| `wait_until` with timeout | 1 | 0 | 0 | 1 | the timer's due entry; when a message wins, the commit after it deletes the entry |
| `start` of a child | 2 + 1 | 0 | 0 | 0 | the child's head and its due entry, and the cas that clears the parent's pending head |
| child `result` | 1 | 1 | 0 | 1 | the child's end tells the parent, a message entry; the parent's wakeup reads it, and its end deletes the message |
| `send` from a workflow | 1 + 1 | 1 | 0 | 0 | before the commit, get the chain's head; after it, the message's due entry and the cas that clears the pending. To a chain that ended or does not exist, `RunEnded` or `RunNotFound` at the call, 1 GET, and nothing created |
| `publish` | 0 | 0 | 0 | 0 | the value rides in the head's cas of the commit |
| `poll`, a try not ready | 3 | 0 | 0 | 1 | a step and a sleep: each try is a wakeup of its own, the lease, the timer's due entry and the suspension's cas, the served entry deleted, measured; the try that is ready ends it as a step does |
| `schedule`, a fire | 3 | 0 | 3 | 0 | the plan's step, the fire and the body's step, each landing in a progress commit, the published value in the head; measured from the fire's timer to the next suspension, the body running one step: PUT 6 (5 cas), LIST 3, DELETE 1, a wakeup, the lease, 3 progress commits (the plan's step, the fire, the body's step), the next timer's due entry, the suspension's cas |
| `iterate` | 0 | 0 | 0 | 0 | no operation per iteration; a continuation when the history passes `continue_as_new_after` |
| `continue_as_new` | 4 | 0 | 0 | 1 | 4 writes: the next run's wakeup, the head with the next run (a cas), the archive, the pointer; the next run is replayed in the same wakeup, its first write clearing the pending, and its wakeup deleted with the served one; its end, two-phase, writes the pointer's expiry too, 2 cas more |
| `c.result(chain)` | 1 | 2 | 0 | 0 | before the commit: get the chain's head, create `watch/<chain>/<own chain>`, get the head again, which catches an end written meanwhile. A chain that ended answers at once, 1 GET; no chain or a removed one raises `RunNotFound`, 1 GET. The end's message then wakes the run, as a child's does. The same chain again in the run costs nothing |
| `child.cancel` | 1 + 1 | 1 | 0 | 0 | as a `send` from a workflow to the child: before the commit, get the child's head; after it, the `substantial.cancel` message and the cas that clears the pending. A child that ended raises `RunEnded` at the call, 1 GET. Then the child pays a cancelled end |
| an end that cancels children: `cancel`, from inside or a client, or a completed or failed end whose chain started a child it left running | 1 per running child not abandoned | 1 per child | 0 | 0 | the end writes its head pending with its expiry, reads each child's head and sends each running child not started with `abandon` `substantial.cancel`, then clears the pending. Each child pays a cancelled end, PUT 3 (2 cas), GET 1, LIST 1, DELETE 2, whose notice wakes the ended parent: it reads its head, lists its watchers again and deletes the notice and its entry, GET 1, LIST 1, DELETE 2, writing nothing. Measured the same for a completed, a failed and a cancelled end, with 1 and 3 children |
| `rollback`, saga, undo | 0 | 0 | 0 | 0 | an undo is a step; the undos of a rollback ride in its end commit |

## Retention and expiry

An ended run is kept, readable by its id and its id refused to a new start, for its retention, a whole number of days counted from its own end whatever its children do, and removed by the store at most 2 days after that (I-16). Nothing in Substantial collects:

- **No request for it.** The end's write of the head carries the retention as its expiry, on the same request. Nothing is listed, read or deleted later: per run, retention costs 0 requests, against a collection and a tombstone end in Substantial 0.3.
- **A chain that continued.** The archives of its earlier runs and the pointer of its current run get the expiry of the chain's end: the end of a later run is two-phase, its head pending, then the pointer's and archives' expiry, then the head's, 2 cas more than the end of a chain of one run.
- **Parents and children.** Each run expires after its own retention: a parent's end reads no child's head for it. A child that outlives its parent, started with `abandon`, tells the ended parent at its end; that late notice costs the parent's chain a GET of its head, a LIST of its watchers and the notice's DELETE, and nothing is written back. A notice that reaches a chain removed, or a run that took its id since, is deleted unconsumed, and so is a parent's cancel to a child gone (I-16).
- **S3 and `h3://`.** Each retention value in use needs a lifecycle `Expiration` rule of that many days on the namespace's prefix, filtered by the tag `substantial-retention=<d>d`. A worker checks at start that a rule exists for its `retention`, each of its workflows' and 1 day, the expiry of the probe keys a crash during a `create` or an `open` leaves: one `GetBucketLifecycleConfiguration`, a GET-class request, per worker start; a worker missing one refuses to start with the rule's JSON.
- **Removed keys stay removed.** A late wakeup of a removed id reads the head, missing, and deletes its entry: 2 operations, nothing written back.

## Measured runs

Each line is one wakeup, a worker serving 1 workflow. PUT counts creates and cas; the `cas` column gives the cas among them.

| Run | Wakeup | PUT | cas | GET | LIST | DELETE | Ops | Round trips |
|---|---|---|---|---|---|---|---|---|
| 0 steps | start to end | 1 | 1 | 1 | 1 | 1 | 4 | 4 |
| 1 step | start to end | 2 | 2 | 1 | 1 | 1 | 5 | 5 |
| 3 steps | start to end | 4 | 4 | 1 | 3 | 1 | 9 | 9 |
| 3 steps, sleep, end | start: 3 steps, suspend | 5 | 4 | 1 | 2 | 1 | 9 | 9 |
| | timer: end | 1 | 1 | 0 | 1 | 1 | 3 | 3 |
| receive, 2 steps, end | start: an empty segment, the rest marker | 2 | 1 | 1 | 0 | 1 | 4 | 4 |
| | message: 2 steps, end | 3 | 3 | 1 | 2 | 2 | 8 | 8 |
| `wait_until` with timeout | start: suspend | 2 | 1 | 1 | 0 | 1 | 4 | 4 |
| | message: end, the timer's entry deleted | 1 | 1 | 1 | 1 | 2 | 5 | 4 |
| parent with a child | parent: start the child, suspend | 5 | 2 | 1 | 0 | 1 | 7 | 7 |
| | child: end, tell the parent | 3 | 2 | 1 | 1 | 1 | 6 | 6 |
| | parent: read the child's end, end | 1 | 1 | 1 | 1 | 2 | 5 | 5 |
| a run sends to a waiting one | sender: send, end | 3 | 2 | 2 | 1 | 1 | 7 | 7 |
| | target: message, end | 1 | 1 | 1 | 1 | 2 | 5 | 5 |
| publish, end | start to end | 1 | 1 | 1 | 1 | 1 | 4 | 4 |
| `continue_as_new`, then end | one wakeup: continue, 4 writes, then the next run's end, two-phase | 7 | 4 | 1 | 1 | 2 | 11 | 10 |
| a step failing once, retried | start: attempt, backoff | 3 | 2 | 1 | 0 | 1 | 5 | 5 |
| | retry: end | 2 | 2 | 0 | 1 | 1 | 4 | 4 |
| a step, then `rollback` | start to failed | 3 | 3 | 1 | 2 | 1 | 7 | 7 |
| a step, then another failing for good, retried | the retry's first wakeup: get the head, which holds the failed run's events, the lease, the end two-phase | 4 | 4 | 1 | 1 | 1 | 7 | 7 |
| cancel a parent with a running child | parent: cancelled with its expiry, cancel the child | 3 | 2 | 2 | 1 | 2 | 8 | 8 |
| | child: cancelled, tell the parent | 3 | 2 | 1 | 1 | 2 | 7 | 7 |
| | ended parent: the child's notice, dropped | 0 | 0 | 1 | 1 | 2 | 4 | 4 |
| a parent before its abandoned child | parent: start the child, end with its expiry | 4 | 2 | 1 | 1 | 1 | 7 | 7 |
| | child: a step, a sleep | 3 | 2 | 1 | 0 | 1 | 5 | 5 |
| | child: end, tell the parent | 3 | 2 | 0 | 1 | 1 | 5 | 5 |
| | ended parent: the child's notice, dropped | 0 | 0 | 1 | 1 | 1 | 3 | 3 |
| a parent cancels the child it started | parent: start the child, cancel it, suspend | 6 | 2 | 2 | 0 | 1 | 9 | 8 |
| | child: cancelled, tell the parent | 3 | 2 | 2 | 1 | 2 | 8 | 7 |
| | parent: read the child's end, end | 1 | 1 | 1 | 1 | 2 | 5 | 5 |
| a run awaits a chain with `c.result` | waiter: register, suspend | 3 | 1 | 3 | 0 | 1 | 7 | 7 |
| | watched run: end, tell 1 watcher | 2 | 1 | 2 | 1 | 3 | 8 | 8 |
| | watched run: end, tell 2 watchers | 3 | 1 | 3 | 1 | 4 | 11 | 11 |
| | waiter: read the end, end | 1 | 1 | 1 | 1 | 2 | 5 | 5 |
| | a chain already ended: register, end | 1 | 1 | 2 | 1 | 1 | 5 | 5 |
| a watched run continues as new | continue, carry the watcher | 6 | 2 | 2 | 0 | 3 | 11 | 10 |
| | next run: end, tell the watcher | 4 | 3 | 2 | 1 | 3 | 10 | 10 |
| a step, then a bug of the code, fixed | start: blocked, nothing recorded, the head's block, the due entry of the backoff, the marker | 4 | 2 | 1 | 0 | 1 | 6 | 5 |
| | after the fix: end, delete the marker | 2 | 2 | 0 | 1 | 2 | 5 | 4 |

A wakeup on the worker that served the chain's last one gets nothing it holds: the head's GET appears only at a chain's first wakeup there. A polling loop, one step then a sleep per iteration, one segment per iteration: the wakeup of each iteration costs PUT 3 (2 cas: the lease and the suspension; the timer's due entry), DELETE 1, 4 round trips; on a worker that does not hold the head, also GET 1. When the history reaches `continue_as_new_after` segments, the next wakeup continues before its step and runs the next run's first iteration in place: PUT 7 (3 cas), DELETE 2, 8 round trips. The test runs it with a threshold of 8 segments, and checks every wakeup against this formula.

## When something goes wrong

Every create is exclusive and every cas names the value it replaces, so doing an operation twice does it once. What a failure costs:

| Where | What happens | Guarantee |
|---|---|---|
| any operation fails | the adapter retries it, `store_retry`: 5 attempts, backoff from 100 ms to 5 s; S3 requests time out after 5 s to connect and 60 s per attempt | I-26 |
| a create or a cas with no answer (timeout, 5xx, lost connection) | the adapter gets the key: for a create the same bytes mean created, other bytes exist, missing means create again; for a cas the same bytes mean written, the ETag named still current means send it again, anything else mismatch; 1 more GET per ambiguous attempt | I-26 |
| an operation still failing in a worker | the core reissues it up to 5 times (`OP_RETRIES`), each with the adapter's retries, then drops the replay; the due entry stays, yet the chain is tried again only once its lease's `claim_ttl` passed, not at the next scan | I-20 `StoreError` |
| an operation still failing in a client | `StoreError`; a `start` or `send` retried with the same id creates once | I-17, I-9 |
| a client crashes after `start` before its due entry | the run waits; starting it again with the same id completes it (the 3-operation row) | I-17 |
| a client send races the run's end | accepted, never consumed | I-17 |
| a worker crashes mid-replay | its lease expires; the due entries that started the replay are still there, so another worker takes the chain over within `claim_ttl + claim_skew` (35 s) of the last renewal: it reads the head and, before its first step attempt, writes its own lease; steps not committed run again | I-1, I-18 |
| a worker finds a live lease | it reads the head, 1 GET, and leaves the chain until the lease expires | I-18 |
| a worker crashes after a commit's cas, before its effects | the head is left pending: the next wakeup does the effects again, a create that exists answering so, then clears the pending, 1 cas; due entries were not deleted yet, so that wakeup is at once | I-9, I-11 |
| a worker crashes after the effects, before its deletes | leftover due entries: a wakeup that reads the head, finds nothing new or the run ended, and deletes them, 2 or 3 operations | I-26 |
| two workers commit the same segment | one cas of the head wins; the other answers mismatch, drops its replay, and its steps ran once more than recorded | I-1, I-2 |
| the workflow code raises an error that is no end, or diverges from its history | the replay is dropped, recording nothing; the head's cas holds the block, then a due entry after the blocked backoff and the queue's marker, `blocked_initial` doubling up to `blocked_max`: 1 s to 1 min in `script`, 10 s to 10 min in `service`, so a run blocked a day at the cap is tried 1,440 times in `script` and 144 in `service`, each about 5 operations (see [Measured runs](#measured-runs)) | I-20 |
| a step hangs | renewals keep the lease, 1 cas every 15 s, until its attempt times out: the step's `timeout`, else `step_timeout`, 5 min by default | I-18 |
| shutdown | no new replay; steps in flight may commit within `shutdown_grace`; chains left keep their due entries | I-18 |

## Settings that drive cost

Values from the presets (I-18):

| Setting | `script` | `service` | Cost |
|---|---|---|---|
| `max_idle` | 1 s | 30 s; 5 min on a waiting store | idle: `W` LIST per `max_idle` per worker; latency of work from another process |
| `due_page` | 1,000 | 1,000 | keys per scan listing; more due runs at once than this per workflow need more scans |
| `due_pages` | 10 | 10 | pages listed past entries the worker skips |
| `claim_ttl` | 30 s | 30 s | a renewal of the lease, 1 cas, every 15 s of a replay; takeover after `claim_ttl + claim_skew` |
| `claim_skew` | 5 s | 5 s | added to takeover time |
| `commit_every` | 0 s | 0 s | 0: a progress commit (2 operations) as each further step starts; more: steps of the span share one, more rerun after a crash |
| `continue_as_new_after` | 1,000 segments or 8 MiB | 1,000 segments or 8 MiB | how often a loop continues, 4 writes and a DELETE per continuation; bounds the head every commit uploads |
| `wait` | 20 s | 20 s | on `h3://`: one waiting listing per workflow per `wait` |
| `store_retry` | 5 attempts, 100 ms to 5 s | same | requests per failing operation |
| `retention` | 30 days | 30 days | no request: the end's write carries it as its expiry; on S3 a lifecycle rule per value in use, checked at each worker start (see [Retention and expiry](#retention-and-expiry)) |
| `blocked_initial`, `blocked_max` | 1 s to 1 min | 10 s to 10 min | a blocked run's wakeups, about 5 operations each, the interval doubling up to the cap |

## Monthly cost on S3

**Prices.** AWS S3 Standard, US East (N. Virginia), from the [AWS price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json) of version 20261109154310, published 2026-11-09, read on 2026-11-15, the table `s.Prices("aws-s3-standard-us-east-1-2026-11-09")` (I-27), where a cas is priced as the PUT it is; DELETE as the [S3 pricing page](https://aws.amazon.com/s3/pricing/) states it, free:

| Item | Price |
|---|---|
| PUT, COPY, POST, LIST requests, creates and cas alike | $0.005 per 1,000 |
| GET and all other requests | $0.0004 per 1,000 |
| DELETE requests | free |
| storage, first 50 TB | $0.023 per GB-month |

A month is 30 days, 2,592,000 s. Requests and storage only. The examples use 1 workflow and one worker, which holds the heads of the chains it serves, unless said, and count the 1 scan per wakeup measured on a lone run, an upper bound. Retention adds no request.

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

This is paid whatever runs; the examples below add to it. A longer `max_idle` costs less and delays work from another process more; fewer, larger workers cost less than many small ones (see [Several workers](#several-workers)).

### A simple workflow

Start, 3 steps, a 1-day sleep, complete; 10,000 runs a month, each kept 30 days after its end; the client starts and does not wait for the result.

| Per run | PUT | GET | LIST | DELETE |
|---|---|---|---|---|
| client `start` | 2 | 0 | 0 | 0 |
| wakeup 1: 3 steps, suspend (4 cas) | 5 | 1 | 2 | 1 |
| wakeup 2: timer, end (1 cas) | 1 | 0 | 1 | 1 |
| scans, 2, one per wakeup | 0 | 0 | 2 | 0 |
| **total** | **8** | **1** | **5** | **2** |

10,000 runs: 130,000 PUT and LIST, $0.65; 10,000 GET, $0.004. **$0.65 a month**, plus the fleet. With 3 workers serving the workflow, its timer's wakeup runs no step, so each of the two others that read the head before the first commit replays it and its cas answers mismatch (see [Several workers](#several-workers)), plus the fleet of 3. If another worker serves the second wakeup, it gets the head: 1 GET more per run, $0.004 a month. A client waiting on `result()` through the day would add 2,891 GET, about $0.0012 per run, $12 a month.

### A polling cron

A loop calling an API every 5 minutes in a step, with `iterate`, one chain all month: 8,640 iterations. Each iteration is one wakeup and one segment; the chain continues every 1,000 segments, about 8.6 times a month.

Per 1,000 iterations (one run of the chain), from the formula the test checks:

| | PUT | GET | LIST | DELETE |
|---|---|---|---|---|
| 1,000 iteration wakeups: 3 PUT (2 cas), 1 DELETE | 3,000 | 0 | 0 | 1,000 |
| the continuation, in the wakeup of the next run's first iteration: its 4 writes and the delete of the next run's wakeup | 4 | 0 | 0 | 1 |
| scans, 1 per wakeup | 0 | 0 | 1,000 | 0 |
| **total** | **3,004** | **0** | **1,000** | **1,001** |

A month: 34,595 PUT and LIST, $0.17; a GET of the head at the worker's first wakeup. **$0.17 a month**, plus the fleet. On a worker that does not hold the head (several workers serving the chain, or a restart), each wakeup gets it: 1 GET more per iteration, 8,640 GET, $0.0035 a month.

### A message-driven order

Start, wait for a payment message, 2 steps, done; 100,000 orders a month, each kept 30 days after its end.

| Per order | PUT | GET | LIST | DELETE |
|---|---|---|---|---|
| client `start` | 2 | 0 | 0 | 0 |
| wakeup 1: an empty segment, the rest marker (1 cas) | 2 | 1 | 0 | 1 |
| client `send` of the payment | 1 | 1 | 0 | 0 |
| wakeup 2: payment, 2 steps, end (3 cas) | 3 | 1 | 2 | 2 |
| scans, 2, one per wakeup | 0 | 0 | 2 | 0 |
| **total** | **8** | **3** | **4** | **3** |

100,000 orders: 1,200,000 PUT and LIST, $6.00; 300,000 GET, $0.12. **$6.12 a month**, plus the fleet. The rest marker that lets `runs` list an order waiting for its payment is 100,000 of those PUT, $0.50.

## What a run records of its cost

`c.cost()` (I-27) reads what the core counted; counting sends no request.

- **Zero requests added.** The counts come from the answers of the operations a run makes anyway: every table above is unchanged by them, and so is every count of `rust/tests/costs.rs`, which also checks that each run's last segment records the requests the counting store saw for its wakeups, and, with the table `aws-s3-standard-us-east-1-2026-11-09`, their money: 5,000 nano-dollars per PUT, cas or LIST, 400 per GET.
- **Bytes added.** Each segment holds the chain's cost so far, a few tens of bytes, more with prices and steps that charged; a continued run's and a retried run's start and a child's end message carry the total. The sizes in [Storage](#storage) include them.
- **What a segment holds.** Every request its worker sent for the chain up to its own commit, and those of its commit as the core plans them, each as sent: the head's GET, the lease and its renewals, each cas, the effects (a child's head and due entry, a message, a continuation's next wakeup, archive and pointer), the deletes, the message listings after progress commits, an end's reads of the heads of the children it cancels and the cancels it sends them, its expiry writes, the listing of its watchers and the delete of its rest marker.
- **What it misses.** The telling of its watchers, after its end's last write: per watcher a GET of its head, its message and the delete of its key. A wakeup after the run's end, which writes no segment: a child's late end notice, a leftover entry. A retry by the adapter, an ambiguous write's read-back or a read a race adds, during a commit, is recorded by the run's next segment only if the same worker writes it, through what the worker holds of the chain; so is a wakeup that wrote no segment (a run left at a shutdown). So a run's cost is a lower bound by those, exact otherwise.
- **Not a run's.** Scans and client calls are the fleet's: count them with this document's tables or with the `StoreOp` events of `observe` (I-29), which also give each operation's money with `prices`.
- **Prices per worker.** Each worker prices the requests it counts with its own `prices`; a worker without them adds counts and no money. A total never goes down.

## Storage

A chain's head, the archives of its earlier runs and its pointers expire with its retention, 30 days after its end by default (I-16), removed by the store at most 2 days later. Hints (due entries, consumed messages, watcher keys) are deleted as they are used; the probe keys are deleted by each `create` and `open` after its probes, and expire after a day when a crash stopped it. Values are stored inline in the history (I-15), so their size adds directly.

Measured with small values (an integer per step, an empty input):

| Run | Keys until removed | Bytes |
|---|---|---|
| 3 steps, sleep, end | 1: the head, 4 segments | 364 |
| receive, 2 steps, end | 1: the head, 3 segments | 392 |
| polling loop | the archive of each continued run, about 110 bytes per segment, and the head of the current one | |

- A value is stored once, in the segment that records it, and the latest value published under each key once more in the head; a run's input in its start, in the head and in its archive.
- **Each commit uploads the head whole.** A head is bounded by `continue_as_new_after`, 8 MiB: on S3 a commit is one PUT whatever its size, so the request cost does not change, but a head near the bound uploads about 8 MiB per commit.
- **Versioned buckets.** Each cas leaves the head before it as a noncurrent version, which the lifecycle rules of I-16 remove: a bucket with versioning on keeps up to a few of them per chain for about a day.
- The examples above, at S3 Standard: 10,000 simple runs add 3.6 MB a month; the cron, about 110 KB per 1,000 iterations, 1 MB a month, plus `V` per poll for a poll value of `V` bytes (1 KiB a poll adds about 8.4 MiB a month); 100,000 orders add 39 MB a month. Each GB kept costs $0.023 a month, so storage stays below a cent a month for these examples. With the default `retention` a namespace holds about the runs of the last 30 days and stops growing after the first month.

## Stores whose listings wait

`h3://` opens the same adapter as `s3://`, with listings that wait (I-16). The code changes when work is seen, not what a run costs:

- **Per run, the same operations.** Measured: a 1-step run on a fake h3 server, 5 operations, as on memory.
- **Idle, more listing requests, each held.** Per worker and workflow: one waiting listing per `wait` (20 s) and one plain listing per `max_idle` (5 min on a waiting store in `service`), 138,240 requests a month against 86,400 on S3 at 30 s. Measured with `wait` 1 s and `max_idle` 3 s: 6 waiting listings and 2 listings in 6 s.
- **Latency.** A start or message from another process is seen as soon as it lands, instead of within `max_idle`. Measured: with `max_idle` 1 h, a run started by another client ran at once.
- **`result` waits instead of polling.** The client reads the head and, while the run has not ended, lists the head's key alone with a wait of 0 s for the listing's etag, reads the head again, then holds a waiting listing on that etag, sent again every 20 s: the listing's etag covers the ETags of the keys it lists, so each commit, a cas of the head, ends the wait, and the client reads the head again, 1 GET per commit. A wait that ends on the same etag reads nothing. A chain removed by its expiry changes the listing too, and the read raises `RunNotFound`. An hour waited on a run that commits nothing: 2 GET and 181 listings, 1 for the etag and 180 held.
- **A connection per held listing.** A worker holds one per workflow it serves, and a client one per `result` waited on. The adapter's connections are not capped in any host, so its other requests never queue behind them.
- An h3 server is self-hosted: its requests are not billed per request by AWS. What reaches its backends depends on the server.

## Redis

A Redis store performs the operations of every table above, each answer waiting for fsyncs on the primary and its replicas: its commands, latency, memory and failovers are in [Redis stores](redis.md).

## Keeping this document true

`rust/tests/costs.rs` runs every row of this document against a store that counts operations by kind, creates and cas apart, and measures round trips from their timing, and asserts the numbers above. A change of the core or of a host that changes a count fails it.

- `cargo test --test costs` checks.
- `COSTS_MEASURE=1 cargo test --test costs -- --nocapture --test-threads 1` prints every measured row without asserting, to update this document and the test together.
- `COSTS_TRACE=1` adds each worker operation with its key, to explain a row.
- Time-dependent counts (idle scans, `result` polls, renewals, waiting listings) are measured over a controlled window and checked within bounds; their formulas are above. The monthly examples are arithmetic on the measured rows and the prices above.
