# A run's lifecycle

This is the path of a run through the store, as the core plans it: start, due entry, scan, claim, replay, commit, effects, suspension, wakeup, end, collection and tombstone ([Architecture](../../architecture.md#how-it-works)). The host performs each operation, runs the code and keeps time; which operation comes next is the core's, the same for every host ([one-core](../hosts.md)).

## Start

A start is durable when it returns and deduplicated by the id the caller picks, so a webhook route is idempotent as long as its provider retries until it gets a 2xx and its ids are stable ([Integration](../../integration.md#what-start-and-send-guarantee), "Durable", "Deduplicated"; I-17).

**§ <a id="ct862686r">ct862686r</a> A start creates the run's `start` key by an exclusive create, then its due entry, so when it returns both exist in the store.**

- (a) A start whose id was already started for the same workflow, version, input and budget returns that run while it is kept, and creates its due entry again, so a start that stopped halfway is completed.

- (b) A start whose id was started with another workflow, version, input or budget raises `RunExists`.

- (c) From a chain's collection and for its tombstone after, a start of its id raises `RunExists` whatever the input, with `collected` and `until`, and withdraws the `start` it created.

## The queue

There is no broker, no server and no queue service: a wakeup is an empty key sorting by time under its workflow, and the workers serving a workflow share its due entries, nothing splitting them ahead, each listing all of them, a claim deciding which one replays a run ([Architecture](../../architecture.md#several-workers)).

**§ <a id="e3rcyme0p">e3rcyme0p</a> A run's wakeups are empty keys `queues/<workflow>/due/<time>~<run>~<tag>` under its workflow, sorted by time, and every worker serving the workflow lists them, nothing splitting them between workers ahead of time.**

- (a) A worker lists the first page of every workflow it serves at each scan, and past a full page of entries all due that it cannot take, so the runs it skips do not hide the runs behind them.

- (b) A run that waits on messages alone has no due entry: its first such wait creates its rest marker, `queues/<workflow>/rest/<run>`, which only `runs` lists, and its end deletes it.

- (c) A run whose workflow no worker serves waits, `pending`, with no error.

- (d) A worker takes the runs of the versions it serves of each workflow, and the runs without a version only when it serves them, `unversioned`.

**§ <a id="qsw5w98x3">qsw5w98x3</a> An idle worker scans every workflow it serves once per `max_idle`, its first idle wait a phase drawn below `max_idle` from its seed, so workers started together do not list in step.**

- (a) A wakeup the worker saw come due, a timer or a retry, starts a scan at its time.

- (b) A collection or a tombstone's end sets no timer: the first scan after it is due serves it.

- (c) On a store whose listings wait, the worker also holds a waiting listing per workflow, which returns as soon as an entry lands, and still lists once per `max_idle`.

## Claims and takeover

A claim keeps two workers from doing the same work, but it is not a lock: the commit point is the segment ([czkkbyhe0](commit.md#czkkbyhe0)), so a paused worker that wakes past its claim loses its commit.

**§ <a id="t452yredf">t452yredf</a> Before a replay a worker creates a claim on the run's next segment, `claim/<n>-<attempt>`, and creates a new attempt every half `claim_ttl` while the replay runs.**

- (a) A worker that finds a live claim leaves the run until the claim expires, then scans again.

- (b) The runs of a worker that crashed are taken over within `claim_ttl` plus `claim_skew` of its last claim or renewal, by the workers serving them.

- (c) A replay that leaves the run, by a suspension, an end, a lost commit or a shutdown, deletes its claims, so the run is resumed at the other workers' next scan.

A replay whose claim was taken over is stopped; from a host it reads as a replay whose commit lost ([czkkbyhe0](commit.md#czkkbyhe0)).

**§ <a id="rb377j7db">rb377j7db</a> An agent replays at most `concurrency` runs at once, and due runs wait while every replay slot of every worker is taken.**

It bounds replays, not steps: a replay's steps all run (Integration, "It bounds replays, not steps").

**§ <a id="bv0addfga">bv0addfga</a> An agent told to shut down starts no new replay, and the runs it leaves keep their wakeups for another worker, their claims deleted.**

## Draining

A service whose runs are all on a schedule, or that tolerates minutes of delay, can do without a worker running all the time: a CronJob starts a pod every few minutes, whose worker drains what is due ([Integration](../../integration.md#a-worker-run-by-a-kubernetes-cronjob); I-18).

**§ <a id="d0pe2x8ey">d0pe2x8ey</a> A draining agent scans every workflow it serves once, takes only entries due by its horizon, and stops once no run is in hand, no scan is in flight, and no wakeup it listed or created, nor the end of a skipped run's claim, comes by the horizon.**

- (a) It serves the children and messages its own runs create, and the runs of a crashed worker whose claim expires by then.

- (b) Its listings are its scans alone, so between two drains a namespace costs nothing.

- (c) A run due after the horizon is left to the next drain.

## Replay and commit

**§ <a id="t916k65mb">t916k65mb</a> A store operation of a worker that still fails after its adapter's retries is issued again up to `op_retries` times, then the replay is dropped and the due entry stays for a later wakeup.**

- (a) The dropped replay's claims are deleted, yet the run is tried again only once its claim's `claim_ttl` passed, not at the worker's next scan.

**§ <a id="wkryj60b0">wkryj60b0</a> With `commit_every` 0, a step's outcome is committed before the next step starts or the run waits, and a step before a suspension or the end lands in that commit.**

**§ <a id="cn55c9k5a">cn55c9k5a</a> A run whose first replay records nothing commits an empty segment, `log/0`, so `status` reads it `suspended` once a worker took it.**

## Ends and chains

A run's history grows with every step, and every wakeup reads and replays it, so a loop continues as a new run of its chain carrying only its state (I-12). A chain is a run and every run it continued into, under its first run's id.

**§ <a id="deycjjrap">deycjjrap</a> A run that continues as new creates the next run's `start`, its `index/chains/<chain>/<seq>-<run>` entry and its due entry with its end, and carries the messages it has not consumed and the runs awaiting the chain.**

- (a) The same workflow keeps the run's version; another runs with the version it was declared with, or none.

**§ <a id="hegd7d4bp">hegd7d4bp</a> A chain is followed from its first run's `result`: a chain that never continued and did not fail is read without a listing, and one that continued or failed lists `index/chains/<chain>/` to find its last run.**

**§ <a id="kqh75fg4a">kqh75fg4a</a> A run's end creates its `result`, its collection's due entry at the end plus its retention, and tells every run awaiting its chain and its parent, as messages.**

- (a) A run awaiting a chain registers under it, `runs/<chain>/watch/<run>`, then reads its end, so an end written before is answered at once and one written after tells it; both are recorded and a replay never reads them again.

## Cancellation

A cancellation is visible to the code, which may catch it and still run steps or a rollback, and undos are never cancelled, so a rollback or a saga goes on to its end (I-13).

**§ <a id="vmk2ydwta">vmk2ydwta</a> A cancel is the message `substantial.cancel`, and the run raises `Cancelled` at every call it is blocked on, then ends cancelled whatever its code does after.**

- (a) A run that never waits, a loop of steps, sees the cancellation after its next step is recorded.

- (b) A step waiting for its retry raises `Cancelled`, and undos run to their end.

- (c) A cancel that reaches the replay in which a timer fires wins, and the run ends cancelled.

- (d) A step attempt already running when the cancel lands runs to its end: its outcome is recorded and returned to the code.

**§ <a id="t873v7eg5">t873v7eg5</a> A run that ends cancelled cancels its children still running, and a child its parent does not cancel goes on after its parent ends.**

- (a) The cancelled end lists the children of each run of its chain, gets each child's `result`, and sends each running child `substantial.cancel`.

## Retry

After fixing the code or once an outage is over, a failed chain is run again with its recorded steps kept, and a step whose effect was half done keeps its idempotency key (I-17 "`retry` runs a failed chain again").

**§ <a id="vb6krfmej">vb6krfmej</a> `retry` starts a new run of a failed chain that replays the failed run's history without its failure: recorded steps return their values, the failed step runs again from its first attempt, and its `start` names the first run of its line of retries, whose step keys it keeps.**

- (a) Only a chain whose last run failed, neither by a `rollback` nor after a saga ran undos, is retried; one that completed, was cancelled or has not ended raises `InvalidArgument`.

- (b) Retries of one failed run, concurrent or repeated, start one run, and a second retry with another budget raises `InvalidArgument`.

- (c) The retry runs the failed run's workflow version and has no parent.

- (d) A collected chain raises `RunNotFound`.

A run whose failure ran undos is refused: the retry would give back the recorded results of steps whose effects were undone, and running them again would reuse their step keys (I-17).

## Status

**§ <a id="gt35c42xy">gt35c42xy</a> `status` is `pending` while no replay committed and no claim is held, `running` while a worker holds a live claim, `suspended` while the run waits on time or messages, then the run's end, and an ended run never reads `running`.**

- (a) A run that continues as new reads as its own state until its result exists, then the chain reads as its next run.

Between two commits of a running replay a run may read `suspended` for a moment (I-17).

**§ <a id="e9a24k5b9">e9a24k5b9</a> A `result` wait reads the chain's end once, then polls the `result` key of the last run it found, one key per poll, and follows the chain from that run when it continued.**

- (a) A follow of a chain collected meanwhile raises `RunNotFound`, so a wait on a collected chain ends.

**§ <a id="j08c2ntyv">j08c2ntyv</a> `runs` reads a workflow's queue, `due/` and `rest/`, never the histories, and lists each live run once, by its earliest wakeup, with what it waits on.**

A run that just ended, or one whose wakeup a crashed worker left, may still be listed until the next scan (I-17).

## Collection

Every ended run is collected once its retention passes: no setting keeps runs for good, so a namespace stops growing (I-16). Keys are never deleted by hand or by a lifecycle rule: runs still going read keys of runs that ended, and deleting them by age can make a child run twice (I-16). So a worker collects, from a due entry found by a scan like any wakeup, after checks ([Architecture](../../architecture.md#collection)).

**§ <a id="yx9pxz6mr">yx9pxz6mr</a> An ended run is collected by a worker serving its workflow's name, any version, once its retention has passed since it ended, a continued run while its chain goes on, and no sooner than twice `retention_min` after its `result` was written.**

- (a) A run is kept while a child it started is kept, while its parent is still starting it, and while an earlier run of its chain is not collected, and a collection whose checks fail tries again `recheck` later.

- (b) The first run of a chain that continued keeps its `start` and `result` while the chain lives.

- (c) A failed run is kept until its retry's first snapshot holds its history.

A collection runs without a claim, so two workers may collect one run at once, and a worker may crash halfway ([Architecture](../../architecture.md#collection)).

**§ <a id="cv078fzet">cv078fzet</a> Every operation of a collection is a delete, or a create of a key a second collector would create the same, and the served due entry is deleted last, so a crash leaves it for the next scan.**

## Tombstones

A redelivery after its event's run was collected raises `RunExists` with `collected` true for 7 days: the event was applied then ([Integration](../../integration.md#webhooks-applied-once), "Long after").

**§ <a id="ja67c3az7">ja67c3az7</a> Collecting a chain's first run creates `runs/<chain>/gone` first, holding until when its id is taken, the collection plus `tombstone`, and an entry due then, which deletes it.**

- (a) Once the collection is done, until then, `start` raises `RunExists`, and every call that follows the chain, `status`, `result`, `history`, `retry`, `send`, `cancel` and a workflow's `c.result`, raises `RunNotFound`, each with `collected` and `until`, and `read` returns none.

- (b) A crash of its collector leaves `gone` with its time, so the tombstone keeps it.

These hold while no client or worker stalls more than 7 days between reading an id and writing under it, and while workers serve due entries within 7 days (I-16).
