# Cost and budgets

On S3 a run costs its requests, and a step may cost money of its own (I-27, I-28). Counting sends no request: the counts come from the answers of the operations a run makes anyway ([Architecture](../../architecture.md#what-a-run-records-of-its-cost), "Zero requests added"). Each worker prices the requests it counts with its own prices, so a total never goes down (I-27). A limit passed is recorded in the history, so every replay raises it at the same place (I-28 "When it is checked").

## Counting

**§ <a id="czph3f9yv">czph3f9yv</a> The core counts each run's store requests from the answers of the operations the run makes anyway, so counting sends no request.**

- (a) The core counts as many requests as each answer says the adapter sent for it.

- (b) A worker's scans, the collection of ended runs and client calls belong to no run.

- (c) Each segment records the chain's cost up to it, the requests of its own commit included as planned, so a run's cost is a lower bound, exact but for what its plan did not foresee and no later segment of the same worker recorded.

**§ <a id="m0vtdttae">m0vtdttae</a> `c.cost()` is the chain's cost, not the run's: a continued run carries its chain's total to the next run, and a retry the failed run's.**

- (a) A child's total reaches its parent with its end, and the parent's cost includes it from the place the run received it.

- (b) A cancelled run's cost is the requests of its own replays, the fan-out that cancels its children apart.

- (c) `c.cost()` is recorded, so a replay gets the value the first run saw.

## Charges

**§ <a id="ap5tfnrja">ap5tfnrja</a> The spend of a step attempt is recorded with its outcome, failed and timed-out attempts included, and summed into the chain's cost by currency.**

- (a) A code outside ISO 4217, or a charge that would take the chain's total past 2^63 - 1, fails the step at once as a permanent `InvalidArgument`.

- (b) Store money is priced by each worker with its own `prices`, a request costing its share of the price per 1,000 rounded up, so a total never goes down.

- (c) A worker without `prices` counts requests and adds no store money.

- (d) Amounts are integers rendered as decimal strings in the history and in `c.cost()`, and currencies are never mixed nor converted.

## Budgets

**§ <a id="j2xzy4akb">j2xzy4akb</a> The core compares the chain's cost with its budget before a step attempt starts, before the run commits its progress or suspends, and when the code starts a child or sends a message, never while a replay walks the recorded history.**

- (a) A limit passed, `used` over `limit`, is recorded in the history with its meter, limit and use, so every replay raises it at the same place whatever its worker counted.

- (b) A run keeps its budget when it continues, and a retry takes its own, else the failed run's.

- (c) A budget meter in a currency outside ISO 4217 raises `InvalidArgument`.

**§ <a id="fkgsx3gvk">fkgsx3gvk</a> A run past its budget raises `BudgetExceeded` at every call it waits on and at every later step, child start and send, and ends failed with that type whatever its code does.**

- (a) Undos still run and `publish` works.

- (b) Its failure's details hold the meter, the limit and what was used.

## What a namespace costs

The cost of a namespace on S3 is its requests ([Architecture](../../architecture.md#monthly-cost-on-s3)). These bounds make it predictable.

**§ <a id="aqrdmj10z">aqrdmj10z</a> A wakeup reads at most one snapshot and the segments after it, at most `snapshot_every - 1`, and none a worker still holds of the run.**

**§ <a id="gnhavbn5m">gnhavbn5m</a> An idle worker costs one listing per workflow it serves per `max_idle`, whatever runs, and a suspended run sends no request until its wakeup.**

What a run costs is the sum of what its verbs and wakeups cost, and users price a workflow from the per-verb tables before they run it: the README's "a run that starts, calls 3 steps, sleeps a day and completes makes 13 PUT, 6 GET, 9 LIST and 13 free DELETE" is arithmetic on them. "The core decides every store operation, so the Python and TypeScript hosts perform the same ones", and "A change of the core or of a host that changes a count fails" the test that measures every row ([Architecture](../../architecture.md#keeping-this-document-true)).

**§ <a id="qashr34zn">qashr34zn</a> Each client verb, workflow verb and wakeup makes the store requests that the per-verb tables of the architecture document list for it, in every host, so a run's request cost follows from the verbs it calls.**
