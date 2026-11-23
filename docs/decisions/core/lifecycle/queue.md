# The queue of a chain

Part of [a run's lifecycle](../lifecycle.md). A chain keeps its id through its continuations, so its wakeups and its messages are named by the chain, not by the run, and they wait in the queue of the workflow of its current run, which every worker serving that workflow lists ([Architecture](../../../architecture.md#several-workers)). A message is due at once, whatever its sender's clock: a sender whose clock runs ahead must not delay what it sends.

**§ <a id="d9vmk6y6y">d9vmk6y6y</a> A chain's wakeups and messages are keys `queues/<workflow>/due/<time>~<chain>~<tag>` under the workflow of its current run, sorted by time, and every worker serving the workflow lists them, nothing splitting them between workers ahead of time.**

- (a) A worker lists the first page of every workflow it serves at each scan, and past a full page of entries all due that it cannot take, so the runs it skips do not hide the runs behind them.

- (b) A run that waits on messages alone has no due entry: its first such wait creates its rest marker, `queues/<workflow>/rest/<chain>`, which only `runs` lists, and its end deletes it.

- (c) A run whose workflow no worker serves waits, `pending`, with no error.

- (d) A worker takes the runs of the versions it serves of each workflow, and the runs without a version only when it serves them, `unversioned`.

- (e) A message's entry is due at once for every worker, whatever its sender's clock, and sorts before every timer of its workflow.
