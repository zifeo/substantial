# Expiry

Part of [a run's lifecycle](../lifecycle.md). A namespace must stop growing, and an id must stay refused to a new start while the run it names may still be read or awaited: a webhook redelivered while its run is kept is applied once ([Integration](../../../integration.md#webhooks-applied-once)). Collection by a worker needed checks, rechecks and a tombstone for the id; the store removes an object by an expiry instead, written with the object, so no worker is needed and no id is ever refused longer or shorter than its run is kept (I-16). Expiry is whole days because S3 lifecycle rules count days, and the margin of 2 days is the rounding to midnight UTC and the delay of a lifecycle run ([r4rsq6spn](../../store.md#r4rsq6spn)).

Keeping a parent while a descendant lives would hold its end without expiry until its last child's notice, which a crash or a lost notice could delay for good, and would cost the parent a read of every child's head at each notice. A child needs nothing of its parent's head to run or to end, and a parent that awaits a child is live until it gets its end, so each run now counts its retention from its own end, whatever its children do.

**§ <a id="a1ys7at7p">a1ys7at7p</a> An ended run is kept, readable by its id and its id refused to a new start, for at least its retention, whole days counted from its own end whatever its children do, and it is removed at most 2 days after that.**

- (a) The store removes an ended run by the expiry its last write carries, with no request of any worker.

- (b) A run's retention is the one its end recorded, so a change of a workflow's retention reaches only the runs that end after it.

- (c) A run another replaced, by a continuation, a retry or a reset, is kept as an ended run from its replacement, and a later run's pointer as long as its chain.

- (d) A retry or a reset holds in its own head the events of the run it resumes, so it replays them whether that run is still kept or not.

A child may outlive its parent, and the parent's id may then start a new run, so a child's end or a parent's cancel can reach a chain that is gone or is no longer the one it was meant for. Neither may touch a run it was not meant for.

**§ <a id="n0ge6zsws">n0ge6zsws</a> A child's end or a cancel that reaches a chain past its retention, or a run that took its id since, is ignored without harm.**

- (a) A chain consumes a child's end only from a child it started and whose end it has not consumed, and deletes any other child's end unconsumed.

- (b) An ending parent's cancel reaches only a live child whose head names the parent's chain as its parent.

A run that takes an expired parent's id may be the same code started again, which starts the same child under the same id and awaits it. That child may have ended after the first parent expired, its end then deleted unconsumed, and the new run would wait for good. Starting the same child again is the start the first parent made, so the new run adopts it, as a redone start does.

**§ <a id="svx76xx41">svx76xx41</a> A run that took an expired parent's id and starts the child that parent started, under the same id with the same workflow, version, input and budget, adopts it as its own child and gets its end, even when the child ended before.**

Once its retention passed, a chain is gone for every caller, and a writer that read it before writes nothing under it after: a head's compare-and-set names the ETag it read, which a removed head never has.

**§ <a id="qkh68yrf1">qkh68yrf1</a> Every call that names a removed chain, `status`, `result`, `history`, `retry`, `send`, `cancel` and a workflow's `c.result`, raises `RunNotFound`, and `read` returns none.**

- (a) A writer that read a chain before its removal writes nothing under it after: its compare-and-set answers mismatch, and an entry it creates under the chain is deleted unserved.
