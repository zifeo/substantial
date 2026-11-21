# Children and chains

A chain is a run and every run it continued into, under its first run's id (I-12, I-17). A workflow started from a workflow is a child of the run, its parent recorded in the child's `start` (I-11). The host passes the call; the core decides the ids, the refusals and what each call records.

## Chains

**§ <a id="ng8x15zmr">ng8x15zmr</a> A client handle's `send`, `result`, `status`, `read` and `cancel` act on the chain's last run, and `result` waits through continuations.**

- (a) `c.chain` is the same through continuations.

**§ <a id="ev8h2ngd8">ev8h2ngd8</a> The state `iterate` carries is a JSON object, and anything else raises `InvalidArgument`.**

## Children

**§ <a id="yb61qtt3s">yb61qtt3s</a> A child started without an id gets an id derived from its parent's.**

- (a) An id taken by a run that is not this child, or by a chain collected less than `tombstone` ago, ends the child failed with `RunExists`, and the other run is left untouched.

- (b) Two children with the same id from one run raise `InvalidArgument`.

- (c) `child.done` turns true when the run consumes the child's end, at the same place on every replay.

**§ <a id="t53k37khf">t53k37khf</a> `child.cancel(reason)` cancels a child as a client's `run.cancel` does, returns once the message is sent, and is recorded like a workflow's send.**

- (a) A child that ended raises `RunEnded`, and a collected one `RunNotFound`.

- (b) The child's `result()` then raises `RunFailed` with the status `cancelled` and the reason.

## Awaiting a chain

**§ <a id="dgn4hcfv0">dgn4hcfv0</a> `c.result(chain)` returns a chain's value, a chain it did not start included, without polling: a chain that ended failed or cancelled raises `RunFailed`, and one that does not exist or was collected `RunNotFound`.**

- (a) A timeout raises `WaitTimeout`, and the call can be made again.

- (b) A chain that continued is followed to its last run.

- (c) Waiting on the run's own chain raises `InvalidArgument`.

- (d) Two runs awaiting one chain both get its end.
