# Leases and takeover

Part of [a run's lifecycle](../lifecycle.md). A lease keeps two workers from doing the same work, but it is not a lock: the commit point is the write of the chain's head ([Commit](../commit.md)), so a paused worker that wakes past its lease loses its commit.

A lease only spares a second worker the work of a step, since the write of the head decides which replay stands: a replay that starts no step does nothing a second replay would do twice but read the run and race for its head, so it runs without a lease and saves the lease's write. The lease lives in the chain's head, so taking, renewing and clearing it are writes of the head, and it costs no key of its own.

**§ <a id="j1nq6rqpf">j1nq6rqpf</a> A worker leases a run by writing its lease into the chain's head, only as its replay is about to start its first step attempt, and renews it every half `claim_ttl` from then while the replay runs; a replay that starts no step takes no lease.**

- (a) A worker that finds a live lease leaves the run until the lease expires, then scans again.

- (b) The runs of a worker that crashed are taken over within `claim_ttl` plus `claim_skew` of its last lease or renewal, by the workers serving them.

- (c) A replay that leaves the run, by a suspension, an end, a block or a shutdown, clears its lease, if it took one, in its last write of the head, so the run is resumed at the other workers' next scan.

- (d) A replay whose lease finds the head written since its read is dropped before its attempt, its wakeups kept.

A replay whose lease was taken over is stopped; from a host it reads as a replay whose commit lost ([Commit](../commit.md)).
