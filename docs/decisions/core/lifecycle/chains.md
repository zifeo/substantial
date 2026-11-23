# Chains, reads by id and ends

Part of [a run's lifecycle](../lifecycle.md). A chain is a run and every run it continued into, under its first run's id (I-12). Its current run lives whole in its head, the one object of the chain every commit rewrites.

A chain is one head, rewritten at every commit, so a continuation replaces the run the head holds by the next one in the commit that ends it. The run before must stay readable by its id, so it is archived from the bytes that commit fixed, the head written first: an archive written before could hold a segment a competing commit replaced. A later run's id must find its run while it is current, so it points to its chain. The next run's first wakeup comes before the head names it, so the chain never lacks one: four writes in all.

**§ <a id="db0mrbxkk">db0mrbxkk</a> A run that continues as new writes the next run's first due entry, then its chain's head holding the next run with its end, then the archive of the run before and a pointer from the next run's id to its chain, and carries the messages it has not consumed and the runs awaiting the chain.**

- (a) Naming the run's own workflow with a version continues into that version, and continuing as itself keeps the run's version; another workflow runs with the version it was declared with, or none.

Every run must stay readable by its id for as long as it is kept, and a chain read without a listing, whatever it continued: the head holds the current run, a later run's id points to its chain while it is current, and a run another replaced is read from its archive.

**§ <a id="fw4aw2ath">fw4aw2ath</a> Every run is read by its id without a listing: a chain from its head in one get, a later run through its pointer to its chain's head while it is current, and a run another replaced from its archive.**

**§ <a id="e93tpdr4p">e93tpdr4p</a> A run's end writes its chain's head with the end and its own retention as its expiry, then tells every run awaiting its chain and its parent, as messages.**

- (a) A run awaiting a chain registers under it, `watch/<chain>/<its own chain>`, then reads the chain's head, so an end written before is answered at once and one written after tells it; both are recorded and a replay never reads them again.
