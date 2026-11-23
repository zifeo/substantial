# The commit point

The store is the only shared thing: no broker, no server, no queue service, and clients and workers use six operations, an exclusive create, a compare-and-set naming an ETag, a get answering the ETag, a list, a delete, and an expiry every write carries ([karttrcdb](../store.md#karttrcdb)). A chain is one object, its head, holding its current run whole, and Substantial replaces an object only by a compare-and-set naming the ETag it read ([kkad6aqxp](../store.md#kkad6aqxp); I-26 "A replace names what it replaces"). So a replay commits by writing the head with its segment: of two workers racing on a run, the first write wins and the other drops its work, and everything else is a hint that can be rebuilt from the heads ([Architecture](../../architecture.md#how-it-works)).

**§ <a id="czkkbyhe0">czkkbyhe0</a> ~~A replay commits what it recorded by an exclusive create of the run's next segment `runs/<run>/log/<n>`, and of two replays of one run the first create wins.~~**

- (a) ~~The replay whose create lost is told to drop its work, then reads the run again, creates the winner's effects if they are missing, and replays from the winner's history.~~

- (b) ~~A claim on writing segment `n` only avoids duplicate work: the segment's create, not the claim, decides which replay's work stands.~~

**§ <a id="rnx5b50bn">rnx5b50bn</a> A replay commits what it recorded by a compare-and-set of its chain's head with its segment appended, naming the ETag of the head it read, and of two replays of one run the first write wins.**

- (a) The replay whose write lost is told to drop its work, then reads the run again, creates the winner's effects if they are missing, and replays from the winner's history.

- (b) A lease only avoids duplicate work: the write of the head, not the lease, decides which replay's work stands.

A worker may crash after a segment, before its effects ([Architecture](../../architecture.md#when-something-goes-wrong)).

**§ <a id="wymbfvjjr">wymbfvjjr</a> ~~The effects of a committed segment are created after it, then its marker `effects/<n>`, and a wakeup that finds a segment without its marker creates its effects again before it replays.~~**

- (a) ~~Creating an effect again does it once: every effect is an exclusive create.~~

**§ <a id="kkcyc97cz">kkcyc97cz</a> The head of a segment with effects is written pending, its effects are created after it, then a second write of the head clears the mark, and a reader that finds the head pending creates its effects again before anything else.**

- (a) Creating an effect again does it once: every effect is an exclusive create of a key derived from the segment.

**§ <a id="nmpjmw8mf">nmpjmw8mf</a> ~~Due entries, rest markers, claims and indexes are hints: a run's truth is its `start`, its segments and its `result`, and every hint can be rebuilt from them.~~**

- (a) ~~A hint, a consumed inbox key, an index entry and the `start` of a creator that withdrew may be created again after they were deleted, and every reader tolerates finding one again.~~

- (b) ~~The keys `log`, `snap`, `effects`, `pub`, `result` and `gone` are created again after their deletion only under an id started again after its tombstone.~~

- (c) ~~A consumed inbox key a crashed worker left is deleted by the run's next replay, by a run that continued when it forwards its inbox, or by the run's collection, and is never delivered again.~~

- (d) ~~A blocked replay's key under `runs/<run>/blocked/` and its queue's marker `queues/<workflow>/blocked/<run>` are hints too: they hold the error and the next try, and the run's next replay that does not block deletes them.~~

**§ <a id="rbvr70kyv">rbvr70kyv</a> Due entries, rest and blocked markers, watcher keys and a head's block are hints: a chain's truth is its head, the runs it archived and the messages it has not consumed, and every hint can be rebuilt from them.**

- (a) A hint, and the entry of a message the chain consumed, may be created again after they were deleted, and every reader tolerates finding one again.

- (b) A head, a pointer or an archive is written again after its removal only by a new start of its id.

- (c) The entry of a message a crashed worker left after its chain consumed it, or moved it into the head, is deleted by the chain's next wakeup and is never delivered again.

- (d) A blocked run's error and next try are in its head's block and in its queue's marker `queues/<workflow>/blocked/<chain>`, and the run's next replay that does not block clears both.
