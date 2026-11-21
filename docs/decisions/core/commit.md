# The commit point

The store is the only shared thing: no broker, no server, no queue service, and clients and workers use four operations, an exclusive create, a get, a list and a delete ([Architecture](../../architecture.md#how-it-works)). Substantial never writes an existing key (I-26 "No overwrite"). So a replay commits by creating its segment: of two workers racing on a run, the first create wins and the other drops its work, and everything else is a hint that can be rebuilt from the histories ([Architecture](../../architecture.md#how-it-works), "The commit point is an exclusive create").

**§ <a id="czkkbyhe0">czkkbyhe0</a> A replay commits what it recorded by an exclusive create of the run's next segment `runs/<run>/log/<n>`, and of two replays of one run the first create wins.**

- (a) The replay whose create lost is told to drop its work, then reads the run again, creates the winner's effects if they are missing, and replays from the winner's history.

- (b) A claim on writing segment `n` only avoids duplicate work: the segment's create, not the claim, decides which replay's work stands.

A worker may crash after a segment, before its effects ([Architecture](../../architecture.md#when-something-goes-wrong)).

**§ <a id="wymbfvjjr">wymbfvjjr</a> The effects of a committed segment are created after it, then its marker `effects/<n>`, and a wakeup that finds a segment without its marker creates its effects again before it replays.**

- (a) Creating an effect again does it once: every effect is an exclusive create.

**§ <a id="nmpjmw8mf">nmpjmw8mf</a> Due entries, rest markers, claims and indexes are hints: a run's truth is its `start`, its segments and its `result`, and every hint can be rebuilt from them.**

- (a) A hint, a consumed inbox key, an index entry and the `start` of a creator that withdrew may be created again after they were deleted, and every reader tolerates finding one again.

- (b) The keys `log`, `snap`, `effects`, `pub`, `result` and `gone` are created again after their deletion only under an id started again after its tombstone.

- (c) A consumed inbox key a crashed worker left is deleted by the run's next replay, by a run that continued when it forwards its inbox, or by the run's collection, and is never delivered again.
