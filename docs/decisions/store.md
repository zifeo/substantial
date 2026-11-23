# The store

Substantial has no broker and no server: workers and clients coordinate through the store alone (README, "No broker, no server"). The store may be memory, a directory, S3 or Redis. A run's state is what the store acknowledged: a create or a compare-and-set acknowledged is durable ([Interfaces](../interfaces.md#i-26-store-adapters)). A worker may pause past its lease while its steps go on ([Integration](../integration.md#several-pods), "A paused pod"), and requests fail or lose their answers ([Architecture](../architecture.md#when-something-goes-wrong)). The commit point built on these operations is a law of [the core](core/commit.md).

## The model

**§ <a id="w89qkqkaj">w89qkqkaj</a> ~~The store is the only thing clients and workers share, and every part of Substantial uses four operations on it: an exclusive create, a get, a list and a delete.~~**

The head layout keeps each chain in one object rewritten at every commit, so the store gains a compare-and-set, and an ended run is removed by the store itself once its retention passed, so every write carries an expiry. Six operations are still few enough for any store with a conditional write: `s3://` and `h3://`, Redis, a directory, memory.

**§ <a id="karttrcdb">karttrcdb</a> The store is the only thing clients and workers share, and every part of Substantial uses six operations on it: an exclusive create, a compare-and-set naming an ETag, a get answering the ETag, a list, a delete, and an expiry in whole days that every write carries.**

An adapter may keep bookkeeping of its own beside the namespace's objects, as the Redis adapter's fence counter and key index ([htbc4k715](adapters.md#htbc4k715)); what the core reads and writes is objects.

**§ <a id="kwvhb7ehw">kwvhb7ehw</a> ~~No object of a namespace is ever overwritten: the only write the store offers is an exclusive create, so every object is created once and read as it was created.~~**

An object is replaced only when its writer read it last: two workers that read one head and each write their commit must not both win, or a run has two histories.

**§ <a id="kkad6aqxp">kkad6aqxp</a> An object is replaced only by a compare-and-set naming the ETag of the value it replaces, atomically with every other operation on its key, so of the writers that read one value at most one replaces it.**

- (a) A compare-and-set on a missing key answers mismatch.

- (b) The ETag of a key changes whenever its bytes change, and the store alone makes it: a caller passes it on and never reads anything in it.

- (c) An acknowledged compare-and-set survives a crash of the store, as an acknowledged create does.

## The contract a store must meet

These are the rules every adapter is checked against, the same for every store (I-26). A store that breaks one is not a store, whatever it advertises, since a second successful create of one segment, or a second replace of one value, is two histories of one run (I-26 "A replace names what it replaces").

**§ <a id="dfd6vt90h">dfd6vt90h</a> A store's create is atomic and exclusive, and an acknowledged create survives a crash of the store.**

- (a) A get returns the bytes of a created key, or missing.

- (b) A list returns the keys under its prefix after `start_after` in byte order of the full key, at most `max_keys`, including every key whose create was acknowledged before the call started.

- (c) A list answers `max_keys` keys whenever at least that many follow `start_after`.

- (d) Deleting a missing key succeeds.

- (e) The stores supported keep every create they acknowledged, so a replicated store acknowledges a create once its replicas hold it.

The contract also requires a create, its retries included, to be applied within a bounded time of being sent, or never: S3 does not promise it, so the bound is the operator's, at least the adapter's 5 attempts of 60 s each plus their backoff (I-26).

A create whose answer was lost may have landed. Assuming either outcome would make a run start twice or never, so the adapter reads the key back. An `exists` after an ambiguous attempt of the same create may be that attempt landing late, so it is read back too (I-26 "Ambiguous outcomes").

**§ <a id="pnvmmdmz4">pnvmmdmz4</a> A create that fails without an answer is followed by a get: the same bytes mean created, other bytes mean exists, and missing means create again, so a worker never assumes it won or lost.**

- (a) An `exists` after an ambiguous attempt of the same create is checked the same way.

A compare-and-set whose answer was lost is resolved the same way, with no versioning: a lost write lands late only if the ETag it names is still current, so it lands as an on-time write would, and a writer that replaced it read the value first (I-26 "Ambiguous outcomes").

**§ <a id="dqf0ythhp">dqf0ythhp</a> A compare-and-set that fails without an answer is followed by a get: the same bytes mean written, the ETag named still current means send it again, and anything else means another writer won.**

- (a) A mismatch after an ambiguous attempt of the same compare-and-set is checked the same way.

## Expiry

An ended run is removed by the store once its retention passed, with no collection by a worker, so a write tells the store how long to keep its key. Whole days are what S3 lifecycle rules count, and the 2 days of margin cover the rounding to midnight UTC and the delay of a lifecycle run (I-26 "Expiry").

**§ <a id="r4rsq6spn">r4rsq6spn</a> A write carrying an expiry of D whole days keeps its key readable as written for at least D days after it lands, and the store removes it at most 2 days after that.**

- (a) A write without an expiry clears any expiry its key had.

- (b) Once removed, a key stays removed until a new write.

- (c) An expiry is a whole number of days from 1 to 36,500, and an adapter refuses any other as a permanent failure.

## Retries

An outage must surface as an error rather than a hang, and the bound on a create landing is at least the adapter's attempts plus their backoff (I-26 "A create lands in time, or never"). The attempts and the backoff are in the reference's table of fixed values (I-18).

**§ <a id="zemxe46k3">zemxe46k3</a> An adapter retries a transient failure itself, a bounded number of times with a growing backoff, then fails with `StoreFailure`, and never retries a permanent one.**

- (a) It reports with each answer how many requests it sent, its retries and an ambiguous create's read-back included, at least 1.

- (b) A client call raises `StoreError` once its adapter's attempts failed.

## Scope and capabilities

**§ <a id="x2qrbh9jk">x2qrbh9jk</a> An adapter is scoped to its namespace, its keys relative to it, and declares whether its listings can wait: the URL scheme names the adapter, and nothing is detected.**

- (a) The adapter's other requests go on while a listing is held waiting: its connections are uncapped, in every host.
