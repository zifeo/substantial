# The store

Substantial has no broker and no server: workers and clients coordinate through the store alone, with four operations, an exclusive create, a get, a list and a delete (README, "No broker, no server"). The store may be memory, a directory, S3 or Redis. A run's state is what the store acknowledged: a create acknowledged is durable ([Interfaces](../interfaces.md#i-26-store-adapters)). A worker may pause past its claim while its steps go on ([Integration](../integration.md#several-pods), "A paused pod"), and requests fail or lose their answers ([Architecture](../architecture.md#when-something-goes-wrong)). The commit point built on these operations is a law of [the core](core/commit.md).

## The model

**§ <a id="w89qkqkaj">w89qkqkaj</a> The store is the only thing clients and workers share, and every part of Substantial uses four operations on it: an exclusive create, a get, a list and a delete.**

An adapter may keep bookkeeping of its own beside the namespace's objects, as the Redis adapter's fence counter and key index ([htbc4k715](adapters.md#htbc4k715)); what the core reads and writes is objects.

**§ <a id="kwvhb7ehw">kwvhb7ehw</a> No object of a namespace is ever overwritten: the only write the store offers is an exclusive create, so every object is created once and read as it was created.**

## The contract a store must meet

These are the rules every adapter is checked against, the same for every store (I-26). A store that breaks one is not a store, whatever it advertises, since a second successful create of one segment is two histories of one run (I-26 "No overwrite").

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

## Retries

An outage must surface as an error rather than a hang, and the bound on a create landing is at least the adapter's attempts plus their backoff (I-26 "A create lands in time, or never"). The attempts and the backoff are in the reference's table of fixed values (I-18).

**§ <a id="zemxe46k3">zemxe46k3</a> An adapter retries a transient failure itself, a bounded number of times with a growing backoff, then fails with `StoreFailure`, and never retries a permanent one.**

- (a) It reports with each answer how many requests it sent, its retries and an ambiguous create's read-back included, at least 1.

- (b) A client call raises `StoreError` once its adapter's attempts failed.

## Scope and capabilities

**§ <a id="x2qrbh9jk">x2qrbh9jk</a> An adapter is scoped to its namespace, its keys relative to it, and declares whether its listings can wait: the URL scheme names the adapter, and nothing is detected.**

- (a) The adapter's other requests go on while a listing is held waiting: its connections are uncapped, in every host.
