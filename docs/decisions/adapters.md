# Store adapters

Each store meets [the store contract](store.md) its own way (I-16, I-26). This note holds what a user or an operator relies on per store: how a URL names it, how credentials reach it, and the mechanism each adapter uses to make its create exclusive and durable. The mechanisms are laws because the contract rests on them and they are easy to simplify away: `O_EXCL` looks equivalent to `link()` but is not reliable on NFS (I-26, per-store table), and a Redis `SET NX` is acknowledged before any disk sees it (I-26 "Durability"). The URL table, the status codes of each store and the timeouts of each request are in the reference (I-16, I-26).

## Naming a store

A store is its URL, and the URL with `credentials` is all that defines it (I-16). The TLS libraries still read the variables of their certificate authorities, as the reference lists, and which other variables an adapter's libraries may read is not decided yet.

**§ <a id="e9ern38bz">e9ern38bz</a> A store is named by its URL alone, its scheme picking the adapter, and the URL with `credentials` define it: no profile or configuration file is read.**

- (a) An unknown scheme raises `ConfigError` naming the supported ones.

- (b) Every part the URL table shows is required, and an unknown query parameter raises `ConfigError`.

- (c) `credentials` is required by `s3://` and `h3://`, and refused by `memory://` and `file:` with `ConfigError`.

- (d) `memory://<ns>` names one namespace shared by every `Substantial` of the process that opens that name.

Temporary keys from a role or a web identity expire. Refreshing before expiry rather than after a refused request keeps requests from failing at every rotation (I-16, the credentials provider). The margin is in the reference.

**§ <a id="kesrnhfsp">kesrnhfsp</a> A credentials provider is called at open, and again before a request made shortly before the last keys' `expires_at`, so keys are refreshed before they expire, and keys without one are never refreshed.**

- (a) A provider that raises fails that request as a store failure, and the next request calls it again.

- (b) `"env"` reads the store's standard variables, and a missing one raises `ConfigError` at open.

- (c) On a Redis store a password provider is called at the first connection, and again by any connection opened shortly before the last password's `expires_at`, and connections already open keep theirs.

## File system

**§ <a id="c7pz5rk26">c7pz5rk26</a> The file store creates a key by writing a temporary file in `<root>/.tmp/`, fsyncing it, `link()`ing it to the key and fsyncing the directory, so a create is exclusive on NFS too, where `O_EXCL` is not reliable.**

- (a) Parent directories are created and fsynced before the link and never removed, so no `rmdir` races a `link`.

## S3

**§ <a id="gvqb04830">gvqb04830</a> The S3 adapter creates with a conditional `PutObject`, `If-None-Match: *`, so the store itself refuses a second create of a key.**

- (a) A missing key is `NoSuchKey` only: a missing bucket is a store failure.

- (b) A request that hangs is an ambiguous outcome, not a stuck worker: an attempt times out after 5 s to connect and 60 s in all, which a waiting listing of at most 20 s fits with 40 s to spare.

- (c) On `h3://` a waiting listing sends `x-substantial-wait-seconds` and the previous etag, and an answer without `x-substantial-listing-etag` is a store failure.

## Redis

Redis acknowledges a write before any disk sees it, and a failover promotes a replica that may not hold it. Either would let a second create of one segment succeed. So a create is acknowledged only once it is fsynced on the primary and on every replica required, and every read waits for a fence to be fsynced, so what a client saw never comes back changed after a failover (I-26 "Durability", "Reads and deletes").

**§ <a id="htbc4k715">htbc4k715</a> A Redis create is acknowledged only once `WAITAOF` reports the primary and `replicas` replicas fsynced it, and a `WAITAOF` short of that count is an ambiguous create.**

- (a) Every `get` and `list` sends a fence with its command, and a `delete` writes one when the key is missing; each then sends `WAITAOF` on the same connection and answers once its write is fsynced on the primary and the replicas.

- (b) AOF off, a `maxmemory-policy` other than `noeviction`, a server without `WAITAOF`, a URL scheme that does not match the server's mode, and a replica named by `redis://` raise `ConfigError` at open.

- (c) On Sentinel and Cluster `replicas` is only `all`, and a primary with no replica online acknowledges no create.

- (d) Every key of a namespace carries the hash tag `{<ns>}`, so a namespace lives on one slot.

- (e) A create and a delete are the scripts `redis/create.lua` and `redis/delete.lua`, shared by every host, and each writes the fence when it finds the key already there or already gone, so the `WAITAOF` after it covers another writer's create or delete not yet fsynced.
