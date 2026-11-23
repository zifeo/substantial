# Redis stores: operations and costs

A Redis store performs the operations of every table of [the architecture](architecture.md), the core deciding them as on S3; its listings wait, as on `h3://`. Its rules are in I-26 of [the interfaces](interfaces.md). What each operation costs in commands and round trips:

| Operation | Commands | Round trips |
|---|---|---|
| create | `EVALSHA` of `create.lua` (`SET NX`, `PX` with an expiry, `ZADD` to the index and to the expiries, `HSET` of the value's ETag in `{ns}:t`, up to 16 expired keys' members removed from the index, the expiries and `{ns}:t`, `PUBLISH` or `SPUBLISH`; `INCR` of the fence when the key exists), then `WAITAOF` on the same connection | 2 |
| compare-and-set | `EVALSHA` of `cas.lua` (`GET` and `redis.sha1hex` compared with the ETag, `SET`, `PX` with an expiry, the expiries updated, `HSET` of the new ETag in `{ns}:t`, up to 16 expired keys' members removed, `PUBLISH` or `SPUBLISH`; `INCR` of the fence on a mismatch), then `WAITAOF` | 2 |
| get | `GET` and `INCR` of the fence, then `WAITAOF` | 2 |
| list | `ZRANGE BYLEX LIMIT`, any number of keys, and `INCR` of the fence, then `WAITAOF` | 2 |
| waiting listing | as a list of `max_keys + 1` keys, then `HMGET` of the page's ETags in `{ns}:t`, the page's etag a digest of its keys, their ETags and whether more follow, so it changes when a key under it is added, removed or rewritten; then a wait on the store's subscription, and a list again for each key written under its prefix | 3, and 3 per wake; 2 for an empty page |
| delete | `EVALSHA` of `delete.lua` (`DEL`, `ZREM` from the index and the expiries, `HDEL` from `{ns}:t`, `PUBLISH` or `SPUBLISH`; `INCR` of the fence when the key is missing), then `WAITAOF` | 2 |
| ambiguous create or compare-and-set resolved | as a get | 2 |
| connection to a primary, once per pool | the primary asked from a sentinel or a cluster node, `INFO`, `WAITAOF`, `SCRIPT LOAD` of each script | 4, and 1 on Sentinel and Cluster |

A wakeup of 20 operations thus waits for about 40 round trips, half of them for fsyncs on the primary and its replicas: every answer is fenced, so that what a client saw, a key or its absence, never changes in a failover (I-26).

**Latency.** Measured through the adapter of each host: 300 rounds of each operation, interleaved, 100-byte values, a listing of 10 keys and an empty one, Valkey 8.1 in Docker on macOS with `appendfsync always`, from the same machine, as `dev/compose.yaml` runs it; p50 / p99 in ms. Each host was measured in a run of its own on a shared, loaded machine, Rust as a debug build: compare p50s, as p99s follow the machine's load.

One server, `replicas=0`:

| Host | get found | get missing | list | list empty | create | delete |
|---|---|---|---|---|---|---|
| Python | 1.1 / 2.8 | 1.2 / 3.2 | 1.2 / 2.7 | 1.2 / 3.0 | 1.1 / 2.3 | 1.1 / 3.2 |
| TypeScript | 0.7 / 2.1 | 0.7 / 1.6 | 0.7 / 1.8 | 0.7 / 1.7 | 0.7 / 5.2 | 0.7 / 1.7 |
| Rust | 0.7 / 1.7 | 0.7 / 2.0 | 0.8 / 1.4 | 0.7 / 1.5 | 0.8 / 2.2 | 0.8 / 1.5 |

Sentinel, 2 replicas, `replicas=all`:

| Host | get found | get missing | list | list empty | create | delete |
|---|---|---|---|---|---|---|
| Python | 1.3 / 2.8 | 1.3 / 3.4 | 1.3 / 3.3 | 1.3 / 3.2 | 1.5 / 4.3 | 1.4 / 3.9 |
| TypeScript | 0.8 / 1.8 | 0.8 / 1.9 | 0.8 / 4.8 | 0.8 / 1.7 | 0.9 / 2.5 | 0.8 / 3.5 |
| Rust | 0.9 / 5.2 | 0.9 / 3.1 | 0.9 / 2.4 | 0.9 / 6.3 | 1.0 / 3.0 | 0.9 / 1.9 |

Cluster, 1 replica per primary, `replicas=all`:

| Host | get found | get missing | list | list empty | create | delete |
|---|---|---|---|---|---|---|
| Python | 1.2 / 2.7 | 1.2 / 2.9 | 1.3 / 3.9 | 1.2 / 3.2 | 1.4 / 3.2 | 1.3 / 3.9 |
| TypeScript | 0.8 / 1.8 | 0.8 / 1.8 | 0.8 / 1.9 | 0.8 / 2.1 | 0.8 / 1.9 | 0.8 / 2.2 |
| Rust | 0.8 / 1.8 | 0.8 / 2.1 | 0.8 / 2.6 | 0.8 / 2.4 | 0.8 / 2.3 | 0.8 / 2.1 |

A `GET` alone takes 0.25 ms at p50: the fence costs 0.5 to 1 ms per operation. With `everysec`, `WAITAOF` makes the primary fsync at once, but each replica fsyncs and acknowledges once a second: a create on Sentinel or Cluster then took 990 to 1,009 ms at p50, hence `always` on every server. With the write and `WAITAOF` in one pipeline instead of two round trips, every create on Sentinel takes about 1 s, even with `always`: replicas then acknowledge their fsync only at their next second.

**Memory.** Everything a namespace keeps is in memory: ended runs until they expire, 30 days after they end by default, when Redis removes them by the `PX` their end wrote (see [Storage](architecture.md#storage)). A run that never continued keeps one object, its chain's head, with its member in the index, its ETag in `{ns}:t` and its expiry in `{ns}:e`; a chain that continued keeps an archive or a pointer per earlier run as well. Due entries, markers and consumed messages are deleted as on any store. The memory a run takes has not been measured for this layout.

**Idle.** Per worker and workflow, one waiting listing per `wait` (20 s), two round trips and an fsync each, three when its page holds keys; one subscription per store, which costs a `PING` after 10 s without a notification; with `replicas=all`, an `INFO` every 10 s while the store is used, to count the replicas. Nothing is billed per request.

**Failover.** Tested with Sentinel (a primary, 2 replicas, 3 sentinels) and a cluster (3 primaries, 3 replicas), killing the primary of the namespace during a stream of creates, each key created by two writers at once: every create acknowledged before or during the failover holds its bytes afterwards, none was acknowledged twice, every key answered `exists` was there, and creates sent after the promotion are acknowledged again, with the old primary still down on Sentinel and once it has rejoined on the test cluster. Operations fail with `StoreError` while no primary answers, 2 to 4 s from the kill to a promotion with `down-after-milliseconds` of 1 s or a `cluster-node-timeout` of 2 s, and while the promoted primary has no replica online: on the test cluster, with one replica per primary, until the killed node is back. A primary that acknowledged creates alone would lose them: sentinels quick to suspect a new primary have promoted, 2 s after the first failover, a replica still syncing from it.
