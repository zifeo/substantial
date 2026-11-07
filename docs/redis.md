# Redis stores: operations and costs

A Redis store performs the operations of every table of [the architecture](architecture.md), the core deciding them as on S3; its listings wait, as on `h3://`. Its rules are in I-26 of [the interfaces](interfaces.md). What each operation costs in commands and round trips:

| Operation | Commands | Round trips |
|---|---|---|
| create | `EVALSHA` of `create.lua` (`SET NX`, `ZADD`, `PUBLISH` or `SPUBLISH`; `INCR` of the fence when the key exists), then `WAITAOF` on the same connection | 2 |
| get | `GET` and `INCR` of the fence, then `WAITAOF` | 2 |
| list | `ZRANGE BYLEX LIMIT`, any number of keys, and `INCR` of the fence, then `WAITAOF` | 2 |
| waiting listing | as a list of `max_keys + 1` keys, then a wait on the store's subscription, and a list again for each key landing under its prefix | 2, and 2 per wake |
| delete | `EVALSHA` of `delete.lua` (`DEL`, `ZREM`, `PUBLISH` or `SPUBLISH`; `INCR` of the fence when the key is missing), then `WAITAOF` | 2 |
| ambiguous create resolved | as a get | 2 |
| connection to a primary, once per pool | the primary asked from a sentinel or a cluster node, `INFO`, `WAITAOF`, `SCRIPT LOAD` of each script | 4, and 1 on Sentinel and Cluster |

A wakeup of 20 operations thus waits for about 40 round trips, half of them for fsyncs on the primary and its replicas: every answer is fenced, so that what a client saw, a key or its absence, never changes in a failover (I-26).

**Latency.** Measured through the adapter of each host: 300 rounds of each operation, interleaved, 100-byte values, a listing of 10 keys and an empty one, Valkey 8.1 in Docker on macOS with `appendfsync always`, from the same machine, as `dev/redis*.compose.yaml` runs it; p50 / p99 in ms. Each host was measured in a run of its own on a shared, loaded machine, Rust as a debug build: compare p50s, as p99s follow the machine's load.

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

**Memory.** Everything a namespace keeps is in memory: ended runs until their collection, 30 days after their chain ends by default (see [Collection](architecture.md#collection) and [Storage](architecture.md#storage)). Measured 4 times on 500 runs of 3 steps, a sleep and an end: 1.8 to 2.7 KiB of `used_memory` per run, for its 5.6 keys on average (`start`, the segments, `result`): 268 bytes of values, 815 bytes of objects with their overhead (`MEMORY USAGE`), about 600 bytes of index entries, and the server's own tables. A million such runs take 2 to 3 GB. Hints (claims, due entries) are deleted as on any store.

**Idle.** Per worker, workflow and partition, one waiting listing per `wait` (20 s), two round trips and an fsync each; one subscription per store, which costs a `PING` after 10 s without a notification; with `replicas=all`, an `INFO` every 10 s while the store is used, to count the replicas. Nothing is billed per request.

**Failover.** Tested with Sentinel (a primary, 2 replicas, 3 sentinels) and a cluster (3 primaries, 3 replicas), killing the primary of the namespace during a stream of creates, each key created by two writers at once: every create acknowledged before or during the failover holds its bytes afterwards, none was acknowledged twice, and every key answered `exists` was there. Operations fail with `StoreError` while no primary answers, 2 to 4 s from the kill to a promotion with `down-after-milliseconds` of 1 s or a `cluster-node-timeout` of 2 s, and while the promoted primary has no replica online: on the test cluster, with one replica per primary, until the killed node is back. A primary that acknowledged creates alone would lose them: sentinels quick to suspect a new primary have promoted, 2 s after the first failover, a replica still syncing from it.
