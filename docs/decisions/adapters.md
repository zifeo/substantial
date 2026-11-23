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

- (a) ~~Parent directories are created and fsynced before the link and never removed, so no `rmdir` races a `link`.~~

A namespace must stop growing, and every hint, due entry and message is a key deleted soon after it is written, so a directory a key leaves behind must go too. An `rmdir` fails while its directory holds anything, so it never takes a version linked into it, and a write whose link finds its directory gone makes it again: the race the clause above avoided is now harmless.

- (b) Parent directories are created and fsynced before the link, and removed by `rmdir` only once empty, so a write whose directory a sweep removed makes it again and links anew.

A replace must be linearizable with every other write of its key, in every host: Node offers no `flock`, and `flock` is not reliable on NFS, while `link()` is exclusive on both. So a key is a directory of numbered versions, and a write links the version after the one it read: of two writers that read one version, the link of one finds the other's number taken. The names are a dot and 20 digits, never a key component, so a key's directory may also hold the keys under it, as layout 2's do.

A key deleted must leave nothing behind, so its directory goes, and a key created again numbers its versions from 0. A core may hold an ETag for hours, through a long step, so a number alone could name a version of the key created again. Each create draws a random incarnation, which every version carries and every ETag names.

**§ <a id="j4b9rkhg8">j4b9rkhg8</a> The file store keeps a key as a directory of numbered versions and writes each by `link()`ing a fsynced temporary file to the number after the version it read, so a link refused because that number exists is the conditional write.**

A delete that unlinked the versions would race a write that read the latest one: its link would land after the delete and bring the key back. A delete is a write too, so the same link orders them: whichever links the next number first wins, and the other finds it taken.

- (a) A delete, and the sweep of a key past its expiry, write a tombstone as the next version, so a later compare-and-set naming an earlier version finds that number taken.

The versions a write supersedes must go, or a key grows with every commit. They go late, so that the latest version and every one written during a listing stay listed or found by name, and lowest first, so that a version still there proves none above it was removed: a write that finds its predecessor there and its successor absent knows no later version exists, and cannot link into a gap below the latest. A read or a write that took longer than the delay could miss that, so it starts again.

- (b) A version is removed only once its successor was written 10 minutes before, the lowest first, and an operation whose reads are that old reads again.

- (c) A get reads the latest version listed, then each next one it finds by name, and lists again when the version it reads was removed.

- (d) Each create draws a random 128-bit incarnation, which every version of that key carries, and an ETag is the incarnation and the version number, so a compare-and-set naming another incarnation answers mismatch.

The expiry and the incarnation travel with the value in one file, written before the link, so a reader never sees a value with another write's header. A file's time would not do: a copy or a restore resets it, which would expire data at once.

**§ <a id="qf98sbvzj">qf98sbvzj</a> The file store starts each version's file with a 24-byte header, its expiry instant in milliseconds since the epoch, 0 for none and -1 for a tombstone, then its key's incarnation, and answers a key whose latest version is past its expiry, or a tombstone, as missing.**

- (a) A sweep at open and then daily writes a tombstone over every key past its expiry and removes the versions superseded long enough, and sweeps of several processes may run at once.

A tombstone only refuses a stale write until the key is created again or its directory goes: once it goes, the incarnation refuses it. So a tombstone goes once it has been the latest version for the same delay as a superseded one.

**§ <a id="w8a4smsae">w8a4smsae</a> The sweep removes the directory of a key whose only version is a tombstone written 10 minutes before, the tombstone first, so a key deleted or expired leaves nothing in the namespace.**

- (a) Each parent directory left empty is removed after it, up to the root, the deepest first.

## S3

**§ <a id="gvqb04830">gvqb04830</a> The S3 adapter creates with a conditional `PutObject`, `If-None-Match: *`, so the store itself refuses a second create of a key.**

- (a) A missing key is `NoSuchKey` only: a missing bucket is a store failure.

- (b) A request that hangs is an ambiguous outcome, not a stuck worker: an attempt times out after 5 s to connect and 60 s in all, which a waiting listing of at most 20 s fits with 40 s to spare.

- (c) ~~On `h3://` a waiting listing sends `x-substantial-wait-seconds` and the previous etag, and an answer without `x-substantial-listing-etag` is a store failure.~~

- (d) On `h3://` a waiting listing sends Herald's `x-herald-wait-seconds` and the previous etag in `x-herald-wait-etag`, and an answer without `x-herald-listing-etag` is a store failure.

A head is rewritten at every commit, and only if it is still the value its writer read, so S3 replaces it with a conditional write. The tag rides on the same request, so setting the expiry costs nothing more.

**§ <a id="c4fyc3wyk">c4fyc3wyk</a> The S3 adapter replaces a key with a `PutObject` carrying `If-Match: <etag>`, and carries a write's expiry of D days as the tag `substantial-retention=<D>d` on the same request.**

- (a) A 412 and a 404 `NoSuchKey` answer mismatch, and a 409 `ConditionalRequestConflict` is sent again as the same request.

- (b) The ETag is the one S3 answers with the object, passed on as it came.

A run's history lives in the store alone, so an object deleted or overwritten by a mistake, an operator, a tool or a store that ignores a condition would lose a run. Bucket versioning kept the previous version of every object for that. The head layout needs none: earlier runs live in their archives, an ambiguous write is resolved by comparing bytes, and the probe of `create` and `open` refuses a store that ignores a condition. Versioning is the only undo of an operator's mistake, and it keeps a version per commit, so it is the operator's choice, with the lifecycle rules of I-16 bounding what it keeps.

**§ <a id="x79k6r9e5">x79k6r9e5</a> An S3 store, `s3://` as `h3://`, works the same on a bucket whose versioning is enabled, suspended or never enabled, and its adapter reads no versioning.**

S3 removes a key only through a lifecycle rule of its bucket, and a key tagged with a retention no rule expires would be kept forever, its id refused to a new start forever. A worker cannot write the rules, which are the operator's, so it refuses to start rather than run what never expires.

**§ <a id="cfdckf61n">cfdckf61n</a> An S3 store's expiry check raises `ConfigError` for each expiry in days that no enabled lifecycle rule of the bucket expires after those days on the namespace's prefix with the tag `substantial-retention=<D>d`, giving the JSON of the missing rules.**

- (a) Reading the rules needs `s3:GetLifecycleConfiguration`, and a refusal of it raises `ConfigError` naming that permission.

- (b) ~~A worker runs the check before it serves anything, with its own retention and every retention of its workflows, and a check that fails kills it with that `ConfigError`.~~

A `create` or an `open` that a crash stops in the middle of its probes leaves its two probe keys, which only their expiry of 1 day removes ([mgfgmd25p.g](core/history.md#mgfgmd25p)): without a rule for 1 day they would stay, two small objects per crash that nothing lists, so the worker checks that rule too, whether or not a workflow keeps its runs 1 day.

- (c) A worker runs the check before it serves anything, with its own retention, every retention of its workflows and the 1 day of the probe keys, and a check that fails kills it with that `ConfigError`.

An S3 directory bucket lists its keys in an order of its own, not in byte order, so a listing after `start_after` would skip keys ([dfd6vt90h.b](store.md#dfd6vt90h)).

**§ <a id="h1b3afst6">h1b3afst6</a> An S3 store is a general purpose bucket: a bucket whose name ends with `--x-s3`, a directory bucket, raises `ConfigError` at open.**

## Redis

Redis acknowledges a write before any disk sees it, and a failover promotes a replica that may not hold it. Either would let a second create of one segment succeed. So a create is acknowledged only once it is fsynced on the primary and on every replica required, and every read waits for a fence to be fsynced, so what a client saw never comes back changed after a failover (I-26 "Durability", "Reads and deletes").

**§ <a id="htbc4k715">htbc4k715</a> A Redis create is acknowledged only once `WAITAOF` reports the primary and `replicas` replicas fsynced it, and a `WAITAOF` short of that count is an ambiguous create.**

- (a) Every `get` and `list` sends a fence with its command, and a `delete` writes one when the key is missing; each then sends `WAITAOF` on the same connection and answers once its write is fsynced on the primary and the replicas.

- (b) AOF off, a `maxmemory-policy` other than `noeviction`, a server without `WAITAOF`, a URL scheme that does not match the server's mode, and a replica named by `redis://` raise `ConfigError` at open.

- (c) On Sentinel and Cluster `replicas` is only `all`, and a primary with no replica online acknowledges no create.

- (d) Every key of a namespace carries the hash tag `{<ns>}`, so a namespace lives on one slot.

- (e) ~~A create and a delete are the scripts `redis/create.lua` and `redis/delete.lua`, shared by every host, and each writes the fence when it finds the key already there or already gone, so the `WAITAOF` after it covers another writer's create or delete not yet fsynced.~~

- (f) A create, a compare-and-set and a delete are the scripts `redis/create.lua`, `redis/cas.lua` and `redis/delete.lua`, shared by every host, and each writes the fence when it finds the key already there, changed or already gone, so the `WAITAOF` after it covers another writer's write not yet fsynced.

- (g) A compare-and-set is acknowledged as a create is, and a `WAITAOF` short of the count after a write is an ambiguous compare-and-set.

Lua offers one digest, `redis.sha1hex`, so the ETag a client names is the SHA-1 of the value, which it computes from what `GET` answered, and the script compares it with the value it holds.

**§ <a id="rz3se15bd">rz3se15bd</a> A Redis compare-and-set writes the value only if the SHA-1 in hex of the value the key holds is the ETag named.**

Redis expires keys itself, exactly, by the server's clock, so no worker's clock is involved. It expires an object, not its member of the index `list` reads, so the scripts remove those members as they go.

**§ <a id="hxvcx7f2a">hxvcx7f2a</a> A Redis write carrying an expiry sets it with `PX` on the object and records the instant in the namespace's sorted set `{<ns>}:e`, and every create and compare-and-set removes from the index the members of up to 16 keys whose instant passed.**

A `result` wait holds a waiting listing of one key, its chain's head, which every commit rewrites in place, so an etag of a page's keys alone would never change for it. Lua cannot read an object's digest without its bytes, so the scripts keep each key's ETag beside the index, and a waiting listing reads them with its page.

**§ <a id="bdh1dmwjr">bdh1dmwjr</a> A Redis waiting listing's etag is a digest of its page's keys, of each key's ETag and of whether more keys follow, so a page changes when a key under it is added, removed or rewritten.**

- (a) The scripts keep each key's ETag in the namespace's hash `{<ns>}:t`: a create and a compare-and-set write it, and a delete and the removal of an expired key's members remove it.
