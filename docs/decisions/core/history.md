# The history and its format

A namespace can be shared by workers and clients in all three languages, as every host loads the same core (README, "One core"). So a run's history is replayed by any worker of any host, and the core alone decides what it answers.

## Replay

**§ <a id="twravbm6m">twravbm6m</a> ~~Every call of the code the history already answers returns its recorded outcome, in the order the code makes it, and a call the history does not answer fails the run with `NonDeterminism`.~~**

- (a) ~~A step is identified by its name and its occurrence, the count of earlier calls of that name in the run, and the core gives the host that occurrence with the call.~~

- (b) ~~`now`, `random` and `uuid` are calls the core records: `random` and `uuid` draw from a ChaCha20 generator seeded for each replay from the worker's seed, its run and its segment, live only, and a uuid is a version 4 UUID.~~

- (c) ~~Every answer says whether the replay is live, having walked the whole recorded history.~~

- (d) ~~The order in which the outcomes of parallel branches reach the code is recorded, so every replay gives them in the same order.~~

A divergence is a change of the code that its runs' histories do not match: failing those runs would end them for good, when rolling the change back or giving it a new version lets them finish, so a divergence blocks the run until then.

**§ <a id="rbwstdetg">rbwstdetg</a> Every call of the code the history already answers returns its recorded outcome, in the order the code makes it, and a call the history does not answer blocks the run with `NonDeterminism`: nothing is recorded, and the run is tried again after a backoff.**

- (a) A step is identified by its name and its occurrence, the count of earlier calls of that name in the run, and the core gives the host that occurrence with the call.

- (b) `now`, `random` and `uuid` are calls the core records: `random` and `uuid` draw from a ChaCha20 generator seeded for each replay from the worker's seed, its run and its segment, live only, and a uuid is a version 4 UUID.

- (c) Every answer says whether the replay is live, having walked the whole recorded history.

- (d) The order in which the outcomes of parallel branches reach the code is recorded, so every replay gives them in the same order.

Code that catches every error, as an `except Exception` around a whole workflow does, would otherwise turn a divergence into a failure it raises on purpose, ending the run for good when a rollback would have let it finish.

- (e) A divergence blocks the run whatever the code does with it, caught or not: once the code is told, every later call of that replay fails the same way, its end and a `Fail` included, so nothing is recorded.

## The stored format

`proto/substantial/layout/v1/layout.proto` opens with the reason: every object of a namespace but its marker is one protobuf message, and "the core alone encodes them, for every host, so the bytes are the same everywhere: no map fields, whose order would vary".

**§ <a id="sqbjc1adw">sqbjc1adw</a> Every object of a namespace but its marker `substantial.json` is a message of `proto/substantial/layout/v1` in protobuf binary encoding, encoded by the core alone, with no map fields, so its bytes are the same whichever host wrote it.**

- (a) ~~Layout 2 keeps the messages of layout 1 unchanged.~~

- (b) ~~The marker holds the layout and the encoding, `{"layout":2,"encoding":"protobuf"}`.~~

- (c) The marker holds the layout and the encoding, `{"layout":3,"encoding":"protobuf"}`.

**§ <a id="mgfgmd25p">mgfgmd25p</a> `open` refuses a namespace with no marker, and `create` and `open` refuse one of another layout, naming its layout and this one, so nothing migrates a namespace: the version that created it drains it.**

- (a) `create` makes the namespace, or opens it if its marker holds this layout.

- (b) ~~`create` and `open` create `probe/exclusive` before the marker, and a store that accepts two creates of it is refused: in a new namespace the probe creates it twice, requiring `created` then `exists`, and otherwise once, requiring `exists`.~~

- (c) ~~`create` and `open` then send a compare-and-set of `probe/exclusive` naming an ETag it never had, and one of `probe/deleted`, created then deleted, naming the ETag it had, and a store that writes either raises `ConfigError`.~~

- (d) A namespace written by Substantial 0.3, of layout 2, raises `ConfigError` saying so, and nothing of it is read.

Shared probe keys let two processes opening one namespace at once refuse a healthy store: on a store whose ETags are digests of the bytes, one opener's delete and another's create of the same empty key gave the deleted key its old ETag back. So each opener probes keys of its own, holding its own id.

- (e) Each `create` and `open` probes keys of its own, `probe/<probe>/`, under an id drawn at random for it and held as their bytes, so two openers at once never share a probe key, and deletes them after its probes.

- (f) A store is refused with `ConfigError` unless a second create of `probe/<probe>/exclusive` answers `exists`, a compare-and-set of it naming an ETag it never had answers mismatch, and one of `probe/<probe>/deleted`, created then deleted, naming the ETag it had answers mismatch.

- (g) Every probe write carries an expiry of 1 day, so the keys of a probe a crash stopped halfway are removed by the store.

The probes catch S3-compatible stores that ignore `If-None-Match` or `If-Match`, and stores that bring a deleted key back under a compare-and-set, as RustFS does on a versioned bucket (I-26 "Probe at start"): a deleted key brought back would let a late writer revive a run its expiry removed. No run of Substantial 0.3 is in production, so its namespaces are refused rather than migrated.

**§ <a id="bq8tt3bh7">bq8tt3bh7</a> `history` gives the events of the chain's last run, those of the failed run it retries included, as the core renders them, each with its run, segment, time, kind and fields, so every host gives the same events with the same fields.**

## Published values

Published values live in the chain's head, the latest under each key, so `read` is one get, and a continuation keeps them.

**§ <a id="fx1tv8nqp">fx1tv8nqp</a> `c.publish(key, value)` makes a value readable while the run goes on, and `run.read(key)` returns the last value any run of the chain published under that key, or none.**

- (a) A published value is readable as soon as the commit that records it lands.

## Values

**§ <a id="w5rs0gck0">w5rs0gck0</a> The core refuses a value of the code whose JSON encoding passes 64 MiB, 67,108,864 bytes, with `VALUE_TOO_LARGE` naming its size and the limit, and a call refused records nothing.**

Every value is stored in the history, and the host checks each value before it reaches the core, which checks it again (I-15).
