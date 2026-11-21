# Logs, progress and observing

A run is replayed many times, and each replay walks code that already ran, so the host writes the code's logs only while the replay is live ([k3zma9hjs.b](hosts.md#k3zma9hjs)): "muted during replay, written once" ([Interfaces](../interfaces.md#i-14-logs-and-progress)). Which logger each host writes to, and the one warning a worker logs when a run ends failed or cancelled, are in the same section. Progress a client reads while the run goes on is a published value, stored, not a log: `c.publish` "makes a value readable by clients while the run goes on" (same section). Metrics are left to the service's own library: `observe` is "a plain function the host calls for each event, with no metrics library in any host" ([Interfaces](../interfaces.md#i-29-observing)).

## Observing

**§ <a id="yp7fv5sa7">yp7fv5sa7</a> `observe` is called synchronously, on the worker's or the client's path before its next operation, for each live store operation, step attempt, replay and scan, with the same event names and fields in every host.**

- (a) An attempt is emitted for each run of a step's code, and a store operation by the process that made it.

- (b) An observer that raises stops the worker with that error, as any fatal error does, and fails the client call it was called for.

A `StoreOp`'s `attempts` is what its adapter reports ([zemxe46k3.a](store.md#zemxe46k3)), and what it costs is in [Cost](cost.md).
