# The test environment

`s.testing.Env` is a `Substantial` on a namespace in memory and a virtual clock, whose workers run in the test's process and move only when the test says so: "a month of timers in a fraction of a second, a crash, a paused pod, a store outage or Chaos in one call" ([Interfaces](../interfaces.md#i-19-testing)). So "a test never sleeps to wait for a worker" ([Testing](../testing.md)), and its workers keep their settings: "The timing is production's" ([Testing](../testing.md#time)). The environment is host code, written in each host with "The same names in every host" ([Testing](../testing.md#in-typescript-and-rust)). What each of its calls does, `outage`, `crash`, `pause`, `hold`, `check`, and the bounds of `Stuck`, is in the testing guide and the reference (I-19).

## The real core

An environment "is a `Substantial` on a namespace in memory and a virtual clock" ([Interfaces](../interfaces.md#i-19-testing)), and "Each wake of a run costs a replay in the core" ([Testing](../testing.md#time)): the runs a test serves are replayed by the same core as in production, so the protocol a test exercises is the one that ships.

**§ <a id="bsw9tpftr">bsw9tpftr</a> An environment is opened as a `Substantial` is, on the core module production loads, so its runs are replayed by the same protocol, its namespace in memory and its clock virtual.**

## The clock

**§ <a id="x55x5yqy4">x55x5yqy4</a> An environment's clock moves only when the test settles it, advances it or drains a worker, and its workers keep production's settings and timing on that clock.**

- (a) `start`, `send`, `cancel`, `status` and `read` act on the store and return, and nothing runs between two calls of the test.

- (b) `advance(d)` stops at each instant a worker has something due and settles there, so a timer fires at its due instant before the test's next call.

- (c) A step that reads the wall clock reads the real one, and `c.now()` the virtual one.

- (d) `drain(within=)` on an environment's worker settles, then moves the clock from one due instant to the next until the worker stopped, and raises `Stuck` if the worker is paused.

## Seeds and faults

**§ <a id="hz0ve24df">hz0ve24df</a> In an environment, `c.random()`, `c.uuid()`, and run, message and worker ids are drawn from its seed and from what they name, so the same test makes the same ids whatever order its workers act in.**

- (a) A `Chaos` fault is drawn from the seed and the operation itself, its kind, its key, its worker and its attempt, so the same seed replays the same faults.

- (b) `Chaos` faults the workers, not the clients.
