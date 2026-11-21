# Workers

A worker serves the workflows it is given: it scans their queues, claims runs, replays them and runs their steps (I-18). What it scans and claims, and when, is the core's agent ([lifecycle](core/lifecycle.md)); the host owns its settings, its signals and grace, its health, and the values every worker of a namespace must agree on.

## Presets

Implicit defaults were rejected: a preset names every value, `script` for a process that runs and exits, `service` for a long-lived one (I-18: "a preset is a named set of every setting, the only defaults there are"). The value each preset gives each setting, and what each costs or buys, are in the reference (I-18) and in [Architecture](../architecture.md#settings-that-drive-cost): `max_idle` trades listings for the latency of work from another process, `claim_ttl` renewals for takeover time, `continue_as_new_after` continuations for history reads.

**§ <a id="yxy5pzfkp">yxy5pzfkp</a> A worker takes every setting from a named preset, `script` or `service`, and only its seven public settings may override it: `concurrency`, `max_idle`, `shutdown_grace`, `claim_ttl`, `continue_as_new_after`, `step_timeout` and `retention`.**

- (a) A worker without a preset, an unknown setting, or a value of the wrong type raises `ConfigError` naming it.

- (b) Every other value is the same in both presets, and no worker can change it.

"The presets fix every other value, the same in both, and no worker can change them" (I-18). A value is a clause here only with the reason beside it; the others are in the reference's table.

**§ <a id="ry26kkc7v">ry26kkc7v</a> Every worker of every host gives the core the same fixed values:**

| (a) | `commit_every` | 0 s: each step is committed as it returns |
| (b) | `claim_skew` | 5 s, added to a claim's expiry for the clocks of two workers |
| (c) | `dedup_window` | 7 days |
| (d) | `tombstone` | 7 days |
| (e) | `due_page` | 1,000 entries, an S3 page, and never more |

`commit_every` 0 is what makes a crash lose only the steps running at that moment (I-1). The 7 days of `dedup_window` and `tombstone` are the stall the tombstone covers: "while no client or worker stalls more than 7 days between reading an id and writing under it" (I-16), and a redelivery "Long after" its run was collected raises `RunExists` for those 7 days ([Integration](../integration.md#webhooks-applied-once)).

**§ <a id="pn928919v">pn928919v</a> A `retention`, a worker's or a workflow's, is at least `retention_min` and at most 100 years, 36,500 days, and any other raises `ConfigError`.**

- (a) A workflow's `retention` overrides the worker's, and versions of one name that give one worker different retentions raise `ConfigError`.

The upper bound has a reason beyond taste: a collection's time is written in its due key with a four-digit year, and every host's clock must hold it (I-18).

## Leaving and dying

**§ <a id="dh7y75pvn">dh7y75pvn</a> A worker that leaves lets the steps in flight end within `shutdown_grace`, then interrupts those still running.**

- (a) `serve()` leaves on SIGTERM or Ctrl-C in every host.

- (b) A step that returned within the grace is recorded.

- (c) In Rust a worker dropped without a shutdown stops serving as one that left.

**§ <a id="pgv9r1j12">pgv9r1j12</a> A worker whose loop dies logs the error at once, and fails with it the `result` waits in progress through the same `Substantial`, while later waits go on.**

- (a) `alive` is whether its loop runs, and `error` the error that killed it, else none.

- (b) A store that fails never stops a worker: a failed operation drops the replay and the run is tried again.

## Waking

**§ <a id="c46hgkfse">c46hgkfse</a> A start or a send through a `Substantial` wakes that `Substantial`'s workers serving the run's workflow at once, to list that workflow alone, without a request.**

## Draining

A worker a CronJob starts every few minutes needs to serve what is due and exit, with nothing listing the store between two jobs (I-18, [Integration](../integration.md#a-worker-run-by-a-kubernetes-cronjob)). Which durations `drain` refuses is in the reference (I-18).

**§ <a id="cjtegwkna">cjtegwkna</a> `drain(within=)` drains to the host's now plus `within`, starting the worker if it was not, so a process can serve what is due by then and exit.**

- (a) It raises the worker's error if the worker died.

What a drain serves and when it stops is [the agent's](core/lifecycle.md).
