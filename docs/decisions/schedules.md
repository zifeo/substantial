# Polling and schedules

Two recurring patterns are calls rather than loops users write: polling a provider that offers no push ([Interfaces](../interfaces.md#i-30-polling)) and a cron ([Interfaces](../interfaces.md#i-31-schedules)). Each host builds both from the steps, sleeps and messages the core records, so a replay keeps their instants: a poll's deadline is recorded "so a replay keeps it" ([Interfaces](../interfaces.md#i-30-polling)), and a schedule's fires are recorded so a replay yields them "whatever its clock or time zone database says: a child named from `at` stays idempotent" ([Interfaces](../interfaces.md#i-31-schedules)).

## Polling

**§ <a id="c5kkpjdz5">c5kkpjdz5</a> `c.poll(name, fn, every=, timeout=)` runs `fn` as the inline step `name` at each try, sleeps `every`, or the time left if shorter, while it returns none, and returns the first value that is not none, or none once its timeout passed.**

- (a) The deadline is the recorded `c.now()` of the first call plus `timeout`, so a replay keeps it.

- (b) Each try is a call of the step `name` with the poll's `retry`, so a try already recorded never runs again and each try retries as its `retry` says.

## Schedules

A schedule is one long-lived run per schedule id, configured by the message `schedule` that `sub.schedule` sends. During a rollout, pods of two deploys run at once and each configures the schedule at startup. The run applies a config only if its revision is higher, so "a pod with an older deploy never undoes a newer one", and the same revision and config does nothing, "so every pod may call it at startup" ([Interfaces](../interfaces.md#i-31-schedules)).

**§ <a id="vnykycs34">vnykycs34</a> `c.schedule()` yields each fire time of the schedule's cron, the instant it was due and never the time it ran, as the workflow's one `iterate`.**

- (a) The fires to run are recorded in the inline step `schedule.next`, so a replay yields the same instants whatever its clock or time zone database says.

- (b) A step of the code named `schedule.next` raises `InvalidArgument`.

- (c) `cron=None` disables the schedule, and a new cron or zone fires from the instant it applies, never before.

- (d) When the run wakes after fire times passed, it yields each one, oldest first, at most `backfill` of them, the most recent, the older ones skipped and counted.

Every worker down, a deploy or an outage wakes the run after fires passed: "Missed fires are backfilled", the most recent ones, and "give `backfill=1` for a job where only the latest matters" (I-31). The default is in the reference.

**§ <a id="ey8zhz0kq">ey8zhz0kq</a> A schedule's run applies a config only if its revision is higher than the one it applied.**

- (a) `sub.schedule` starts the run if needed, with an empty input, and sends the config as the message `schedule` with an id made of its revision and a digest of its canonical JSON, the same in every host.

- (b) Once the run published a revision, `sub.schedule` with that revision and config does nothing, and one with a lower revision, or with the same revision and another config, raises `ConfigError`.

- (c) ~~A `schedule` message that does not hold exactly the config fields fails the run with `InvalidArgument`.~~

- (d) The run publishes `schedule` when a config applies, at each fire and when its next fire changes, with the config, the last fire, the most recent ones, the skipped ones and the next, the same fields in every host.

- (e) Of several configs the run received at once, the one of the highest revision applies.

- (f) ~~A config that differs from another of its revision, the one the run applied last or one received with it, fails the run with `ConfigError`, while a config of an earlier revision is ignored whatever it holds.~~

- (g) A `schedule` message that does not hold exactly the config fields blocks the run with `InvalidArgument`.

- (h) A config that differs from another of its revision, the one the run applied last or one received with it, blocks the run with `ConfigError`, while a config of an earlier revision is ignored whatever it holds.

The id dedups the same config sent by every pod, while a second config of one revision, which two deploys disagreeing on the revision send before the run applied either, reaches the run and blocks it instead of being dropped unseen, until the deploys agree. After the run published a revision, `sub.schedule` refuses such a config itself ((b)). The run keeps only the config it applied last, so it compares nothing with an earlier revision's config, which is stale whatever it holds and which `sub.schedule` refuses once a higher revision is published.

Every pod of a deploy calls `sub.schedule` at its start, and a deploy of a new version would find the old version's schedule running. The schedule moves to the new version at its loop's next boundary instead, which needs that loop to opt into upgrades ([zsyv7772c](chains.md#zsyv7772c)) and a worker that serves both versions at that boundary, as a rolling deploy has.

**§ <a id="sfzkmddwj">sfzkmddwj</a> `sub.schedule` given a newer version of its workflow than the running schedule's sends its config to the running schedule, and says that the schedule moves to that version at its next boundary that upgrades.**

## Fire times

Each host computes fire times itself, from its own copy of the IANA time zone database, and the shared vectors of `dev/cross/cron.json` keep the three in agreement. Hosts whose copies differ can compute different fire times for a zone whose rules changed recently: "keep them up to date, or give such a schedule's workflow to one host" ([Interfaces](../interfaces.md#i-31-schedules)).

**§ <a id="d08sxcdv8">d08sxcdv8</a> Every host computes the same fire times from a five-field cron and an IANA time zone: a local time a clock change skips fires at the end of the gap, and a local time it repeats fires at its first occurrence only.**

- (a) When neither day field starts with `*`, a day matching either fires.

The syntax of an expression, and what each host refuses, are in the reference (I-31).
