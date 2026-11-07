# Testing workflows

`s.testing.Env` is a `Substantial` for tests: a namespace in memory, on a virtual clock, whose workers run in the test's process and move only when the test says so. A month with nothing due passes in milliseconds, a test never sleeps to wait for a worker, and a crash, a paused pod or a store outage is one call.

```python
from datetime import UTC, datetime

import substantial as s


@s.workflow
async def debounce(c: s.Context, key: str) -> list[int]:
    events: list[int] = []
    c.handle("event", events.append)
    seen = 0
    while await c.wait_until(lambda: len(events) > seen, timeout=s.minutes(5)):
        seen = len(events)
    return events


async def test_debounce() -> None:
    async with (
        s.testing.Env.open(now=datetime(2026, 11, 6, 9, 0, tzinfo=UTC)) as env,
        env.worker([debounce], preset="service"),
    ):
        run = await env.start(debounce, {"key": "k"}, id="k")
        await run.send("event", 1)
        await env.advance(s.minutes(4))
        await run.send("event", 2)
        await env.advance(s.minutes(4))
        assert await run.status() == "suspended"     # 4 minutes after the last event
        await env.advance(s.minutes(1))
        assert await run.result() == [1, 2]          # 5 minutes after it
        await env.check()
```

## The environment

- `Env.open(now=)` opens a fresh namespace in memory whose clock starts at `now`. There is no store to choose: a test makes no S3 or Redis request and writes no file, so it costs nothing and runs anywhere, in CI as on a laptop. The clock counts whole milliseconds, as the store's times do: a `now` with a finer part (a parsed timestamp with microseconds) raises `InvalidArgument` rather than being cut. Leaving `async with` kills its workers, as a SIGKILL would, interrupting their steps in flight, waits up to 10 real seconds for the step bodies running in a thread (a Python plain `def`, a Rust `step_blocking`) to return, and drops the namespace; a body still running then is left to run, so that a hung step fails the test with the `Stuck` of its settle rather than hanging it.
- `env` is a `Substantial` (I-16 to I-18): `env.worker`, `env.start`, `env.run` and the handles they return. `env.client()` opens another `Substantial` on the same namespace and clock, as another pod or the application under test would hold; like `Substantial.open`, await it or `async with` it. Hand it to an ASGI app whose lifespan starts its own worker and closes its client.
- `env.now` is the virtual time. `c.now()`, every timer, timeout, claim, backoff and retry of the environment read it. Code inside a step that reads the wall clock reads the real one: give a step the time it needs as an argument, from `c.now()`.
- `c.random()`, `c.uuid()`, run, message and worker ids are drawn from the environment's seed (`Chaos`'s, else 0) and from what they name: a worker's id from its place among the test's `env.worker` calls and how many times Chaos replaced it, a run's random from its run. The same test makes the same ids, whatever order the workers act in.

## Time

Nothing runs between two calls of the test. Only `settle` and `advance` let workers work and the clock move:

- `start`, `send`, `cancel`, `status` and `read` act on the store and return. As in production, a `Substantial` wakes the workers it started at once on its own `start`, `send` and `cancel`, and every other worker finds the run at its next scan, up to `max_idle` later; the woken worker acts at the next `settle`. So two `send` calls in a row reach the run together, and `await env.settle()` between them has the run see the first before the second exists.
- `await env.settle()` lets every worker do all it can at the current time, and returns once nothing can happen without the clock moving: every run waits on a timer, a message, its end or a held step, and no replay, step or store operation is in flight.
- `await env.advance(d)` settles, then moves the clock forward by `d`, stopping at each moment a worker has something due (a run's timer, a scan, a claim renewal, a backoff, a step's timeout), settling there before moving on. A timer fires at its due instant, before the test's next call: a step whose timeout is due at 5:00 and is still held then has timed out, and a `release` after `advance` reached 5:00 comes too late. A cancel that reaches the replay in which a timer fires wins, and the run ends `cancelled`: sent from another pod 1 ms before the timer, it is read with the timer; sent once `advance` reached the timer's instant, it finds the run ended and raises `RunEnded`.
- `await run.result()` settles, then returns the value if the run ended, and raises `s.testing.Stuck` naming what the run waits on otherwise: a timer and its due time, a message, a workflow no worker serves. `result(timeout=d)` raises `WaitTimeout` instead, at once, as it would once `d` passed. Neither moves the clock: advance it to where the run ends. Application code that polls in a loop of its own cannot run inside the environment.
- **A step cannot wait for the environment.** `result`, `settle` and `advance` called from a step body raise `Stuck` at once, because the settle they start waits for that step to return. A step that needs another run's value in production (it starts a run through a client and awaits its `result`) is written for the environment as a workflow that starts the other run as a child with `c.start` and awaits its result, or that gives the step the value as an argument.
- **The timing is production's.** Workers keep their settings: a worker wakes its own runs at their timers, finds a run another worker suspended or started at its next scan, renews its claims every `claim_ttl / 2`, and a crashed worker's runs are taken over 1 ms past `claim_ttl + claim_skew` after its last renewal. A child started with `c.start` is found at once by the worker that started it, and an ended run is collected after its `retention` (I-16), so a test that advances past it reads `RunNotFound`. Store operations take no virtual time; a worker's retries wait their backoff in virtual time, a client's retries take none.
- **Idle time is cheap, a wake is not.** A scan that cannot find anything is skipped, so a month of an idle worker takes milliseconds. Each wake of a run costs a replay in the core, of the order of 10 to 30 ms in Python: a nightly job takes seconds for a year of virtual time, an actor applying an operation a day about 10 s. A replay reads the run's history since its last compression, so `continue_as_new_after=0` (I-19) makes a long test several times faster: about 7 s instead of 40 s for the nightly job. Under Chaos a scan is skipped only when the fault draw of each of its listings succeeds, drawn as the scan would draw it, so the instants stay those of a test that skips nothing; a scan whose draw fails runs, and its backoff moves the scans after it. At a `rate` of 0.05, a month of an idle worker under Chaos takes about 3 s in Python with `service` and 90 s with `script`, whose worker scans every second (TypeScript 0.3 s and 6 s, Rust 1 s and 25 s in a debug build). In an outage no scan is skipped, as every scan fails.
- **Nothing hangs.** `settle` raises `Stuck` naming what is still active when a step body has not returned after 10 real seconds, or when runs keep waking each other without the clock moving, naming the run replayed more than 100 times in one settle. A synchronous TypeScript step that never returns blocks the event loop, and nothing can report it.

## Workers and steps

```python
async with (
    s.testing.Env.open(now=t0) as env,
    env.worker([checkout], preset="service"),
    env.worker([checkout], preset="service"),          # another pod on the same namespace
):
    charge = env.hold("charge")
    run = await env.start(checkout, order)
    await env.settle()
    assert charge.held == 1                            # its body returned: the card is charged
    charge.crash()                                     # its pod dies before recording it
    await env.advance(s.seconds(36))                   # past claim_ttl + claim_skew: the other pod takes over
    await env.advance(s.minutes(1))
    assert (await run.result())["payment"] == "pay-a-42"
    assert len(psp.charges) == 2                       # it ran again: its effect must be idempotent
```

- `env.worker(workflows, preset=, **settings)` is a worker on the namespace, started by `async with` as `Substantial.worker` (I-18). Several stand for several pods.
- `w.crash()` kills a worker at once, as a SIGKILL would: its claims stay until they expire, and its steps in flight are abandoned. An abandoned step's body is interrupted as in production (I-6): Python cancels an `async` body at its `await`, Rust drops it at its `.await`, TypeScript aborts its `s.stepSignal()`, and `settle` no longer waits for it. A body in a thread runs to its end, its outcome dropped, and `settle` waits for it, up to `Stuck`.
- `w.pause()` freezes a worker as a long garbage collection or a network partition would: it stops renewing its claims and reaching the store, while its steps' bodies go on to their end. `w.resume()` lets it go on from where it was, past whatever happened meanwhile.
- `await w.shutdown()` settles, then leaves as a SIGTERM does: steps that returned are recorded, held ones are abandoned, and the runs it leaves have their claims deleted and their wakeup kept, for another worker.
- `h = env.hold(step, times=1)` holds the next `times` attempts of `step` (its name, or `"<step>.undo"` for its undo) once their body has returned or raised: the worker does not see the outcome until the test decides. A held attempt does not stop `settle` or `advance`.
  - `h.held` is the number of attempts held now.
  - `h.release()` hands their outcomes to their workers.
  - `h.crash()` crashes the workers that hold them.
  - Otherwise a held attempt times out like a hung step once the clock reaches its timeout (I-18).
- A deploy: leave the old worker's `async with` (or `await w.shutdown()`), then start `env.worker([checkout_v2], preset="service")`.

## Failures and faults

- `await env.check()` raises `s.testing.Unchecked` naming every run of the namespace, started by the test, the application or a workflow, that ended failed without anyone reading it: the test through `env` (a `result()` that raised its failure, or a `status()` that returned `failed`), or a workflow of the namespace (a parent whose `result()` of its child raised the failure, handled or not). A run collected by its retention before `check` is still named. End every test with it, or call it from the fixture that opens the environment, so a failure in the background fails the test.
- `env.outage(d)`: for the next `d` of virtual time every store operation fails. Workers retry their store operations, waiting their backoff in virtual time, and a client call raises `StoreError` at once, after its retries. A worker's operation that still fails after its retries, a few seconds (I-26), drops the replay it served: a step that returned then has its outcome lost and runs again once the store is back and the claim lapsed, as after a crash. A step returned at the start of a 10 s outage runs again at about 40 s; one returned during a 5 s outage is recorded once the store is back.
- `Env.open(now=, chaos=s.testing.Chaos(seed=7, rate=0.05))` faults the workers, not the clients: each worker store operation fails with probability `rate`, half of the failed creates applied anyway with their answer lost, and each step attempt that returns crashes its worker with probability `rate`, a fresh worker of the same workflows and settings taking its place. A fault is drawn from the seed and the operation itself (its kind, its key, the worker, and how many times that worker tried that operation on that key), not from its place in a sequence: the same seed replays the same faults whatever the workers' order, as long as the steps do the same thing, and a replaced worker draws afresh, so a run always ends. `env.faults` counts what was injected (`failed`, `ambiguous`, `crashed`), so a test can require that a seed injected something; `failed` counts the failures not applied and `ambiguous` the creates applied with their answer lost, never the same one twice.

## In TypeScript and Rust

```ts
await using env = await s.testing.Env.open({ now: new Date("2026-11-06T09:00:00Z") });
await using worker = env.worker([debounce], { preset: "service" });
const run = await env.start(debounce, { key: "k" }, { id: "k" });
await run.send("event", 1);
await env.advance(s.minutes(5));
expect(await run.result()).toEqual([1]);
```

```rust
let env = s::testing::Env::open(t0).await?;                  // feature "testing"
let worker = env.worker(debounce).preset(s::Preset::Service).start()?;
let run = env.start(debounce, Key { key: "k".into() }).id("k").await?;
run.send("event", 1).await?;
env.advance(s::minutes(5)).await?;
assert_eq!(run.result().await?, vec![1]);
```

The same names in every host: `Env.open`, `client`, `worker`, `crash`, `pause`, `resume`, `hold`, `held`, `release`, `outage`, `settle`, `advance`, `now`, `check`, `Chaos`, `faults`, `Stuck`, `Unchecked`. TypeScript takes options as objects (`env.hold("charge", { times: 2 })`, `new s.testing.Chaos({ seed: 7, rate: 0.05 })`).

- In TypeScript `s.testing` comes with the library, `import * as s from "@substantial/substantial"`, and a test is an async function of any runner. Node 24 or later runs `.test.ts` files as they are: `node --test` runs this repository's (`npm test`). A `Date` holds whole milliseconds, as the clock does.
- In Rust `s::testing` is behind the feature `testing`, and a test is a `#[tokio::test]`.

- In TypeScript a worker runs from its creation, as `Substantial.worker` does, and `await using` only shuts it down. Leaving `await using` on an error cannot see the error, so it settles and shuts the worker down rather than crashing it; if that fails too, JavaScript throws a `SuppressedError` holding both.
