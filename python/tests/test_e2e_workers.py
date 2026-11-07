"""Workers under pressure: concurrency, shutdown, timers during steps, and two workers racing."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import timedelta

import pytest
from conftest import LONG, Recording, eventually, script

from substantial import Context, RunStatus, Substantial, Workflow, step, workflow
from substantial.stores import Store

FAST = {"claim_ttl": timedelta(milliseconds=200), "claim_skew": timedelta(0)}


class Probe:
    """A step sleeping `ms(attempt)` that counts its attempts and the most in flight at once."""

    def __init__(self, ms: Callable[[int], float]) -> None:
        self.attempts = 0
        self.in_flight = 0
        self.most = 0

        @step(name="probe")
        async def probe() -> int:
            self.attempts += 1
            attempt = self.attempts
            self.in_flight += 1
            self.most = max(self.most, self.in_flight)
            try:
                await asyncio.sleep(ms(attempt) / 1000)
            finally:
                self.in_flight -= 1
            return attempt

        @workflow
        async def probed(c: Context) -> int:
            return await probe()

        self.workflow: Workflow = probed


async def test_concurrency_bounds_the_replays_at_once(adapter: str, store: Store) -> None:
    concurrency = 1 if adapter == "memory" else 2
    probe = Probe(lambda _: 100)
    sub = await Substantial._create(store, partitions=2)
    runs = [await sub.start(probe.workflow) for _ in range(4)]
    async with script(sub, [probe.workflow], concurrency=concurrency):
        for run in runs:
            await run.result(timeout=LONG)
    assert probe.most == concurrency
    assert probe.attempts == 4


async def test_shutdown_lets_a_step_end_within_the_grace(store: Store) -> None:
    probe = Probe(lambda _: 300)
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [probe.workflow]):
        run = await sub.start(probe.workflow)

        async def started() -> bool:
            return probe.attempts == 1

        await eventually("the step", started)
        assert await run.status() == RunStatus.RUNNING
    async with script(sub, [probe.workflow]):
        assert await run.result(timeout=LONG) == 1
    assert probe.attempts == 1, "the step ended within the grace"


async def test_shutdown_past_the_grace_loses_the_step_in_flight(store: Store) -> None:
    probe = Probe(lambda attempt: 30_000 if attempt == 1 else 10)
    sub = await Substantial._create(store, partitions=1)
    settings = {"shutdown_grace": timedelta(milliseconds=100), **FAST}
    worker = script(sub, [probe.workflow], **settings)
    async with worker:
        run = await sub.start(probe.workflow)

        async def started() -> bool:
            return probe.attempts == 1

        await eventually("the step", started)
        began = time.monotonic()
    assert time.monotonic() - began < 5
    async with script(sub, [probe.workflow], **settings):
        assert await run.result(timeout=LONG) == 2


async def test_a_timer_fires_while_a_step_is_in_flight(store: Store) -> None:
    waited: list[float] = []

    @step
    async def slow() -> None:
        await asyncio.sleep(1.5)

    @workflow
    async def racing(c: Context) -> bool:
        started = time.monotonic()

        async def wait() -> None:
            await c.sleep(timedelta(milliseconds=100))
            waited.append(time.monotonic() - started)

        await asyncio.gather(slow(), wait())
        return True

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [racing]):
        run = await sub.start(racing)
        assert await run.result(timeout=LONG) is True
    assert waited and waited[0] < 1.0, f"the sleep ended after {waited}"


async def test_two_workers_racing_for_a_commit(store: Store) -> None:
    started = asyncio.Event()
    attempts: list[int] = []

    @step
    async def work() -> int:
        started.set()
        attempts.append(1)
        return len(attempts)

    @workflow
    async def raced(c: Context) -> int:
        return await work()

    slow = Recording(store)
    slow.freeze_on_segment = 1.5
    sub_a = await Substantial._create(slow, partitions=1)
    sub_b = await Substantial._open(store)
    async with script(sub_a, [raced], **FAST):
        run = await sub_a.start(raced)
        await started.wait()
        async with script(sub_b, [raced], **FAST):
            value = await sub_b.run(raced, run.chain).result(timeout=LONG)
            assert value == 2, "the second worker's attempt won"

            async def lost() -> bool:
                return any("/log/" in k and c == "exists" for k, c in slow.created)

            await eventually("the frozen commit", lost)


@pytest.mark.parametrize("adapter", ["memory"])
async def test_a_step_runs_on_the_hosts_loop(store: Store) -> None:
    host = asyncio.get_running_loop()
    loops: list[bool] = []

    @step
    async def where() -> None:
        loops.append(asyncio.get_running_loop() is host)

    @workflow
    async def located(c: Context) -> None:
        loops.append(asyncio.get_running_loop() is host)
        await where()

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [located]):
        await (await sub.start(located)).result(timeout=LONG)
    assert loops[0] is False, "the code runs on a loop of its own"
    assert loops[-1] is True, "a step runs on the host's loop"
