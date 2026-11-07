"""The test environment of docs/testing.md: failures and faults, what never hangs, what is cheap."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import Counter
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

import substantial as s
from substantial.stores.retry import Ambiguous, Transient
from substantial.testing import _time

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)
pushes: list[tuple[datetime, list[int]]] = []


@pytest.fixture(autouse=True)
def fakes() -> None:
    pushes.clear()


@s.workflow
async def debounce(c: s.Context, key: str) -> list[int]:
    events: list[int] = []
    c.handle("event", events.append)
    seen = 0
    while await c.wait_until(lambda seen=seen: len(events) > seen, timeout=s.minutes(2)):
        seen = len(events)

    async def push(at: str, events: list[int]) -> None:
        pushes.append((datetime.fromisoformat(at), events))

    await c.step("push", push, (await c.now()).isoformat(), events)
    return events


@s.workflow
async def failing(c: s.Context) -> None:
    async def fail() -> None:
        raise s.Permanent("unknown invoice")

    await c.step("fail", fail)


@s.workflow
async def failing_parent(c: s.Context) -> None:
    await c.start(failing, id="its-child")


@s.workflow
async def napping(c: s.Context) -> None:
    await c.sleep(s.days(30))


@s.workflow
async def echo(c: s.Context) -> None:
    pings: list[int] = []
    c.handle("ping", pings.append)
    while True:
        seen = len(pings)
        await c.wait_until(lambda seen=seen: len(pings) > seen)
        await c.send(c.chain, "ping", seen)


async def test_an_outage_inside_the_debounce_window() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([debounce], preset="service"),
    ):
        run = await env.start(debounce, {"key": "c1"}, id="contact-c1")
        await run.send("event", 1, id="crm-1")
        await env.advance(s.minutes(1))
        env.outage(s.minutes(10))
        with pytest.raises(s.StoreError):
            await run.send("event", 2, id="crm-2")
        assert env.now == T0 + s.minutes(1)
        await env.advance(s.minutes(12))
        assert await run.result() == [1]
        # The timer came due in the outage: the worker retried, and pushed once the store was back.
        [(at, events)] = pushes
        assert T0 + s.minutes(11) <= at <= T0 + s.minutes(12) and events == [1]
        await env.check()


async def test_a_thousand_events_in_one_instant() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([debounce], preset="service"),
    ):
        run = await env.start(debounce, {"key": "c1"})
        for i in range(1000):
            await run.send("event", i, id=f"crm-{i:04}")
        await env.advance(s.minutes(2))
        assert await run.result() == list(range(1000))
        assert pushes == [(T0 + s.minutes(2), list(range(1000)))]


async def test_a_run_failing_in_the_background_fails_the_test() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([failing, failing_parent], preset="service"),
        env.client() as app,
    ):
        read = await env.start(failing, id="read")
        looked = await env.start(failing, id="looked")
        await app.start(failing, id="started-by-the-app")
        await env.start(failing_parent, id="parent")
        cancelled = await env.start(napping, id="cancelled")
        await cancelled.cancel("not needed")
        await env.advance(s.minutes(1))
        with pytest.raises(s.RunFailed):
            await read.result()
        assert await looked.status() == "failed"
        assert await app.run(failing, "started-by-the-app").status() == "failed"
        with pytest.raises(s.testing.Unchecked) as unchecked:
            await env.check()
        message = str(unchecked.value)
        assert "unknown invoice" in message
        for chain in ("started-by-the-app", "its-child"):
            assert f"{chain} (" in message
        for chain in ("read", "looked", "cancelled", "parent"):
            assert f" {chain} (" not in message
        with pytest.raises(s.RunFailed):
            await env.run(failing, "started-by-the-app").result()
        with pytest.raises(s.RunFailed):
            await env.run(failing, "its-child").result()
        await env.check()


@s.workflow(retention=s.hours(1))
async def failing_briefly_kept(c: s.Context) -> None:
    async def fail() -> None:
        raise s.Permanent("card declined")

    await c.step("fail", fail)


@s.workflow
async def handling_parent(c: s.Context) -> str:
    child = await c.start(failing, id="handled-child")
    try:
        await child.result()
    except s.RunFailed:
        return "shipped without it"
    return "shipped"


async def test_a_child_failure_its_parent_read_is_read() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([failing, handling_parent], preset="service"),
    ):
        run = await env.start(handling_parent, id="order")
        await env.advance(s.minutes(1))
        assert await run.result() == "shipped without it"
        await env.check()


async def test_a_failed_run_collected_before_check_is_reported() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([failing_briefly_kept], preset="service"),
    ):
        run = await env.start(failing_briefly_kept, id="nightly")
        await env.advance(s.days(1))
        with pytest.raises(s.RunNotFound):
            await run.status()
        with pytest.raises(s.testing.Unchecked, match=r"nightly \(StepFailed: .*card declined\)"):
            await env.check()


async def test_a_hung_step_is_stuck_and_interrupted_when_its_worker_crashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_time, "STEP_LIMIT", 0.05)
    answered = asyncio.Event()
    ended: list[str] = []

    @s.workflow
    async def hanging(c: s.Context) -> None:
        async def call_the_psp() -> None:
            await answered.wait()
            ended.append("charged")

        await c.step("charge", call_the_psp)

    async with s.testing.Env.open(now=T0) as env:
        async with env.worker([hanging], preset="service") as worker:
            await env.start(hanging, id="hung")
            with pytest.raises(s.testing.Stuck, match="hung/charge"):
                await env.settle()
            worker.crash()
        answered.set()
        await asyncio.sleep(0)
    assert ended == []


async def test_a_hung_step_fails_the_test_instead_of_hanging_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_time, "STEP_LIMIT", 0.2)
    answered = asyncio.Event()

    @s.workflow
    async def hanging(c: s.Context) -> None:
        await c.step("charge", answered.wait)

    async def scenario() -> None:
        async with s.testing.Env.open(now=T0) as env, env.worker([hanging], preset="service"):
            await env.start(hanging, id="hung")
            await env.settle()

    with pytest.raises(s.testing.Stuck, match="hung/charge"):
        await asyncio.wait_for(scenario(), 3)
    answered.set()


@s.workflow
async def quote(c: s.Context, sku: str) -> int:
    return 42


engines: list[s.Substantial] = []


@s.workflow
async def priced(c: s.Context, sku: str) -> int:
    async def ask_quote() -> int:
        run = await engines[0].start(quote, {"sku": sku}, id=f"quote-{sku}")
        return await run.result(timeout=s.seconds(30))

    return await c.step("ask_quote", ask_quote)


async def test_a_step_waiting_on_the_environment_fails_at_once() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([priced, quote], preset="service"),
        env.client() as engine,
    ):
        engines[:] = [engine]
        run = await env.start(priced, {"sku": "a"}, id="p1")
        started = time.perf_counter()
        with pytest.raises(s.RunFailed, match="a step cannot wait for the environment to settle"):
            await run.result()
        assert time.perf_counter() - started < 1
        await env.check()


async def test_runs_waking_each_other_without_the_clock_moving_are_stuck() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([echo], preset="service") as worker,
    ):
        run = await env.start(echo, id="echo")
        await run.send("ping", 0)
        with pytest.raises(s.testing.Stuck, match="run echo was replayed more than 100 times"):
            await env.advance(s.minutes(1))
        assert env.now == T0
        worker.crash()


@s.workflow
async def counter(c: s.Context) -> int:
    events: list[int] = []
    c.handle("event", events.append)
    await c.wait_until(lambda: len(events) >= 150)
    return len(events)


async def test_many_settles_at_one_instant_are_not_a_livelock() -> None:
    async with s.testing.Env.open(now=T0) as env, env.worker([counter], preset="service"):
        run = await env.start(counter, id="counter")
        for i in range(150):
            await run.send("event", i)
            await env.settle()
        assert await run.result() == 150


@s.workflow
async def order(c: s.Context, order: str) -> dict[str, Any]:
    async def reserve(order: str) -> str:
        return f"stock-{order}"

    async def capture(order: str) -> str:
        return f"cap-{order}"

    stock = await c.step("reserve", reserve, order)
    paid = await c.receive("paid")
    capture_id = await c.step("capture", capture, order)
    receipt = await (await c.start(receipt_of, {"capture": capture_id})).result()
    return {"stock": stock, "paid": paid, "receipt": receipt}


@s.workflow
async def receipt_of(c: s.Context, capture: str) -> str:
    async def mail(capture: str) -> str:
        return f"mailed {capture}"

    await c.sleep(s.seconds(10))
    return await c.step("mail", mail, capture)


async def chaotic(seed: int, rate: float, workers: int) -> tuple[Any, ...]:
    async with s.testing.Env.open(now=T0, chaos=s.testing.Chaos(seed=seed, rate=rate)) as env:
        async with contextlib.AsyncExitStack() as stack:
            for _ in range(workers):
                await stack.enter_async_context(env.worker([order, receipt_of], preset="service"))
            runs = [await env.start(order, {"order": f"o-{i}"}) for i in range(3)]
            for run in runs:
                await run.send("paid", 42)
            await env.advance(s.minutes(30))
            results = [await run.result() for run in runs]
        await env.check()
        # A worker Chaos replaced has an id of its own.
        ids = [(w._place, w._replaced, w._id) for w in env._world.workers]
        assert len({i for _, _, i in ids}) == len(ids)
        assert all(r == 0 for _, r, _ in ids) == (env.faults.crashed == 0)
        return env.faults, [run.chain for run in runs], results, ids


@pytest.mark.parametrize("seed", range(50))
async def test_an_order_survives_any_fault(seed: int) -> None:
    faults, _, results, _ = await chaotic(seed, 0.05, workers=1)
    assert results == [
        {"stock": f"stock-o-{i}", "paid": 42, "receipt": f"mailed cap-o-{i}"} for i in range(3)
    ]
    assert faults.failed + faults.ambiguous + faults.crashed > 0


async def test_the_same_seed_replays_the_same_faults_and_ids() -> None:
    first = await chaotic(11, 0.2, workers=2)
    assert first[0].failed and first[0].ambiguous and first[0].crashed
    assert first == await chaotic(11, 0.2, workers=2)
    assert first[:2] != (await chaotic(12, 0.2, workers=2))[:2]


@s.workflow
async def many_steps(c: s.Context, n: int) -> int:
    async def step(i: int) -> int:
        return i

    for i in range(n):
        await c.step("step", step, i)
    return n


async def test_chaos_crashes_a_worker_after_about_rate_of_the_step_attempts() -> None:
    crashed = 0
    for seed in range(4):
        chaos = s.testing.Chaos(seed=seed, rate=0.05)
        async with (
            s.testing.Env.open(now=T0, chaos=chaos) as env,
            env.worker([many_steps], preset="service", continue_as_new_after=0),
        ):
            run = await env.start(many_steps, {"n": 100})
            await env.advance(s.hours(1))
            assert await run.result() == 100
            crashed += env.faults.crashed
    # 400 steps, each attempt that returns drawing a crash with probability 0.05.
    assert 10 <= crashed <= 35, crashed


async def test_chaos_faults_the_workers_not_the_clients() -> None:
    async with s.testing.Env.open(now=T0, chaos=s.testing.Chaos(seed=1, rate=1)) as env:
        run = await env.start(napping)
        await run.send("note", 1)
        assert await run.status() == "pending"
        assert env.faults.failed == env.faults.ambiguous == env.faults.crashed == 0
        async with env.worker([napping], preset="service") as worker:
            await env.advance(s.seconds(1))
            assert env.faults.failed > 0
            worker.crash()


@s.workflow
async def stamp(c: s.Context) -> str:
    return (await c.now()).isoformat()


async def test_an_outage_moves_the_scans_after_it() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([stamp], preset="service"),
        env.client() as other_pod,
    ):
        await env.advance(s.seconds(5))
        env.outage(s.seconds(40))
        await env.advance(s.seconds(57))
        run = await other_pod.start(stamp, id="late")
        await env.advance(s.minutes(1))
        # The scan due at 30 s failed and was retried once the store was back, which moved the
        # scans after it: the run is found at 69 s, not at 90 s.
        assert await run.result() == (T0 + s.seconds(69)).isoformat()


async def test_chaos_faults_the_scans_of_an_idle_worker() -> None:
    async with (
        s.testing.Env.open(now=T0, chaos=s.testing.Chaos(seed=1, rate=0.5)) as env,
        env.worker([stamp], preset="service"),
    ):
        await env.advance(s.hours(1))
        # About 120 scans, half of them failed at least once.
        assert env.faults.failed > 60


async def test_chaos_counts_a_create_that_found_its_key_as_failed() -> None:
    async with s.testing.Env.open(now=T0, chaos=s.testing.Chaos(seed=1, rate=1)) as env:
        world = env._world
        await world.memory.create("taken", b"x")
        clock = SimpleNamespace(id="w", paused=False, tries=Counter())
        store = world.store(clock)  # type: ignore[arg-type]
        world.time.open = True
        for _ in range(20):
            with pytest.raises((Transient, Ambiguous)):
                await store._try("create", "taken", lambda: store._create("taken", b"x"))
        world.time.open = False
        assert (env.faults.failed, env.faults.ambiguous) == (20, 0)


@s.workflow
async def ticking(c: s.Context) -> list[str]:
    async def tick() -> None:
        return None

    instants = []
    for _ in range(6):
        await c.sleep(s.hours(4))
        await c.step("tick", tick)
        instants.append((await c.now()).isoformat())
    return instants


async def a_chaotic_day(skipping: bool) -> tuple[Any, ...]:
    async with s.testing.Env.open(now=T0, chaos=s.testing.Chaos(seed=11, rate=0.05)) as env:
        env._world.skipping = skipping
        async with env.worker([ticking], preset="service"), env.client() as other_pod:
            run = await env.start(ticking, id="early")
            await env.advance(s.minutes(7 * 60 + 13))
            late = await other_pod.start(ticking, id="late")
            await env.advance(s.hours(26))
            results = [await run.result(), await late.result()]
        world = env._world
        return results, env.faults, dict(world.memory._objects), env.now, world.skipped


async def test_chaos_skips_only_the_scans_whose_draws_succeed() -> None:
    *skipped, skips = await a_chaotic_day(skipping=True)
    *ran, runs = await a_chaotic_day(skipping=False)
    # The same faults, the same histories, every event at the same instant: the store's keys and
    # bytes, the segments and their times included.
    assert skipped == ran
    faults = skipped[1]
    assert faults.failed > 0 and faults.crashed > 0, faults
    assert skips > 2000 and runs == 0, (skips, runs)


@pytest.mark.parametrize("preset", ["service", "script"])
async def test_a_month_of_virtual_time_is_cheap(preset: str) -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([napping], preset=preset),
        env.client() as other_pod,
    ):
        run = await env.start(napping)
        started = time.perf_counter()
        await env.advance(s.days(31))
        assert time.perf_counter() - started < 0.5
        assert await run.status() == "completed"
        # The scans skipped kept their instants: one started 10 s after a scan is found at the next.
        await env.advance(s.seconds(10))
        late = await other_pod.start(napping, id="late")
        await env.advance(s.seconds(19 if preset == "service" else 0))
        assert await late.status() == "pending"
        await env.advance(s.seconds(1))
        assert await late.status() == "suspended"
