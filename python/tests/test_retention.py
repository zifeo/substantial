"""Retention on the test environment: ended runs removed by their expiry, their ids taken while
they are kept, and the order of one client's messages. The TypeScript and Rust hosts port these
tests under the same names."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from conftest import LONG, eventually, script

import substantial as s
from substantial.stores.memory import MemoryStore

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)
KEPT_DAYS = 90


@pytest.fixture
async def env() -> AsyncIterator[s.testing.Env]:
    async with s.testing.Env.open(now=T0) as env:
        yield env
        await env.check()


@s.workflow
async def quick(c: s.Context) -> int:
    return 1


@s.workflow(retention=KEPT_DAYS)
async def kept(c: s.Context) -> int:
    return 2


@s.workflow
async def counter(c: s.Context, left: int, done: int) -> int:
    """Continues as new every hour, `left` times, then ends with the count."""
    if left == 0:
        return done
    await c.sleep(s.hours(1))
    await c.continue_as_new({"left": left - 1, "done": done + 1})
    raise AssertionError("continue_as_new returned")


@s.workflow
async def inbox(c: s.Context, n: int) -> list[str]:
    """Collects the values of its messages until it has `n`."""
    names: list[str] = []
    c.handle("m", names.append)
    await c.wait_until(lambda: len(names) >= n)
    return names


@s.workflow
async def sender(c: s.Context, to: str) -> str:
    """Sends to `to` and returns the error the send raised."""
    try:
        await c.send(to, "m", 1)
    except s.RunNotFound as e:
        return f"RunNotFound {e.chain}"
    raise AssertionError("sent")


@s.workflow
async def ordered(c: s.Context, to: str) -> None:
    """Sends `m` to `to` under the ids `e` to `a`, in that order, the first three at once."""
    ids = ["e", "d", "c", "b", "a"]
    await asyncio.gather(*(c.send(to, "m", id, id=id) for id in ids[:3]))
    for id in ids[3:]:
        await c.send(to, "m", id, id=id)


@s.workflow
async def adopting(c: s.Context) -> int:
    """Starts a child kept 90 days and returns its value."""
    child = await c.start(kept, id="child")
    return await child.result()


@s.workflow
async def failing(c: s.Context) -> int:
    raise s.Fail("Refused", "not today")


def keys(env: s.testing.Env, prefix: str) -> list[str]:
    return env._world.keys(prefix)


async def test_a_run_is_removed_30_days_after_its_end(env: s.testing.Env) -> None:
    async with env.worker([quick], preset="script"):
        run = await env.start(quick, id="q")
        assert await run.result() == 1
        await env.advance(s.days(30) - s.minutes(1))
        assert await run.status() == "completed"
        await env.advance(s.minutes(2))
        with pytest.raises(s.RunNotFound):
            await run.status()
        with pytest.raises(s.RunNotFound):
            await run.result()
        assert await run.read("any") is None
        assert keys(env, "runs/") == []


async def test_a_start_while_the_run_is_kept_raises_run_exists(env: s.testing.Env) -> None:
    async with env.worker([quick], preset="script"):
        run = await env.start(quick, id="q")
        assert await run.result() == 1
        await env.advance(s.days(30) - s.minutes(1))
        with pytest.raises(s.RunExists) as e:
            await env.start(quick, {"other": True}, id="q")
        assert e.value.id == "q"
        assert str(e.value) == "id q is taken by another workflow, version or input"


async def test_the_id_starts_a_new_run_once_its_retention_passed(env: s.testing.Env) -> None:
    async with env.worker([quick], preset="script"):
        run = await env.start(quick, id="q")
        assert await run.result() == 1
        await env.advance(s.days(30) + s.minutes(1))
        with pytest.raises(s.RunNotFound):
            await run.status()
        run = await env.start(quick, id="q")
        assert await run.status() == "pending", "a new run"
        assert await run.result() == 1


async def test_a_workflow_retention_overrides_the_workers(env: s.testing.Env) -> None:
    async with env.worker([quick, kept], preset="script", retention=2):
        q = await env.start(quick, id="q")
        k = await env.start(kept, id="k")
        assert await q.result() == 1
        assert await k.result() == 2
        await env.advance(s.days(3))
        with pytest.raises(s.RunNotFound):
            await q.status()
        assert await k.status() == "completed"
        await env.advance(s.days(88))
        with pytest.raises(s.RunNotFound):
            await k.status()


async def test_a_long_lived_chain_keeps_a_bounded_number_of_runs(env: s.testing.Env) -> None:
    async with env.worker([counter], preset="script", retention=1):
        run = await env.start(counter, {"left": 72, "done": 0}, id="c")
        most = 0
        for _ in range(72):
            await env.advance(s.hours(1))
            most = max(most, len(keys(env, "runs/")) + len(keys(env, "archives/")))
        assert await run.result() == 72
        # A day of hourly runs, each kept by its archive and its id, and the chain's head.
        assert most <= 2 * 25 + 1, f"{most} objects kept at once"
        await env.advance(s.days(1) + s.minutes(1))
        assert keys(env, "runs/") == []
        assert keys(env, "archives/") == []


async def test_messages_of_one_client_in_one_millisecond_are_consumed_in_send_order(
    env: s.testing.Env,
) -> None:
    async with env.worker([inbox], preset="script"):
        run = await env.start(inbox, {"n": 4}, id="i")
        await env.settle()
        for id in ["d", "b", "c", "a"]:
            await run.send("m", id, id=id)
        assert await run.result() == ["d", "b", "c", "a"]


async def test_messages_of_one_run_in_one_millisecond_are_consumed_in_call_order(
    env: s.testing.Env,
) -> None:
    async with env.worker([ordered], preset="script"):
        run = await env.start(inbox, {"n": 5}, id="i")
        sending = await env.start(ordered, {"to": "i"})
        await sending.result()
        # Served once every message waits: its first replay consumes them in key order.
        async with env.worker([inbox], preset="script"):
            assert await run.result() == ["e", "d", "c", "b", "a"]


async def test_a_result_waiter_ends_with_run_not_found_when_its_run_is_removed(
    env: s.testing.Env,
) -> None:
    async with env.worker([quick], preset="script"):
        run = await env.start(quick, id="q")
        await env.settle()
        await env.advance(s.days(31))
        with pytest.raises(s.RunNotFound):
            await run.result()


async def test_a_parent_is_removed_after_its_own_retention_whatever_its_child(
    env: s.testing.Env,
) -> None:
    async with env.worker([adopting, kept], preset="script"):
        run = await env.start(adopting, id="p")
        assert await run.result() == 2
        child = env.run(kept, "child")
        # § a1ys7at7p@8e85x
        # The parent's own retention is 30 days, its child's 90.
        await env.advance(s.days(30) + s.minutes(1))
        with pytest.raises(s.RunNotFound):
            await run.status()
        assert await child.status() == "completed"
        await env.advance(s.days(60))
        with pytest.raises(s.RunNotFound):
            await child.status()
        # § end a1ys7at7p


@s.workflow
async def lingering(c: s.Context) -> int:
    """Waits for `go`."""
    return await c.receive("go")


@s.workflow
async def continuing(c: s.Context, again: bool = False) -> int:
    """Starts `lingering` as `kid`, abandoned, then continues once and ends."""
    if again:
        return 1
    await c.start(lingering, id="kid", abandon=True)
    await c.continue_as_new({"again": True})
    raise AssertionError("continue_as_new returned")


async def test_a_continued_run_expires_while_its_abandoned_child_lives(
    env: s.testing.Env,
) -> None:
    async with env.worker([continuing, lingering], preset="script", retention=1):
        run = await env.start(continuing, {}, id="p")
        assert await run.result() == 1
        # § a1ys7at7p.c@346q2
        await env.advance(s.days(1) + s.minutes(1))
        assert keys(env, "archives/") == [], "the continued run"
        with pytest.raises(s.RunNotFound):
            await run.status()
        kid = env.run(lingering, "kid")
        assert await kid.status() == "suspended"
        # § end a1ys7at7p.c
        # § n0ge6zsws.a@23smx
        # Its end tells a parent its retention removed: the notice is deleted unconsumed.
        await kid.send("go", 2)
        assert await kid.result() == 2
        await env.settle()
        assert keys(env, "queues/") == [], keys(env, "")
        with pytest.raises(s.RunNotFound):
            await run.status()
        # § end n0ge6zsws.a


@s.workflow
async def leaving(c: s.Context) -> int:
    """Starts `lingering` as `kid`, abandoned, and ends."""
    await c.start(lingering, id="kid", abandon=True)
    return 0


async def test_a_child_s_end_reaching_a_run_that_took_its_parent_s_id_is_ignored(
    env: s.testing.Env,
) -> None:
    async with env.worker([leaving, lingering], preset="script", retention=1):
        parent = await env.start(leaving, id="p")
        assert await parent.result() == 0
        await env.advance(s.days(1) + s.minutes(1))
        with pytest.raises(s.RunNotFound):
            await parent.status()
        # § n0ge6zsws@ds491
        stranger = await env.start(lingering, id="p")
        await env.settle()
        kid = env.run(lingering, "kid")
        await kid.send("go", 2)
        assert await kid.result() == 2
        await env.settle()
        assert await stranger.status() == "suspended"
        history = await stranger.history()
        assert all(e.data.get("name") != "substantial.ended" for e in history), history
        await stranger.send("go", 3)
        assert await stranger.result() == 3
        # § end n0ge6zsws


@s.workflow
async def late(c: s.Context) -> int:
    """Sleeps 2 days, past its parent's retention, then ends with 4."""
    await c.sleep(s.days(2))
    return 4


def returning(awaiting: bool) -> s.Workflow[..., int]:
    """Starts `late` as `k4`, abandoned, and ends at once, or, `awaiting`, also returns its result:
    the same workflow deployed again with the same child start."""

    @s.workflow(name="returning", retention=1)
    async def code(c: s.Context) -> int:
        child = await c.start(late, id="k4", abandon=True)
        if awaiting:
            return await child.result()
        return 0

    return code


async def test_a_run_that_took_its_parent_s_id_adopting_its_ended_child_gets_its_end(
    env: s.testing.Env,
) -> None:
    async with env.worker([returning(False), late], preset="script"):
        parent = await env.start(returning(False), id="p4")
        assert await parent.result() == 0
        await env.advance(s.days(2) + s.minutes(1))
        with pytest.raises(s.RunNotFound):
            await parent.status()
        assert await env.run(late, "k4").result() == 4
    async with env.worker([returning(True), late], preset="script"):
        # § svx76xx41@vzy09
        again = await env.start(returning(True), id="p4")
        assert await again.result() == 4
        # § end svx76xx41


@s.workflow
async def unserved(c: s.Context) -> int:
    """A workflow no worker of these tests serves."""
    return 9


@s.workflow
async def owing(c: s.Context) -> int:
    """Starts `unserved` as `kid`, then waits for `go` and ends, which cancels its child."""
    await c.start(unserved, id="kid")
    return await c.receive("go")


async def child_gone(store: MemoryStore, sub: s.Substantial) -> s.Run[int]:
    """A parent whose child never ran, served by no worker; then the child's head and due entry
    go, as its retention's expiry removes a chain."""
    await sub.start(owing, id="p")
    run = sub.run(owing, "p")
    await eventually("the wait", lambda: _is(run, "suspended"), within=LONG.total_seconds())
    await store.delete("runs/kid")
    listing = await store.list("queues/unserved/", start_after=None, max_keys=100, wait=None)
    for key in listing.keys:
        await store.delete(key)
    return run


async def _is(run: s.Run, status: str) -> bool:
    return await run.status() == status


async def test_a_cancel_reaching_a_child_past_its_retention_is_dropped() -> None:
    store = MemoryStore("retention-cancel-gone")
    async with await s.Substantial._create(store) as sub:
        async with script(sub, [owing, lingering]):
            run = await child_gone(store, sub)
            # § n0ge6zsws.b@e22mj
            await run.send("go", 1)
            assert await run.result(timeout=LONG) == 1
        listing = await store.list("", start_after=None, max_keys=100, wait=None)
        assert listing.keys == ["runs/p", "substantial.json"]
        # § end n0ge6zsws.b


async def test_a_cancel_never_reaches_a_run_that_took_its_child_s_id() -> None:
    store = MemoryStore("retention-cancel-retaken")
    async with await s.Substantial._create(store) as sub, script(sub, [owing, lingering]):
        run = await child_gone(store, sub)
        # § n0ge6zsws.b@284fz
        stranger = await sub.start(lingering, id="kid")
        await run.send("go", 1)
        assert await run.result(timeout=LONG) == 1
        await stranger.send("go", 3)
        assert await stranger.result(timeout=LONG) == 3
        # § end n0ge6zsws.b


async def test_a_retention_change_reaches_only_the_runs_that_end_after_it(
    env: s.testing.Env,
) -> None:
    async with env.worker([quick], preset="script", retention=2):
        before = await env.start(quick, id="before")
        assert await before.result() == 1
    async with env.worker([quick], preset="script"):
        after = await env.start(quick, id="after")
        assert await after.result() == 1
        # § a1ys7at7p.b@prwev
        await env.advance(s.days(3))
        with pytest.raises(s.RunNotFound):
            await before.status()
        assert await after.status() == "completed"
        # § end a1ys7at7p.b


async def test_a_retry_of_a_removed_chain_raises_run_not_found(env: s.testing.Env) -> None:
    async with env.worker([failing], preset="script"):
        run = await env.start(failing, id="f")
        with pytest.raises(s.RunFailed):
            await run.result()
        await env.advance(s.days(31))
        with pytest.raises(s.RunNotFound) as e:
            await run.retry()
        assert e.value.chain == "f"


def retried(version: int, steps: list[int], refuse: list[bool]) -> s.Workflow[..., int]:
    """Version 1 counts the runs of its step, then fails while `refuse` is set; version 2
    ends."""

    async def count() -> int:
        steps[0] += 1
        return steps[0]

    async def run(c: s.Context) -> int:
        if version == 2:
            return 0
        n = await c.step("count", count)
        if refuse[0]:
            raise s.Fail("Refused", "not today")
        return n

    return s.Workflow(run, name="retried", version=version, retention=1)


async def test_a_failed_run_is_kept_until_its_retry_holds_its_history(
    env: s.testing.Env,
) -> None:
    steps, refuse = [0], [True]

    # The retry's entry, due while no worker takes it, keeps the environment from skipping
    # scans: the service preset and a long `max_idle` keep them few.
    def worker(version: int) -> s.Worker:
        return env.worker([retried(version, steps, refuse)], preset="service", max_idle=s.hours(1))

    run = await env.start(retried(1, steps, refuse), id="f")
    async with worker(1):
        with pytest.raises(s.RunFailed):
            await run.result()
    refuse[0] = False
    await run.retry()
    # Serves the name, but not the retry's version, which waits.
    async with worker(2):
        # The failed run's retention passed: its archive expired, its history is in the retry's
        # head.
        await env.advance(s.days(2))
        assert keys(env, "archives/") == []
        async with worker(1):
            assert await run.result() == 1, "the recorded step, from the retry's head"
    assert steps[0] == 1


async def test_a_retention_under_a_day_is_a_config_error(env: s.testing.Env) -> None:
    for retention in (0, s.hours(23), True):
        with pytest.raises(s.ConfigError):
            env.worker([quick], preset="script", retention=retention)

    @s.workflow(retention=0)
    async def short(c: s.Context) -> int:
        return 1

    with pytest.raises(s.ConfigError):
        env.worker([short], preset="script")


async def test_a_retention_over_100_years_is_a_config_error(env: s.testing.Env) -> None:
    for retention in (36_501, 213_503_982_334_602):
        with pytest.raises(s.ConfigError, match="whole days from 1 to 36500, "):
            env.worker([quick], preset="script", retention=retention)
    for retention in (36_501, 2**32 - 1):

        @s.workflow(retention=retention)
        async def long(c: s.Context) -> int:
            return 1

        with pytest.raises(s.ConfigError, match="whole days from 1 to 36500, "):
            env.worker([long], preset="script")

    @s.workflow(retention=36_500)
    async def longest(c: s.Context) -> int:
        return 1

    async with env.worker([longest], preset="script", retention=36_500):
        pass


async def test_a_send_to_a_removed_chain_raises_run_not_found(env: s.testing.Env) -> None:
    async with env.worker([quick, sender], preset="script"):
        run = await env.start(quick, id="q")
        assert await run.result() == 1
        await env.advance(s.days(31))
        with pytest.raises(s.RunNotFound) as e:
            await run.send("m", 1)
        assert e.value.chain == "q"
        with pytest.raises(s.RunNotFound):
            await run.cancel("late")
        from_workflow = await env.start(sender, {"to": "q"})
        assert await from_workflow.result() == "RunNotFound q"


async def test_a_result_wait_sees_its_chain_removed() -> None:
    sub = await s.Substantial.create("memory://retention-removed")
    run = await sub.start(quick, id="q")
    wait = asyncio.create_task(run.result(timeout=timedelta(seconds=10)))
    await asyncio.sleep(0.02)
    await sub._store.delete("runs/q")
    with pytest.raises(s.RunNotFound):
        await wait
    await sub.close()
