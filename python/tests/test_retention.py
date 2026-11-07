"""Retention on the test environment: ended runs collected, their ids taken for 7 days, and the
order of one client's messages. The TypeScript and Rust hosts port these tests under the same
names."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest

import substantial as s

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)


@pytest.fixture
async def env() -> AsyncIterator[s.testing.Env]:
    async with s.testing.Env.open(now=T0) as env:
        yield env
        await env.check()


@s.workflow
async def quick(c: s.Context) -> int:
    return 1


@s.workflow(retention=s.days(90))
async def kept(c: s.Context) -> int:
    return 2


@s.workflow(retention=s.FOREVER)
async def forever(c: s.Context) -> int:
    return 3


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
        return f"RunNotFound {e.collected} {e.until}"
    raise AssertionError("sent")


def runs(env: s.testing.Env) -> set[str]:
    return {key.split("/")[1] for key in env._world.keys("runs/")}


async def test_a_run_is_collected_30_days_after_its_end(env: s.testing.Env) -> None:
    async with env.worker([quick], preset="script"):
        run = await env.start(quick, id="q")
        assert await run.result() == 1
        await env.advance(s.days(30) - s.minutes(1))
        assert await run.status() == "completed"
        await env.advance(s.minutes(2))
        with pytest.raises(s.RunNotFound) as e:
            await run.status()
        assert e.value.collected
        assert e.value.until is not None
        assert env.now + s.days(7) - s.minutes(2) < e.value.until <= env.now + s.days(7)
        with pytest.raises(s.RunNotFound) as e:
            await run.result()
        assert e.value.collected
        assert await run.read("any") is None
        assert env._world.keys("runs/q/") == ["runs/q/gone"]


async def test_a_start_during_the_tombstone_raises_run_exists(env: s.testing.Env) -> None:
    async with env.worker([quick], preset="script"):
        run = await env.start(quick, id="q")
        assert await run.result() == 1
        await env.advance(s.days(31))
        with pytest.raises(s.RunExists) as e:
            await env.start(quick, id="q")
        assert e.value.collected
        assert e.value.until is not None
        assert e.value.until > env.now + s.days(5)
        assert "collected" in str(e.value)


async def test_the_id_starts_a_new_run_after_the_tombstone(env: s.testing.Env) -> None:
    async with env.worker([quick], preset="script"):
        run = await env.start(quick, id="q")
        assert await run.result() == 1
        await env.advance(s.days(30) + s.days(7) + s.minutes(2))
        with pytest.raises(s.RunNotFound) as e:
            await run.status()
        assert not e.value.collected
        assert e.value.until is None
        assert env._world.keys("runs/q/") == []
        run = await env.start(quick, id="q")
        assert await run.result() == 1


async def test_a_workflow_retention_overrides_the_workers(env: s.testing.Env) -> None:
    async with env.worker([quick, kept], preset="script", retention=s.days(2)):
        q = await env.start(quick, id="q")
        k = await env.start(kept, id="k")
        assert await q.result() == 1
        assert await k.result() == 2
        await env.advance(s.days(3))
        with pytest.raises(s.RunNotFound) as e:
            await q.status()
        assert e.value.collected
        assert await k.status() == "completed"
        await env.advance(s.days(88))
        with pytest.raises(s.RunNotFound) as e:
            await k.status()
        assert e.value.collected


async def test_forever_keeps_a_run(env: s.testing.Env) -> None:
    async with env.worker([quick, forever], preset="script", retention=s.FOREVER):
        q = await env.start(quick, id="q")
        f = await env.start(forever, id="f")
        assert await q.result() == 1
        assert await f.result() == 3
        await env.advance(s.days(400))
        assert await q.status() == "completed"
        assert await f.result() == 3


async def test_a_long_lived_chain_keeps_a_bounded_number_of_runs(env: s.testing.Env) -> None:
    async with env.worker([counter], preset="script", retention=s.hours(1)):
        run = await env.start(counter, {"left": 48, "done": 0}, id="c")
        most = 0
        for _ in range(48):
            await env.advance(s.hours(1))
            most = max(most, len(runs(env)))
        assert await run.result() == 48
        assert most <= 4, f"{most} runs kept at once"
        await env.advance(s.hours(3))
        assert env._world.keys("runs/") == ["runs/c/gone"]
        assert env._world.keys("index/") == []


async def test_messages_of_one_client_in_one_millisecond_are_consumed_in_send_order(
    env: s.testing.Env,
) -> None:
    async with env.worker([inbox], preset="script"):
        run = await env.start(inbox, {"n": 4}, id="i")
        await env.settle()
        for id in ["d", "b", "c", "a"]:
            await run.send("m", id, id=id)
        assert await run.result() == ["d", "b", "c", "a"]


async def test_a_result_waiter_ends_with_run_not_found_when_its_run_is_collected(
    env: s.testing.Env,
) -> None:
    async with env.worker([quick], preset="script"):
        run = await env.start(quick, id="q")
        await env.settle()
        await env.advance(s.days(31))
        with pytest.raises(s.RunNotFound) as e:
            await run.result()
        assert e.value.collected


async def test_a_retention_under_an_hour_is_a_config_error(env: s.testing.Env) -> None:
    with pytest.raises(s.ConfigError):
        env.worker([quick], preset="script", retention=s.minutes(59))

    @s.workflow(retention=s.minutes(59))
    async def short(c: s.Context) -> int:
        return 1

    with pytest.raises(s.ConfigError):
        env.worker([short], preset="script")


async def test_a_retention_over_100_years_is_a_config_error(env: s.testing.Env) -> None:
    with pytest.raises(s.ConfigError, match="at most 3153600000000 ms or forever"):
        env.worker([quick], preset="script", retention=s.days(36_501))

    @s.workflow(retention=s.days(36_501))
    async def long(c: s.Context) -> int:
        return 1

    with pytest.raises(s.ConfigError):
        env.worker([long], preset="script")

    @s.workflow(retention=s.days(36_500))
    async def longest(c: s.Context) -> int:
        return 1

    async with env.worker([longest], preset="script", retention=s.days(36_500)):
        pass


async def test_a_send_to_a_collected_chain_raises_run_not_found(env: s.testing.Env) -> None:
    async with env.worker([quick, sender], preset="script"):
        run = await env.start(quick, id="q")
        assert await run.result() == 1
        await env.advance(s.days(31))
        with pytest.raises(s.RunNotFound) as e:
            await run.send("m", 1)
        assert e.value.collected
        until = e.value.until
        with pytest.raises(s.RunNotFound) as e:
            await run.cancel("late")
        assert e.value.collected
        from_workflow = await env.start(sender, {"to": "q"})
        assert await from_workflow.result() == f"RunNotFound True {until}"


async def test_a_result_wait_follows_its_chain_again_and_sees_it_collected() -> None:
    sub = await s.Substantial.create("memory://retention-refollow", partitions=1)
    run = await sub.start(quick, id="q")
    run._refollow = 0.05
    wait = asyncio.create_task(run.result(timeout=timedelta(seconds=10)))
    await asyncio.sleep(0.02)
    store = sub._store
    for key in (await store.list("runs/q/", start_after=None, max_keys=1000, wait=None)).keys:
        await store.delete(key)
    with pytest.raises(s.RunNotFound) as e:
        await wait
    assert not e.value.collected
    await sub.close()
