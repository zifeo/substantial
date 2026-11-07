"""The test environment of docs/testing.md: the environment, its ids and its time. The TypeScript
and Rust hosts port these tests under the same names."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

import substantial as s
from substantial.stores.memory import _NAMESPACES

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)


@pytest.fixture
async def env() -> AsyncIterator[s.testing.Env]:
    async with s.testing.Env.open(now=T0) as env:
        yield env
        await env.check()


@s.workflow
async def debounce(c: s.Context, key: str) -> list[int]:
    events: list[int] = []
    c.handle("event", events.append)
    seen = 0
    while await c.wait_until(lambda seen=seen: len(events) > seen, timeout=s.minutes(5)):
        seen = len(events)
    return events


@s.workflow
async def seen(c: s.Context) -> int:
    """Publishes how many events it has seen, until two."""
    events: list[int] = []
    c.handle("event", events.append)
    while len(events) < 2:
        n = len(events)
        c.publish("seen", n)
        await c.wait_until(lambda n=n: len(events) > n)
    return len(events)


@s.workflow
async def clocks(c: s.Context) -> list[str]:
    def wall() -> str:
        return datetime.now(UTC).isoformat()

    return [(await c.now()).isoformat(), await c.step("wall", wall)]


@s.workflow
async def napping(c: s.Context, days: int) -> str:
    await c.sleep(s.days(days))
    return (await c.now()).isoformat()


@s.workflow
async def randoms(c: s.Context) -> list[object]:
    async def nothing() -> None:
        return None

    await c.step("nothing", nothing)
    return [await c.random(), str(await c.uuid())]


@s.workflow
async def parent(c: s.Context) -> str:
    child = await c.start(napping, {"days": 0}, id="the-child")
    return await child.result()


async def test_the_debounce_of_the_docs() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([debounce], preset="service"),
    ):
        run = await env.start(debounce, {"key": "k"}, id="k")
        await run.send("event", 1)
        await env.advance(s.minutes(4))
        await run.send("event", 2)
        await env.advance(s.minutes(4))
        assert await run.status() == "suspended"
        await env.advance(s.minutes(1))
        assert await run.result() == [1, 2]
        await env.check()


async def test_an_environment_opens_at_now_and_drops_its_namespace_when_left() -> None:
    async with s.testing.Env.open(now=T0) as env:
        assert isinstance(env, s.Substantial)
        assert env.now == T0
        ns = env._world.ns
        assert ns in _NAMESPACES
    assert ns not in _NAMESPACES
    with pytest.raises(s.InvalidArgument):
        s.testing.Env.open(now=datetime(2030, 1, 7))


async def test_an_environments_clock_counts_whole_milliseconds() -> None:
    async with s.testing.Env.open(now=T0.replace(microsecond=123000)) as env:
        assert env.now == T0.replace(microsecond=123000)
    with pytest.raises(
        s.InvalidArgument, match=r"whole milliseconds, not 2030-01-07T09:00:00.123456"
    ):
        s.testing.Env.open(now=T0.replace(microsecond=123456))


async def test_nothing_runs_between_two_calls_of_the_test(env: s.testing.Env) -> None:
    async with env.worker([napping], preset="service"):
        run = await env.start(napping, {"days": 1})
        assert await run.status() == "pending"
        await env.settle()
        assert await run.status() == "suspended"
        assert env.now == T0


async def test_a_run_that_only_waits_reads_suspended_once_a_worker_took_it(
    env: s.testing.Env,
) -> None:
    @s.workflow
    async def idle(c: s.Context) -> int:
        return await c.receive("go")

    async with env.worker([idle], preset="service"):
        run = await env.start(idle)
        assert await run.status() == "pending"
        await env.settle()
        assert await run.status() == "suspended"
        await run.send("go", 1)
        assert await run.result() == 1


async def test_two_sends_in_a_row_reach_the_run_together(env: s.testing.Env) -> None:
    async with env.worker([seen], preset="service"):
        together = await env.start(seen, id="together")
        await together.send("event", 1)
        await together.send("event", 2)
        apart = await env.start(seen, id="apart")
        await apart.send("event", 1)
        await env.settle()
        assert await apart.read("seen") == 1
        await apart.send("event", 2)
        assert await together.result() == await apart.result() == 2
        # One replay took both messages, in one segment; the other run woke for each.
        assert len(env._world.keys("runs/together/log/")) == 1
        assert len(env._world.keys("runs/apart/log/")) == 2


async def test_a_client_shares_the_namespace_and_the_clock(env: s.testing.Env) -> None:
    async with env.client() as client, env.worker([napping], preset="service"):
        assert isinstance(client, s.Substantial)
        await env.advance(s.seconds(10))
        run = await client.start(napping, {"days": 0}, id="from-a-client")
        # No worker of the client: the environment's finds the run at its next scan, at 30 s.
        await env.advance(s.seconds(19))
        assert await run.status() == "pending"
        await env.advance(s.seconds(1))
        assert await env.run(napping, "from-a-client").result() == (T0 + s.seconds(30)).isoformat()
        assert await run.status() == "completed"
    other = await env.client()
    assert await other.run(napping, "from-a-client").status() == "completed"
    await other.close()


async def test_a_step_reads_the_wall_clock_and_the_run_the_virtual_one(
    env: s.testing.Env,
) -> None:
    async with env.worker([clocks], preset="script"):
        virtual, wall = await (await env.start(clocks)).result()
        assert virtual == T0.isoformat()
        assert abs(datetime.fromisoformat(wall) - datetime.now(UTC)) < s.minutes(1)


async def test_a_timer_fires_at_its_due_instant(env: s.testing.Env) -> None:
    async with env.worker([napping], preset="service"):
        run = await env.start(napping, {"days": 30})
        await env.advance(s.days(30) - s.seconds(1))
        assert await run.status() == "suspended"
        await env.advance(s.seconds(1))
        assert await run.result() == (T0 + s.days(30)).isoformat()


async def test_a_child_is_found_at_once_by_the_worker_that_started_it(
    env: s.testing.Env,
) -> None:
    async with env.worker([parent, napping], preset="service"):
        run = await env.start(parent)
        assert await run.result() == T0.isoformat()


async def test_result_settles_and_never_moves_the_clock(env: s.testing.Env) -> None:
    async with env.worker([napping], preset="service"):
        run = await env.start(napping, {"days": 2})
        with pytest.raises(s.testing.Stuck, match=r"timer due at 2030-01-09T09:00:00\+00:00"):
            await run.result()
        with pytest.raises(s.WaitTimeout):
            await run.result(timeout=s.days(3))
        assert env.now == T0
        await env.advance(s.days(2))
        assert await run.result(timeout=s.seconds(1)) == (T0 + s.days(2)).isoformat()


async def test_stuck_names_a_message_and_a_workflow_no_worker_serves(
    env: s.testing.Env,
) -> None:
    async with env.worker([seen, parent], preset="service"):
        waiting = await env.start(seen, id="waiting")
        with pytest.raises(s.testing.Stuck, match="a message"):
            await waiting.result()
        orphan = await env.start(napping, {"days": 0}, id="orphan")
        with pytest.raises(s.testing.Stuck, match="napping, which no worker serves"):
            await orphan.result()
        with pytest.raises(s.testing.Stuck, match="the-child"):
            await (await env.start(parent, id="a-parent")).result()


async def ids(chaos: s.testing.Chaos | None, takeover: bool) -> tuple[object, ...]:
    """The ids and randoms of one run, by one worker or by a second one after the first crashed."""
    async with (
        s.testing.Env.open(now=T0, chaos=chaos) as env,
        env.worker([randoms], preset="service") as first,
        env.worker([randoms], preset="service") as second,
    ):
        hold = env.hold("nothing") if takeover else None
        run = await env.start(randoms)
        await env.settle()
        if hold is not None:
            hold.crash()
            await env.advance(s.minutes(1))
        return run.chain, await run.result(), first._id, second._id


async def test_ids_and_randoms_are_drawn_from_the_seed_and_what_they_name() -> None:
    plain = await ids(None, takeover=False)
    assert plain == await ids(None, takeover=False)
    assert plain == await ids(s.testing.Chaos(seed=0, rate=0), takeover=True)
    seeded = await ids(s.testing.Chaos(seed=7, rate=0), takeover=False)
    assert all(a != b for a, b in zip(plain, seeded, strict=True))
