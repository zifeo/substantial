"""`c.poll`: a step tried again after a sleep until it returns a value or its timeout passes. The
TypeScript and Rust hosts port these tests under the same names."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

import substantial as s

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)

pokes: list[datetime] = []


@pytest.fixture
async def env() -> AsyncIterator[s.testing.Env]:
    pokes.clear()
    async with s.testing.Env.open(now=T0) as env:
        yield env
        await env.check()


@s.workflow
async def poller(c: s.Context, n: int, every: int, timeout: int) -> str | None:
    def check() -> str | None:
        pokes.append(T0)
        return "ready" if len(pokes) >= n else None

    return await c.poll("check", check, every=s.minutes(every), timeout=s.minutes(timeout))


async def test_poll_returns_the_first_value_that_is_not_none(env: s.testing.Env) -> None:
    async with env.worker([poller], preset="service"):
        run = await env.start(poller, {"n": 3, "every": 1, "timeout": 10})
        await env.advance(s.minutes(2))
        assert await run.result() == "ready"
        assert len(pokes) == 3
        events = [e for e in await run.history() if e.data.get("name") == "check"]
        assert [(e.data["name"], e.data["occurrence"]) for e in events] == [
            ("check", 0),
            ("check", 1),
            ("check", 2),
        ]


async def test_poll_returns_none_once_its_timeout_passed(env: s.testing.Env) -> None:
    async with env.worker([poller], preset="service"):
        run = await env.start(poller, {"n": 100, "every": 3, "timeout": 10})
        await env.advance(s.minutes(9))
        assert len(pokes) == 4
        await env.advance(s.minutes(1))
        assert await run.result() is None
        # At 0, 3, 6 and 9 minutes, and at 10, the sleep bounded by the time left.
        assert len(pokes) == 5


async def test_a_recorded_poke_never_runs_again(env: s.testing.Env) -> None:
    async with env.worker([poller], preset="service", continue_as_new_after=0):
        run = await env.start(poller, {"n": 2, "every": 1, "timeout": 10})
        await env.advance(s.minutes(1))
        assert await run.result() == "ready"
        assert len(pokes) == 2


@s.workflow
async def flaky(c: s.Context) -> int:
    def check() -> int | None:
        pokes.append(T0)
        if len(pokes) == 1:
            raise RuntimeError("provider down")
        return len(pokes)

    retry = s.Retry(max_attempts=2, initial=s.seconds(1), max=s.seconds(1), multiplier=1)
    return await c.poll("check", check, every=s.minutes(1), timeout=s.minutes(5), retry=retry)


async def test_each_poke_retries_as_its_step_says(env: s.testing.Env) -> None:
    async with env.worker([flaky], preset="service"):
        run = await env.start(flaky)
        await env.advance(s.seconds(1))
        assert await run.result() == 2


@pytest.mark.parametrize(
    ("every", "timeout", "message"),
    [(0, 10, "every is a positive timedelta"), (1, -1, "timeout is a timedelta")],
)
async def test_poll_checks_every_and_timeout(
    env: s.testing.Env, every: int, timeout: int, message: str
) -> None:
    async with env.worker([poller], preset="service"):
        run = await env.start(poller, {"n": 1, "every": every, "timeout": timeout})
        with pytest.raises(s.RunFailed, match=message):
            await run.result()
