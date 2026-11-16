"""`worker.drain(within=)`: a worker woken on a schedule serves what is due by its horizon, then
stops. The TypeScript and Rust hosts port these tests under the same names."""

from __future__ import annotations

import secrets
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


@s.step
def ship(order: str) -> str:
    return f"shipped {order}"


@s.workflow
async def order(c: s.Context, order: str, wait: int) -> str:
    await c.sleep(s.seconds(wait))
    return await ship(order)


async def test_drain_serves_a_run_due_now_then_stops(env: s.testing.Env) -> None:
    run = await env.start(order, {"order": "o1", "wait": 0})
    worker = env.worker([order], preset="script")
    await worker.drain(within=s.minutes(1))
    assert not worker.alive and worker.error is None
    assert await run.result() == "shipped o1"
    assert env.now == T0


async def test_drain_serves_a_run_due_within_its_horizon(env: s.testing.Env) -> None:
    run = await env.start(order, {"order": "o1", "wait": 30})
    await env.worker([order], preset="script").drain(within=s.minutes(1))
    assert await run.result() == "shipped o1"
    assert env.now == T0 + timedelta(seconds=30)


async def test_drain_leaves_a_run_due_after_its_horizon_for_later(env: s.testing.Env) -> None:
    run = await env.start(order, {"order": "o1", "wait": 120})
    await env.worker([order], preset="script").drain(within=s.minutes(1))
    assert env.now == T0
    assert await run.status() == "suspended"
    await env.advance(s.minutes(2))
    await env.worker([order], preset="script").drain(within=s.minutes(1))
    assert await run.result() == "shipped o1"


async def test_drain_waits_for_a_crashed_workers_claim_expiring_within_its_horizon(
    env: s.testing.Env,
) -> None:
    held = env.hold("ship")
    async with env.worker([order], preset="script") as crashed:
        run = await env.start(order, {"order": "o1", "wait": 0})
        await env.settle()
        assert held.held == 1
        crashed.crash()
    await env.worker([order], preset="script").drain(within=s.minutes(1))
    assert await run.result() == "shipped o1"
    # Past claim_ttl plus claim_skew of the crashed worker's claim.
    assert env.now == T0 + timedelta(seconds=35, milliseconds=1)


async def test_drain_raises_the_workers_error(env: s.testing.Env) -> None:
    class ObserverDown(Exception):
        pass

    def observe(event: object) -> None:
        if isinstance(event, s.Attempt):
            raise ObserverDown("observer down")

    async with s.testing.Env.open(now=T0, observe=observe) as failing:
        await failing.start(order, {"order": "o1", "wait": 0})
        worker = failing.worker([order], preset="script")
        with pytest.raises(ObserverDown, match="observer down"):
            await worker.drain(within=s.minutes(1))
        assert isinstance(worker.error, ObserverDown)
        await failing.settle()


async def test_drain_checks_within(env: s.testing.Env) -> None:
    worker = env.worker([order], preset="script")
    with pytest.raises(s.InvalidArgument, match="within"):
        await worker.drain(within=timedelta(seconds=-1))


async def test_drain_on_the_real_clock() -> None:
    async with s.Substantial.create(f"memory://drain-{secrets.token_hex(4)}") as sub:
        runs = [await sub.start(order, {"order": f"o{i}", "wait": 0}) for i in range(3)]
        late = await sub.start(order, {"order": "late", "wait": 3600})
        worker = sub.worker([order], preset="script")
        await worker.drain(within=s.seconds(1))
        assert [await r.result() for r in runs] == ["shipped o0", "shipped o1", "shipped o2"]
        assert await late.status() == "suspended"
        assert not worker.alive and worker.error is None
