"""A run's end cancels its children still running, unless started with `abandon`; a replay
claims its run only before a step attempt. The TypeScript and Rust hosts port these tests under
the same names."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from conftest import LONG, eventually, script

import substantial as s
from substantial import worker
from substantial._proto import boundary, layout
from substantial.clock import Clock
from substantial.stores import Store

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)


@pytest.fixture
async def env() -> AsyncIterator[s.testing.Env]:
    async with s.testing.Env.open(now=T0) as env:
        yield env
        await env.check()


@s.workflow
async def waiter(c: s.Context) -> str:
    return await c.receive("go")


@s.workflow
async def leaving(c: s.Context, fail: bool) -> str:
    child = await c.start(waiter, id="kid")
    if fail:
        raise s.Fail("Done", "the parent gave up")
    return child.chain


@s.workflow
async def abandoning(c: s.Context) -> str:
    child = await c.start(waiter, id="kid", abandon=True)
    return child.chain


async def cancelled_with(env: s.testing.Env) -> str:
    with pytest.raises(s.RunFailed) as failed:
        await env.run(waiter, "kid").result()
    assert failed.value.status == "cancelled"
    return failed.value.error.message


async def test_a_completing_parent_cancels_its_running_child(env: s.testing.Env) -> None:
    async with env.worker([leaving, waiter], preset="service"):
        run = await env.start(leaving, {"fail": False}, id="p")
        assert await run.result() == "kid"
        await env.settle()
        # § fca4h4ker@1da5b
        assert await env.run(waiter, "kid").status() == "cancelled"
        # § end fca4h4ker
        # § fca4h4ker.b@hw0jv
        assert await cancelled_with(env) == "the parent run p completed"
        # § end fca4h4ker.b


async def test_a_failing_parent_cancels_its_running_child(env: s.testing.Env) -> None:
    async with env.worker([leaving, waiter], preset="service"):
        run = await env.start(leaving, {"fail": True}, id="p")
        with pytest.raises(s.RunFailed):
            await run.result()
        await env.settle()
        # § fca4h4ker.a@ch9e3
        assert await cancelled_with(env) == "the parent run p failed"
        # § end fca4h4ker.a


async def test_an_abandoned_child_goes_on_and_ends_on_its_own(env: s.testing.Env) -> None:
    async with env.worker([abandoning, waiter], preset="service"):
        run = await env.start(abandoning, id="p")
        assert await run.result() == "kid"
        await env.settle()
        child = env.run(waiter, "kid")
        # § fca4h4ker@cf2f8
        assert await child.status() == "suspended", "its parent's end did not cancel it"
        # § end fca4h4ker
        await child.send("go", "now")
        assert await child.result() == "now"


def leases(monkeypatch: pytest.MonkeyPatch) -> list[bool]:
    """Whether each head the workers write holds a lease, in order."""
    leased: list[bool] = []
    perform = worker.perform

    async def recorded(store: Store, op: boundary.StoreOp) -> boundary.OpResult:
        if op.WhichOneof("op") == "cas" and op.cas.key.startswith("runs/"):
            run = layout.Run.FromString(op.cas.bytes)
            leased.append(run.WhichOneof("kind") == "head" and run.head.HasField("lease"))
        return await perform(store, op)

    monkeypatch.setattr(worker, "perform", recorded)
    return leased


@s.workflow
async def stepping(c: s.Context) -> int:
    await c.receive("go")
    return await c.step("one", lambda: 1)


async def test_a_replay_claims_its_run_only_before_a_step(
    env: s.testing.Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    leased = leases(monkeypatch)
    async with env.worker([stepping], preset="service"):
        run = await env.start(stepping, id="r")
        await env.settle()
        # § j1nq6rqpf@fspmb
        assert leased and not any(leased), "a replay that runs no step"
        await run.send("go", None)
        assert await run.result() == 1
        assert any(leased), "the lease taken before the step"
        # § end j1nq6rqpf
        # § j1nq6rqpf.c@ar0wv
        assert not leased[-1], "the end releases it"
        # § end j1nq6rqpf.c


class Ahead(Clock):
    """The real clock, a minute ahead."""

    def now(self) -> layout.Time:
        return layout.Time(ms=super().now().ms + 60_000)


async def test_a_worker_whose_clock_is_behind_a_runs_last_segment_leaves_it_alone() -> None:
    told: list[s.Skewed] = []
    sub = await s.Substantial.create(
        "memory://skewed", observe=lambda e: told.append(e) if isinstance(e, s.Skewed) else None
    )
    async with sub:
        # A minute ahead, past `claim_skew`, 5 s in the script preset: its segment is in the
        # future of every right clock.
        ahead = script(sub, [waiter])
        ahead._clock = Ahead()
        async with ahead:
            run = await sub.start(waiter)
            await eventually("the wait", lambda: _is(run, "suspended"))
        async with script(sub, [waiter]):
            await run.send("go", "now")
            # § kr6b4hm8k@4yy50
            await eventually("the skew told", lambda: _told(told), within=LONG.total_seconds())
            [skew] = told[:1]
            assert skew.run == run.chain
            # Not continued: the chain's current run is its first.
            assert skew.current == run.chain
            assert skew.segment_at - skew.now > timedelta(seconds=55), skew
            assert await run.status() == "suspended", "left alone"
            # § end kr6b4hm8k


async def _is(run: s.Run, status: str) -> bool:
    return await run.status() == status


async def _told(told: list[s.Skewed]) -> bool:
    await asyncio.sleep(0)
    return bool(told)
