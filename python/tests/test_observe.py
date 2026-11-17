"""What a `Substantial` tells its observer, in a test environment and on the real clock."""

from __future__ import annotations

import asyncio
import secrets
import time
from datetime import UTC, datetime, timedelta

import pytest
from conftest import script

import substantial as s

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)
S3 = "aws-s3-standard-us-east-1-2026-11-09"


class Recorder:
    def __init__(self) -> None:
        self.seen: list[s.StoreOp | s.Attempt | s.Replay | s.Scan] = []

    def __call__(self, event: s.StoreOp | s.Attempt | s.Replay | s.Scan) -> None:
        self.seen.append(event)

    def store_ops(self) -> list[s.StoreOp]:
        return [e for e in self.seen if isinstance(e, s.StoreOp)]

    def attempts(self) -> list[s.Attempt]:
        return [e for e in self.seen if isinstance(e, s.Attempt)]

    def replays(self) -> list[s.Replay]:
        return [e for e in self.seen if isinstance(e, s.Replay)]

    def scans(self) -> list[s.Scan]:
        return [e for e in self.seen if isinstance(e, s.Scan)]


def flaky() -> s.Workflow[[], int]:
    """A step that fails once, retried, one that fails at once, then a sleep."""
    tries = [0]

    async def once() -> int:
        tries[0] += 1
        if tries[0] == 1:
            raise ValueError("once")
        return tries[0]

    async def no() -> int:
        raise ValueError("no")

    @s.workflow
    async def flaky(c: s.Context) -> int:
        retry = s.Retry(max_attempts=2, initial=s.seconds(1), max=s.seconds(1), multiplier=1.0)
        value = await c.step("once", once, retry=retry)
        with pytest.raises(s.StepFailed):
            await c.step("no", no)
        await c.sleep(s.minutes(1))
        return value

    return flaky


async def test_every_kind_of_event_with_its_fields() -> None:
    seen = Recorder()
    wf = flaky()
    async with (
        s.testing.Env.open(now=T0, prices=s.Prices(S3), observe=seen) as env,
        env.worker([wf], preset="service"),
    ):
        run = await env.start(wf)
        await env.advance(s.minutes(2))
        assert await run.result() == 2

    ops = seen.store_ops()
    per = {"create": 5000, "list": 5000, "get": 400, "delete": 0}
    outcomes = {
        "create": {"created", "exists"},
        "get": {"found", "missing"},
        "list": {"listed"},
        "delete": {"deleted"},
    }
    for op in ops:
        assert op.cost == s.Amount("USD", per[op.op] * op.attempts), op
        assert op.attempts >= 1 and op.seconds >= 0, op
        assert op.outcome in outcomes[op.op], op
    assert {op.op for op in ops} == set(per), ops

    tried = []
    for a in seen.attempts():
        assert a.workflow == "flaky" and a.seconds >= 0, a
        tried.append((a.step, a.attempt, a.outcome))
    assert tried == [("once", 1, "retry"), ("once", 2, "ok"), ("no", 1, "failed")]

    replays = seen.replays()
    assert all(r.workflow == "flaky" for r in replays)
    ends = [r.end for r in replays]
    assert ends[-1] == "ended" and "suspended" in ends, replays
    assert all(r.delay is not None and r.delay >= 0 for r in replays), "each serves a due entry"

    scans = seen.scans()
    assert all(scan.workflow == "flaky" for scan in scans)
    assert any(scan.due > 0 for scan in scans), scans


async def test_events_match_by_keyword() -> None:
    def kind(event: s.StoreOp | s.Attempt | s.Replay | s.Scan) -> str:
        match event:
            case s.Attempt(step=step, outcome="failed"):
                return f"failed {step}"
            case s.Replay(delay=delay) if delay is not None:
                return "late"
            case s.StoreOp(cost=s.Amount(nano=nano)):
                return f"{nano} nano"
            case _:
                return "other"

    assert kind(s.Attempt("w", "pay", 1, 0.5, "failed")) == "failed pay"
    assert kind(s.Replay("w", 1.5, 0.1, "idle")) == "late"
    assert kind(s.StoreOp("get", 0.1, 2, "found", s.Amount("USD", 800))) == "800 nano"
    assert kind(s.Scan("w", 3)) == "other"


async def test_attempts_that_time_out_or_are_left_behind() -> None:
    """On the real clock: an attempt past its timeout, then one left behind by a shutdown."""
    seen = Recorder()

    async def late() -> int:
        await asyncio.sleep(5)
        return 0

    async def slow() -> int:
        await asyncio.sleep(3600)
        return 0

    @s.workflow
    async def leaving(c: s.Context) -> int:
        with pytest.raises(s.StepFailed):
            await c.step("late", late, timeout=timedelta(milliseconds=100))
        return await c.step("slow", slow)

    url = f"memory://observe-{secrets.token_hex(4)}"
    async with s.Substantial.create(url, observe=seen) as sub:
        worker = script(sub, [leaving], shutdown_grace=timedelta(milliseconds=100))
        async with worker:
            await sub.start(leaving)
            deadline = time.monotonic() + 5
            while not seen.attempts():
                assert time.monotonic() < deadline, "no attempt ended"
                await asyncio.sleep(0.02)
            # The second step runs once the timeout is recorded.
            await asyncio.sleep(0.3)
    assert [(a.step, a.outcome) for a in seen.attempts()] == [
        ("late", "timeout"),
        ("slow", "abandoned"),
    ]
    assert seen.replays()[-1].end == "dropped", seen.replays()


async def test_a_store_operation_retried_reports_its_attempts() -> None:
    seen = Recorder()

    @s.workflow
    async def steps(c: s.Context) -> int:
        def ident(i: int) -> int:
            return i

        for i in range(5):
            await c.step("s", ident, i)
        return 0

    chaos = s.testing.Chaos(seed=7, rate=0.3)
    async with (
        s.testing.Env.open(now=T0, chaos=chaos, observe=seen) as env,
        env.worker([steps], preset="service"),
    ):
        run = await env.start(steps)
        await env.advance(s.minutes(5))
        assert await run.result() == 0
        assert env.faults.failed > 0
    ops = seen.store_ops()
    assert any(op.attempts > 1 and op.outcome != "failed" for op in ops), ops
    assert all(op.cost is None for op in ops), "no prices, no cost"


class ObserverDown(Exception):
    pass


async def test_an_observer_that_raises_stops_the_worker_or_fails_the_client_call() -> None:
    def on_scans(event: s.StoreOp | s.Attempt | s.Replay | s.Scan) -> None:
        if isinstance(event, s.Scan):
            raise ObserverDown("observer down")

    @s.workflow
    async def w(c: s.Context) -> int:
        return 0

    async with s.testing.Env.open(now=T0, observe=on_scans) as env:
        worker = env.worker([w], preset="service")
        with pytest.raises(ObserverDown, match="observer down"):
            async with worker:
                await env.settle()
        assert not worker.alive and isinstance(worker.error, ObserverDown)

    def on_ops(event: s.StoreOp | s.Attempt | s.Replay | s.Scan) -> None:
        if isinstance(event, s.StoreOp):
            raise ObserverDown("observer down")

    with pytest.raises(ObserverDown, match="observer down"):
        await s.testing.Env.open(now=T0, observe=on_ops)
    with pytest.raises(ObserverDown, match="observer down"):
        await s.Substantial.create(f"memory://observe-{secrets.token_hex(4)}", observe=on_ops)


async def test_a_store_operation_of_a_worker_whose_observer_raises_stops_it() -> None:
    def on_deletes(event: s.StoreOp | s.Attempt | s.Replay | s.Scan) -> None:
        if isinstance(event, s.StoreOp) and event.op == "delete":
            raise ObserverDown("observer down")

    @s.workflow
    async def w(c: s.Context) -> int:
        await c.sleep(s.minutes(1))
        return 0

    async with s.testing.Env.open(now=T0, observe=on_deletes) as env:
        worker = env.worker([w], preset="service")
        with pytest.raises(ObserverDown, match="observer down"):
            async with worker:
                await env.start(w)
                await env.advance(s.minutes(2))
        assert not worker.alive and isinstance(worker.error, ObserverDown)
