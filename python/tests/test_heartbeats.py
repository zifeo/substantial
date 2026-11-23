"""Heartbeats of step attempts (I-6): an attempt silent past its heartbeat timeout fails with a
retryable `StepTimeout`, one that beats goes on. The ports of `rust/tests/heartbeats.rs`, under
the same names."""

from __future__ import annotations

import asyncio
import secrets
import time
from datetime import UTC, datetime, timedelta

import pytest
from conftest import LONG, script

import substantial as s
from substantial import (
    Context,
    InvalidArgument,
    NotInWorkflow,
    Retry,
    StepFailed,
    Substantial,
    UndoFailed,
    heartbeat,
    workflow,
)
from substantial.stores.memory import MemoryStore

BEAT = timedelta(milliseconds=150)
T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)


async def working(c: Context, name: str, beats: bool, own: timedelta | None) -> str:
    """A step that runs 500 ms, beating every 50 ms when `beats`: `ok`, or its cause's type and
    message."""

    async def work() -> None:
        for _ in range(10):
            await asyncio.sleep(0.05)
            if beats:
                heartbeat()

    try:
        await c.step(name, work, timeout=timedelta(seconds=5), heartbeat_timeout=own)
    except StepFailed as e:
        return f"{e.cause.type}: {e.cause.message}"
    return "ok"


def sync_work() -> str:
    """A step in a thread that runs 500 ms and beats every 50 ms."""
    for _ in range(10):
        time.sleep(0.05)
        heartbeat()
    return "ok"


@workflow
async def own(c: Context) -> list[str]:
    return [
        await working(c, "silent", False, BEAT),
        await working(c, "beating", True, BEAT),
        await working(c, "unset", False, None),
        await c.step("threaded", sync_work, heartbeat_timeout=BEAT),
    ]


@workflow(heartbeat_timeout=BEAT)
async def inherited(c: Context) -> list[str]:
    return [
        await working(c, "silent", False, None),
        await working(c, "patient", False, timedelta(seconds=2)),
    ]


async def test_a_silent_attempt_fails_and_a_beating_one_goes_on() -> None:
    sub = await Substantial._create(MemoryStore(f"heartbeats-{secrets.token_hex(4)}"))
    silent = "StepTimeout: no heartbeat within 150 ms"
    async with script(sub, [own, inherited]):
        ended = await (await sub.start(own)).result(timeout=LONG)
        # § vvp37rp40@1f6q3
        assert ended == [silent, "ok", "ok", "ok"]
        ended = await (await sub.start(inherited)).result(timeout=LONG)
        assert ended == [silent, "ok"]


async def test_a_silent_attempt_is_retried_and_an_undo_has_its_steps_heartbeat() -> None:
    attempts: list[int] = []

    async def flaky() -> int:
        n = len(attempts)
        attempts.append(n)
        for _ in range(10):
            await asyncio.sleep(0.05)
            if n > 0:
                heartbeat()
        return n

    @workflow
    async def retried(c: Context) -> int:
        retry = Retry(
            max_attempts=2,
            initial=timedelta(milliseconds=10),
            max=timedelta(milliseconds=10),
            multiplier=1.0,
        )
        return await c.step("flaky", flaky, retry=retry, heartbeat_timeout=BEAT)

    async def slow_undo(value: int) -> None:
        await asyncio.sleep(0.5)

    @workflow
    async def undone(c: Context) -> str:
        await c.step("charge", lambda: 1, heartbeat_timeout=BEAT, undo=slow_undo)
        try:
            await c.rollback("refused")
        except UndoFailed as e:
            return f"{e.step}: {e.failure.message}"
        raise AssertionError("the undo did not time out")

    sub = await Substantial._create(MemoryStore(f"heartbeats-{secrets.token_hex(4)}"))
    async with script(sub, [retried, undone]):
        assert await (await sub.start(retried)).result(timeout=LONG) == 1, "the second attempt beat"
        assert attempts == [0, 1]
        ended = await (await sub.start(undone)).result(timeout=LONG)
        # § vvp37rp40.b@xy99c
        assert ended.startswith("charge.undo: "), ended
        assert "no heartbeat within 150 ms" in ended, ended


def test_a_heartbeat_outside_a_step_raises() -> None:
    # § vvp37rp40.a@nyxt3
    with pytest.raises(NotInWorkflow):
        heartbeat()


def test_a_heartbeat_timeout_is_a_positive_timedelta() -> None:
    for invalid in [timedelta(0), timedelta(seconds=-1), 5]:
        with pytest.raises(InvalidArgument):
            workflow(heartbeat_timeout=invalid)(working)  # type: ignore[arg-type]
        with pytest.raises(InvalidArgument):
            s.step(heartbeat_timeout=invalid)(sync_work)  # type: ignore[arg-type]


async def test_a_held_attempt_is_silent_past_its_heartbeat_timeout() -> None:
    async def export() -> None:
        pass

    @workflow
    async def held(c: Context) -> str:
        try:
            await c.step("export", export, timeout=s.minutes(10), heartbeat_timeout=s.minutes(1))
        except StepFailed as e:
            return e.cause.type
        return "ok"

    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([held], preset="service"),
    ):
        hold = env.hold("export")
        run = await env.start(held)
        await env.advance(s.minutes(1) - timedelta(milliseconds=1))
        assert hold.held == 1, "still within its heartbeat timeout"
        await env.advance(timedelta(milliseconds=1))
        assert hold.held == 0
        assert await run.result() == "StepTimeout"
        await env.check()
