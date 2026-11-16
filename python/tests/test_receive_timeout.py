"""`c.receive(name, timeout=)`: the next message of a name, or `WaitTimeout` once the timeout
passed with none. The TypeScript and Rust hosts port these tests under the same names."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest

import substantial as s

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)


@pytest.fixture
async def env() -> AsyncIterator[s.testing.Env]:
    async with s.testing.Env.open(now=T0) as env:
        yield env
        await env.check()


@s.workflow
async def receiver(c: s.Context) -> dict[str, Any]:
    try:
        return {"value": await c.receive("note", timeout=s.minutes(5))}
    except s.WaitTimeout as e:
        return {"timeout": str(e), "at": (await c.now()).isoformat()}


async def test_a_message_within_the_timeout_is_returned(env: s.testing.Env) -> None:
    async with env.worker([receiver], preset="service"):
        run = await env.start(receiver)
        await env.advance(s.minutes(4))
        await run.send("note", "hello")
        assert await run.result() == {"value": "hello"}


async def test_a_none_value_is_returned_as_such(env: s.testing.Env) -> None:
    async with env.worker([receiver], preset="service"):
        run = await env.start(receiver)
        await run.send("note", None)
        assert await run.result() == {"value": None}


async def test_none_within_the_timeout_raises_wait_timeout_at_its_instant(
    env: s.testing.Env,
) -> None:
    async with env.worker([receiver], preset="service"):
        run = await env.start(receiver)
        await env.advance(s.minutes(5))
        assert await run.result() == {
            "timeout": "no message 'note' within 0:05:00",
            "at": "2030-01-07T09:05:00+00:00",
        }


@s.workflow
async def interleaved(c: s.Context) -> list[Any]:
    seen: list[int] = []
    c.handle("n", seen.append)
    first = await c.receive("n")
    second = await c.receive("n", timeout=s.minutes(5))
    try:
        await c.receive("n", timeout=s.minutes(5))
        timed_out = False
    except s.WaitTimeout:
        timed_out = True
    third = await c.receive("n")
    fourth = await c.receive("n", timeout=s.minutes(1))
    return [first, second, timed_out, third, fourth, seen]


async def test_receives_with_and_without_timeout_interleave_in_order(
    env: s.testing.Env,
) -> None:
    async with env.worker([interleaved], preset="service"):
        run = await env.start(interleaved)
        await run.send("n", 1)
        await run.send("n", 2)
        await env.advance(s.minutes(6))
        await run.send("n", 3)
        await run.send("n", 4)
        assert await run.result() == [1, 2, True, 3, 4, [1, 2, 3, 4]]


@s.workflow
async def pair(c: s.Context) -> list[Any]:
    waits = [c.receive("n", timeout=s.minutes(5)), c.receive("n", timeout=s.minutes(5))]
    results = await asyncio.gather(*waits, return_exceptions=True)
    return [r if not isinstance(r, Exception) else type(r).__name__ for r in results]


async def test_two_receives_waiting_together_take_one_message_each(env: s.testing.Env) -> None:
    async with env.worker([pair], preset="service"):
        run = await env.start(pair)
        await env.advance(s.minutes(1))
        await run.send("n", 1)
        await env.advance(s.minutes(5))
        assert await run.result() == [1, "WaitTimeout"]


@s.workflow
async def mixed(c: s.Context, timed_first: bool) -> str:
    try:
        if timed_first:
            timed = c.receive("n", timeout=s.minutes(5))
            try:
                c.receive("n")
            finally:
                timed.close()  # type: ignore[attr-defined]
        else:
            c.receive("n")
            await c.receive("n", timeout=s.minutes(5))
    except s.InvalidArgument as e:
        return str(e)
    return "accepted"


@pytest.mark.parametrize("timed_first", [True, False])
async def test_a_receive_with_and_one_without_timeout_pending_at_once_are_refused(
    env: s.testing.Env, timed_first: bool
) -> None:
    async with env.worker([mixed], preset="service"):
        run = await env.start(mixed, {"timed_first": timed_first})
        result = await run.result()
        assert "pending at once" in result, result
