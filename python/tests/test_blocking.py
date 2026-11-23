"""An error of the code that is no end blocks its run: nothing is recorded, the run reads blocked
with its error, and it goes on once code that runs it is deployed (I-20). The TypeScript and Rust
hosts port these tests under the same names."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import pytest

import substantial as s

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)
DOUBLED: list[int] = []


class Broken(Exception):
    pass


def double(n: int) -> int:
    DOUBLED.append(n)
    return n * 2


def doubling(fixed: bool) -> s.Workflow[..., int]:
    @s.workflow(name="doubling")
    async def code(c: s.Context, n: int) -> int:
        x = await c.step("double", double, n)
        if not fixed:
            raise Broken(f"bug at {x}")
        return x

    return code


async def test_an_error_of_the_code_blocks_the_run_until_a_fix(
    caplog: pytest.LogCaptureFixture,
) -> None:
    DOUBLED.clear()
    told: list[s.Observed] = []
    caplog.set_level(logging.WARNING, logger="substantial")
    async with s.testing.Env.open(now=T0, observe=told.append) as env:
        async with env.worker([doubling(False)], preset="script"):
            run = await env.start(doubling(False), {"n": 2}, id="d")
            await env.settle()
            # § hzew2nw4v@c7d73
            assert await run.status() == "blocked"
            [info] = [i async for i in env.runs(doubling(False))]
            assert (info.waiting_on, info.error) == ("blocked", s.Failure("Broken", "bug at 4"))
            assert info.due_at == env.now + s.seconds(1)
            # § end hzew2nw4v
            assert await run.history() == [], "nothing recorded, the step since the last commit"
            blocked = [e for e in told if isinstance(e, s.Blocked)]
            assert blocked == [s.Blocked("doubling", "d", s.Failure("Broken", "bug at 4"))]
            assert "run d blocked: Broken: bug at 4" in caplog.text
            await env.advance(s.seconds(1))
            [info] = [i async for i in env.runs(doubling(False))]
            assert info.due_at == env.now + s.seconds(2), "the backoff doubles"
        async with env.worker([doubling(True)], preset="script"):
            await env.advance(s.seconds(2))
            assert await run.result() == 4
        assert DOUBLED == [2, 2, 2], "a step the replay did not commit runs again"
        assert [e.kind for e in await run.history()] == ["step", "completed"]
        assert await run.status() == "completed"


def waiting(first: bool) -> s.Workflow[..., int]:
    """Steps then waits for `go`, or, changed, waits for `go` then steps: the same name and no
    version."""

    @s.workflow(name="waiting")
    async def code(c: s.Context) -> int:
        if first:
            await c.step("one", lambda: 1)
            await c.receive("go")
        else:
            await c.receive("go")
            await c.step("one", lambda: 1)
        return 1

    return code


async def test_a_divergence_blocks_the_run_until_the_code_is_rolled_back() -> None:
    async with s.testing.Env.open(now=T0) as env:
        async with env.worker([waiting(True)], preset="script"):
            run = await env.start(waiting(True))
            await env.settle()
        await run.send("go")
        async with env.worker([waiting(False)], preset="script"):
            await env.settle()
            assert await run.status() == "blocked"
            [info] = [i async for i in env.runs(waiting(False))]
            assert info.error is not None and info.error.type == "NonDeterminism", info
        async with env.worker([waiting(True)], preset="script"):
            await env.advance(s.seconds(1))
            assert await run.result() == 1


def catching(first: bool) -> s.Workflow[..., int]:
    """`waiting`, whose code catches what its body raises and fails on purpose, as code that
    wraps a whole workflow in a handler does."""

    @s.workflow(name="waiting")
    async def code(c: s.Context) -> int:
        try:
            if first:
                await c.step("one", lambda: 1)
                await c.receive("go")
            else:
                await c.receive("go")
                await c.step("one", lambda: 1)
        except Exception as e:
            raise s.Fail("Caught", str(e)) from e
        return 1

    return code


async def test_a_divergence_the_code_catches_and_fails_on_still_blocks_the_run() -> None:
    async with s.testing.Env.open(now=T0) as env:
        async with env.worker([catching(True)], preset="script"):
            run = await env.start(catching(True))
            await env.settle()
        await run.send("go")
        async with env.worker([catching(False)], preset="script"):
            await env.settle()
            # § rbwstdetg.e@dxas1
            assert await run.status() == "blocked"
            [info] = [i async for i in env.runs(catching(False))]
            assert info.error is not None and info.error.type == "NonDeterminism", info
            # § end rbwstdetg.e
        async with env.worker([catching(True)], preset="script"):
            await env.advance(s.seconds(1))
            assert await run.result() == 1


@s.workflow
async def refusing(c: s.Context) -> None:
    raise s.Fail("Refused", "not today")


async def test_fail_ends_the_run_failed() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([refusing], preset="script"),
    ):
        run = await env.start(refusing)
        # § dxgjekysz.d@8eww8
        with pytest.raises(s.RunFailed) as failed:
            await run.result()
        assert failed.value.error == s.Failure("Refused", "not today")
        assert await run.status() == "failed"
        # § end dxgjekysz.d


@s.workflow
async def timestamped(c: s.Context) -> Any:
    return await c.now()


async def test_a_result_json_cannot_hold_blocks_the_run() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([timestamped], preset="script"),
    ):
        run = await env.start(timestamped)
        with pytest.raises(s.testing.Stuck, match="blocked by Json: the value of timestamped"):
            await run.result()
