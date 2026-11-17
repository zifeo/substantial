"""`child.cancel`: a workflow cancels a child it started, as a client's `run.cancel` does. The
TypeScript and Rust hosts port these tests under the same names."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

import substantial as s

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
async def quick(c: s.Context) -> int:
    return 1


@s.workflow
async def canceller(c: s.Context) -> list[str]:
    child = await c.start(waiter)
    await child.cancel("no longer needed")
    try:
        await child.result()
    except s.RunFailed as e:
        return [child.chain, e.status, e.error.message]
    raise AssertionError("the child was not cancelled")


async def test_cancel_a_running_child(env: s.testing.Env) -> None:
    async with env.worker([canceller, waiter], preset="service"):
        run = await env.start(canceller)
        chain, status, reason = await run.result()
        assert (status, reason) == ("cancelled", "no longer needed")
        with pytest.raises(s.RunFailed) as failed:
            await env.run(waiter, chain).result()
        assert failed.value.status == "cancelled"


@s.workflow
async def late_canceller(c: s.Context) -> list[str]:
    child = await c.start(quick)
    await child.result()
    try:
        await child.cancel("too late")
    except s.RunEnded as e:
        return [child.chain, str(e)]
    raise AssertionError("cancelling an ended child did not raise")


async def test_cancel_a_child_that_ended_raises_run_ended(env: s.testing.Env) -> None:
    async with env.worker([late_canceller, quick], preset="service"):
        run = await env.start(late_canceller)
        chain, message = await run.result()
        assert message == f"run {chain} has ended, completed"


@s.workflow
async def suspending_canceller(c: s.Context) -> list[str]:
    child = await c.start(waiter)
    await child.cancel("no longer needed")
    await c.receive("resume")
    try:
        await child.result()
    except s.RunFailed as e:
        return [child.chain, e.status]
    raise AssertionError("the child was not cancelled")


async def test_the_cancel_outcome_replays(env: s.testing.Env) -> None:
    async with env.worker([suspending_canceller, waiter], preset="service"):
        run = await env.start(suspending_canceller)
        await env.settle()
    # The child ended cancelled: a cancel read again would raise RunEnded.
    async with env.worker([suspending_canceller, waiter], preset="service"):
        await run.send("resume")
        chain, status = await run.result()
        assert status == "cancelled"
    sent = [e.data for e in await run.history() if e.kind == "sent"]
    assert [(e["name"], e["value"]) for e in sent] == [("substantial.cancel", "no longer needed")]
    cancelled = [e for e in await env.run(waiter, chain).history() if e.kind == "cancelled"]
    assert len(cancelled) == 1


@s.workflow
async def odd_canceller(c: s.Context) -> str:
    child = await c.start(waiter)
    try:
        await child.cancel(42)  # type: ignore[arg-type]
    except s.InvalidArgument as e:
        await child.send("go", "done")
        await child.result()
        return str(e)
    raise AssertionError("a reason that is not a string was accepted")


async def test_a_non_string_reason_raises_invalid_argument(env: s.testing.Env) -> None:
    async with env.worker([odd_canceller, waiter], preset="service"):
        run = await env.start(odd_canceller)
        assert await run.result() == "a reason is a string, not 42"


@s.workflow
async def impostor(c: s.Context) -> list[str]:
    child = await c.start(waiter)
    refused = []
    for send in (
        lambda: child.send("substantial.cancel", "x"),
        lambda: c.send(child.chain, "substantial.cancel", "x"),
    ):
        try:
            send()
        except s.InvalidArgument as e:
            refused.append(str(e))
    await child.send("go", "done")
    await child.result()
    return refused


async def test_a_workflow_sending_substantial_cancel_raises_invalid_argument(
    env: s.testing.Env,
) -> None:
    async with env.worker([impostor, waiter], preset="service"):
        run = await env.start(impostor)
        message = "message name 'substantial.cancel' starts with substantial., which is reserved"
        assert await run.result() == [message, message]
