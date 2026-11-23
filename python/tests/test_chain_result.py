"""`c.result(chain, timeout=)`: a workflow awaits the end of any chain, a child of its own or not.
The TypeScript and Rust hosts port these tests under the same names."""

from __future__ import annotations

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
async def quick(c: s.Context) -> int:
    return 1


@s.workflow
async def waiter(c: s.Context) -> str:
    return await c.receive("go")


@s.workflow
async def doomed(c: s.Context) -> None:
    raise s.Fail("ValueError", await c.receive("go"))


@s.workflow
async def relay(c: s.Context, left: int) -> str:
    """Continues as new at each `go` while `left`, then ends with the last one's value."""
    got = await c.receive("go")
    if left > 0:
        await c.continue_as_new({"left": left - 1})
    return got


@s.workflow
async def watcher(c: s.Context, chain: str) -> dict[str, Any]:
    try:
        return {"value": await c.result(chain)}
    except s.RunFailed as e:
        return {"status": e.status, "error": e.error.message}
    except s.RunNotFound as e:
        return {"not_found": str(e)}


ALL = [quick, waiter, doomed, relay, watcher]


async def test_a_chain_ended_before_the_watch_answers_at_once(env: s.testing.Env) -> None:
    async with env.worker(ALL, preset="service"):
        target = await env.start(quick)
        assert await target.result() == 1
        run = await env.start(watcher, {"chain": target.chain})
        assert await run.result() == {"value": 1}


async def test_a_chain_that_ends_while_watched_wakes_the_watcher(env: s.testing.Env) -> None:
    async with env.worker(ALL, preset="service"):
        target = await env.start(waiter)
        run = await env.start(watcher, {"chain": target.chain})
        await env.settle()
        assert await run.status() == "suspended"
        await target.send("go", "done")
        assert await run.result() == {"value": "done"}


async def test_a_failed_chain_raises_run_failed(env: s.testing.Env) -> None:
    async with env.worker(ALL, preset="service"):
        target = await env.start(doomed)
        run = await env.start(watcher, {"chain": target.chain})
        await env.settle()
        await target.send("go", "boom")
        assert await run.result() == {"status": "failed", "error": "boom"}


async def test_a_cancelled_chain_raises_run_failed_cancelled(env: s.testing.Env) -> None:
    async with env.worker(ALL, preset="service"):
        target = await env.start(waiter)
        run = await env.start(watcher, {"chain": target.chain})
        await env.settle()
        await target.cancel("enough")
        assert await run.result() == {"status": "cancelled", "error": "enough"}


async def test_a_chain_that_continued_is_awaited_to_its_last_run(env: s.testing.Env) -> None:
    async with env.worker(ALL, preset="service"):
        target = await env.start(relay, {"left": 1})
        run = await env.start(watcher, {"chain": target.chain})
        await env.settle()
        await target.send("go", "first")
        await env.settle()
        assert await run.status() == "suspended"
        await target.send("go", "second")
        assert await run.result() == {"value": "second"}


async def test_a_removed_chain_raises_run_not_found(env: s.testing.Env) -> None:
    async with env.worker(ALL, preset="service"):
        target = await env.start(quick, id="q")
        assert await target.result() == 1
        await env.advance(s.days(31))
        run = await env.start(watcher, {"chain": "q"})
        assert await run.result() == {"not_found": "no chain q"}


async def test_a_missing_chain_raises_run_not_found(env: s.testing.Env) -> None:
    async with env.worker(ALL, preset="service"):
        run = await env.start(watcher, {"chain": "nobody"})
        assert await run.result() == {"not_found": "no chain nobody"}


@s.workflow
async def patient(c: s.Context, chain: str) -> list[Any]:
    try:
        await c.result(chain, timeout=s.minutes(5))
    except s.WaitTimeout as e:
        return [str(e), (await c.now()).isoformat(), await c.result(chain)]
    raise AssertionError("the chain ended before the timeout")


async def test_the_timeout_raises_wait_timeout_then_awaiting_again_gets_the_value(
    env: s.testing.Env,
) -> None:
    async with env.worker([waiter, patient], preset="service"):
        target = await env.start(waiter)
        run = await env.start(patient, {"chain": target.chain})
        await env.advance(s.minutes(6))
        await target.send("go", "late")
        assert await run.result() == [
            f"run {target.chain} did not end before the timeout",
            "2030-01-07T09:05:00+00:00",
            "late",
        ]


async def test_two_watchers_of_one_chain_both_get_its_end(env: s.testing.Env) -> None:
    async with env.worker(ALL, preset="service"):
        target = await env.start(waiter)
        runs = [await env.start(watcher, {"chain": target.chain}) for _ in range(2)]
        await env.settle()
        await target.send("go", "both")
        assert [await r.result() for r in runs] == [{"value": "both"}] * 2


@s.workflow
async def narcissist(c: s.Context) -> list[str]:
    refused = []
    for chain in (c.chain, "Not An Id"):
        try:
            c.result(chain)
        except s.InvalidArgument as e:
            refused.append(str(e))
    return refused


async def test_waiting_on_its_own_chain_raises_invalid_argument(env: s.testing.Env) -> None:
    async with env.worker([narcissist], preset="service"):
        run = await env.start(narcissist, id="me")
        own, invalid = await run.result()
        assert "cannot await its own chain me" in own
        assert invalid.startswith("run id 'Not An Id' must match")


@s.workflow
async def resumed(c: s.Context, chain: str) -> str:
    value = await c.result(chain)
    await c.receive("resume")
    return value


async def test_the_outcome_replays(env: s.testing.Env) -> None:
    async with env.worker([waiter, resumed], preset="service"):
        target = await env.start(waiter)
        run = await env.start(resumed, {"chain": target.chain})
        await env.settle()
        await target.send("go", "kept")
        await env.settle()
        # The watched chain is removed: a result read again would raise RunNotFound.
        await env.advance(s.days(31))
        with pytest.raises(s.RunNotFound):
            await target.status()
    async with env.worker([waiter, resumed], preset="service"):
        await run.send("resume")
        assert await run.result() == "kept"
    history = await run.history()
    watched = [e.data for e in history if e.kind == "watched"]
    assert [(w["chain"], w["waiting"]) for w in watched] == [(target.chain, target.chain)]
    ended = [e.data["ended"] for e in history if e.data.get("name") == "substantial.watched"]
    assert [(e["status"], e["value"]) for e in ended] == [("completed", "kept")]
