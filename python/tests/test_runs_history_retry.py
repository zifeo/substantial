"""Listing a workflow's live runs, reading a run's history and retrying a failed run."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import aclosing
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
async def waiter(c: s.Context, mode: str) -> str:
    if mode == "sleep":
        await c.sleep(s.days(1))
    elif mode == "receive":
        await c.receive("go")
    return mode


@s.workflow(name="tagged", version=2)
async def tagged(c: s.Context) -> None:
    await c.receive("go")


async def listed(env: s.Substantial, workflow: s.Workflow) -> dict[str, s.RunInfo]:
    infos = [info async for info in env.runs(workflow)]
    by_chain = {info.chain: info for info in infos}
    assert len(by_chain) == len(infos), "each run once"
    return by_chain


async def test_runs_lists_the_live_runs_and_what_they_wait_on(env: s.testing.Env) -> None:
    async with env.worker([waiter, tagged], preset="service"):
        for mode in ["sleep", "receive", "nudged", "done"]:
            await env.start(waiter, {"mode": "receive" if mode == "nudged" else mode}, id=mode)
        await env.start(tagged, id="tagged")
        await env.settle()
        assert await env.run(waiter, "done").result() == "done"
        await env.advance(s.minutes(1))
        await env.start(waiter, {"mode": "done"}, id="pending")
        await env.run(waiter, "nudged").send("go")
        runs = await listed(env, waiter)
        assert set(runs) == {"sleep", "receive", "nudged", "pending"}
        assert runs["pending"] == s.RunInfo(
            chain="pending",
            run="pending",
            version=None,
            due_at=T0 + s.minutes(1),
            waiting_on="start",
        )
        assert (runs["sleep"].waiting_on, runs["sleep"].due_at) == ("time", T0 + s.days(1))
        assert (runs["receive"].waiting_on, runs["receive"].due_at) == ("message", None)
        assert (runs["nudged"].waiting_on, runs["nudged"].due_at) == ("inbox", T0 + s.minutes(1))
        assert (await listed(env, tagged))["tagged"].version == 2


async def test_runs_can_be_left_early(env: s.testing.Env, monkeypatch: pytest.MonkeyPatch) -> None:
    for i in range(3):
        await env.start(waiter, {"mode": "done"}, id=f"run-{i}")
    machines: list[str] = []
    new_machine, drop_machine = env._core.new_machine, env._core.drop_machine

    def new(*args: object) -> int:
        machines.append("new")
        return new_machine(*args)  # type: ignore[arg-type]

    def drop(machine: int) -> None:
        machines.append("drop")
        drop_machine(machine)

    monkeypatch.setattr(env._core, "new_machine", new)
    monkeypatch.setattr(env._core, "drop_machine", drop)
    async with aclosing(env.runs(waiter)) as infos:
        async for info in infos:
            assert info.waiting_on == "start"
            break
    assert machines == ["new", "drop"]
    assert len(await listed(env, waiter)) == 3
    with pytest.raises(s.InvalidArgument, match="runs takes a workflow"):
        await anext(env.runs("waiter"))  # type: ignore[arg-type]


@s.workflow
async def stepped(c: s.Context) -> str:
    await c.step("one", lambda: 1)
    await c.sleep(s.minutes(1))
    return "done"


async def test_history_gives_the_events_of_the_last_run(env: s.testing.Env) -> None:
    async with env.worker([stepped], preset="service"):
        run = await env.start(stepped, id="stepped")
        await env.advance(s.minutes(1))
        assert await run.result() == "done"
        events = await run.history()
    assert [e.kind for e in events] == ["step", "timer", "fired", "completed"]
    assert {"name": "one", "value": 1}.items() <= events[0].data.items()
    assert all(e.run == "stepped" for e in events)
    assert [e.segment for e in events] == sorted(e.segment for e in events)
    assert events[0].at == T0
    assert events[-1].at == T0 + s.minutes(1)
    with pytest.raises(s.RunNotFound):
        await env.run(stepped, "unknown").history()


async def test_retry_replays_the_recorded_steps_and_runs_the_failed_one_again(
    env: s.testing.Env,
) -> None:
    calls = {"a": 0, "b": 0}
    keys: list[str] = []
    broken = True

    def a() -> int:
        calls["a"] += 1
        return calls["a"]

    def b() -> str:
        calls["b"] += 1
        keys.append(s.step_key())
        if broken:
            raise s.Permanent("broken")
        return "b"

    @s.workflow
    async def flaky(c: s.Context) -> list[object]:
        return [await c.step("a", a), await c.step("b", b)]

    @s.workflow
    async def rolling(c: s.Context) -> None:
        await c.rollback("why")

    async with env.worker([flaky, rolling], preset="service"):
        run = await env.start(flaky, id="flaky")
        with pytest.raises(s.RunFailed):
            await run.result()
        broken = False
        await run.retry()
        assert await run.result() == [1, "b"]
        assert calls == {"a": 1, "b": 2}
        assert keys[0] == keys[1]
        with pytest.raises(s.InvalidArgument):
            await run.retry()
        with pytest.raises(s.InvalidArgument):
            await (await env.start(flaky, id="live")).retry()
        with pytest.raises(s.RunNotFound):
            await env.run(flaky, "unknown").retry()
        rolled = await env.start(rolling, id="rolled")
        with pytest.raises(s.RunFailed):
            await rolled.result()
        with pytest.raises(s.InvalidArgument):
            await rolled.retry()


async def test_retry_refuses_a_run_whose_failure_ran_undos(env: s.testing.Env) -> None:
    undone: list[str] = []

    def broken() -> None:
        raise s.Permanent("broken")

    @s.workflow
    async def undoing(c: s.Context) -> None:
        async with c.saga():
            await c.step("booked", lambda: "booked", undo=lambda value: undone.append(value))
            await c.step("broken", broken)

    async with env.worker([undoing], preset="service"):
        run = await env.start(undoing, id="undoing")
        with pytest.raises(s.RunFailed):
            await run.result()
        assert undone == ["booked"]
        with pytest.raises(s.InvalidArgument, match="ran undos"):
            await run.retry()
        with pytest.raises(s.RunFailed, match="broken"):
            await run.result()
