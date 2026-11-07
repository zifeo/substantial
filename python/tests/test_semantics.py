"""Time, step keys, handlers, waits, sends, cancellation from inside, publishing and versions, end
to end."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from conftest import LONG, eventually, script

from substantial import (
    Context,
    InvalidArgument,
    RunEnded,
    RunFailed,
    RunNotFound,
    RunStatus,
    Substantial,
    step_key,
    workflow,
)
from substantial.stores import Store


async def test_now_sleep_until_and_step_keys(store: Store) -> None:
    @workflow
    async def timed(c: Context) -> tuple[float, list[str]]:
        start = await c.now()
        await c.sleep_until(start + timedelta(milliseconds=300))
        end = await c.now()
        keys = [await c.step("keyed", step_key) for _ in range(2)]
        return (end - start).total_seconds(), keys

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [timed]):
        run = await sub.start(timed, id="timed-1")
        slept, keys = await run.result(timeout=LONG)
        assert slept >= 0.3
        assert keys == ["timed-1/keyed/0", "timed-1/keyed/1"]


def approvals(c: Context) -> list[str]:
    """The distinct names approving, bound to the messages `approve`."""
    names: list[str] = []

    def approve(name: str) -> None:
        if name not in names:
            names.append(name)

    c.handle("approve", approve)
    return names


@workflow
async def approver(c: Context, target: str) -> list[str]:
    names = approvals(c)
    await c.wait_until(lambda: len(names) >= 2)
    await c.send(target, "note", ",".join(names))
    return names


@workflow
async def waiting(c: Context) -> str:
    return await c.receive("note")


async def test_a_handler_sees_past_messages_and_a_workflow_sends(store: Store) -> None:
    sub = await Substantial._create(store, partitions=1)
    target = await sub.start(waiting, id="target")
    run = await sub.start(approver, {"target": "target"})
    for name in ["alice", "alice", "bob"]:
        await run.send("approve", name)
    async with script(sub, [approver, waiting]):
        assert await run.result(timeout=LONG) == ["alice", "bob"]
        assert await target.result(timeout=LONG) == "alice,bob"


@workflow
async def refused(c: Context, ended: str) -> list[list[str]]:
    seen = []
    for target in ["nobody", ended]:
        try:
            await c.send(target, "note")
        except (RunNotFound, RunEnded) as e:
            seen.append(f"{type(e).__name__}: {e}")
    live = await c.step("live", lambda: seen)
    await c.receive("go")
    return [live, seen]


async def test_a_workflow_send_raises_and_every_replay_gives_the_same_outcome(
    store: Store,
) -> None:
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [refused, waiting]):
        ended = await sub.start(waiting, id="ended")
        await ended.send("note", "hi")
        assert await ended.result(timeout=LONG) == "hi"
        run = await sub.start(refused, {"ended": "ended"})

        async def suspended() -> bool:
            return await run.status() == RunStatus.SUSPENDED

        await eventually("the sends recorded", suspended)
        # A read again would now find the chain: the replay gives the recorded outcome.
        await sub.start(waiting, id="nobody")
        await run.send("go")
        expected = ["RunNotFound: no chain nobody", "RunEnded: run ended has ended, completed"]
        assert await run.result(timeout=LONG) == [expected, expected]


async def test_a_gather_returning_exceptions_lets_a_suspension_through(store: Store) -> None:
    gathered: list[list[object]] = []

    @workflow
    async def both(c: Context) -> list[object]:
        got = await asyncio.gather(c.receive("a"), c.receive("b"), return_exceptions=True)
        gathered.append(got)
        return got

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [both]):
        run = await sub.start(both)
        await run.send("a", 1)

        async def suspended() -> bool:
            return await run.status() == RunStatus.SUSPENDED

        await eventually("a suspension with one message", suspended)
        await run.send("b", 2)
        assert await run.result(timeout=LONG) == [1, 2]
    assert gathered == [[1, 2]]


async def test_handlers_see_every_message_and_receive_takes_the_next(store: Store) -> None:
    @workflow
    async def notes(c: Context) -> dict[str, object]:
        seen: list[str] = []
        first = await c.receive("note")
        c.handle("note", seen.append)
        await c.wait_until(lambda: len(seen) >= 3)
        return {"first": first, "seen": seen, "second": await c.receive("note")}

    sub = await Substantial._create(store, partitions=1)
    run = await sub.start(notes)
    for note in ["a", "b", "c"]:
        await run.send("note", note)
    async with script(sub, [notes]):
        assert await run.result(timeout=LONG) == {
            "first": "a",
            "seen": ["a", "b", "c"],
            "second": "b",
        }


async def test_handlers_bound_together_see_past_messages_in_arrival_order(store: Store) -> None:
    @workflow
    async def ordered(c: Context) -> dict[str, list]:
        await c.receive("go")
        seen: list[str] = []
        c.handle(
            {
                "add": lambda n: seen.append(f"add:{n}"),
                "remove": lambda n: seen.append(f"remove:{n}"),
            }
        )
        adds: list[int] = []
        c.handle("add", adds.append)
        return {"seen": seen, "adds": adds}

    sub = await Substantial._create(store, partitions=1)
    run = await sub.start(ordered)
    for name, n in [("add", 1), ("remove", 1), ("add", 2), ("remove", 2)]:
        await run.send(name, n)
    await run.send("go")
    async with script(sub, [ordered]):
        assert await run.result(timeout=LONG) == {
            "seen": ["add:1", "remove:1", "add:2", "remove:2"],
            "adds": [1, 2],
        }


async def test_a_wait_returns_false_when_its_timeout_passes(store: Store) -> None:
    @workflow
    async def impatient(c: Context) -> list[bool]:
        names = approvals(c)
        late = await c.wait_until(lambda: len(names) >= 1, timeout=timedelta(milliseconds=200))
        on_time = await c.wait_until(lambda: len(names) >= 1, timeout=timedelta(seconds=30))
        return [late, on_time]

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [impatient]):
        run = await sub.start(impatient)
        await asyncio.sleep(0.6)
        await run.send("approve", "alice")
        assert await run.result(timeout=LONG) == [False, True]


async def test_reserved_and_invalid_names_are_refused_in_a_workflow(store: Store) -> None:
    @workflow
    async def names(c: Context) -> list[str]:
        refused = []
        for bad in ["substantial.ended", "a note", ""]:
            for call in (lambda b: c.receive(b), lambda b: c.handle(b, print)):
                try:
                    call(bad)
                except InvalidArgument:
                    refused.append(bad)
        return refused

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [names]):
        run = await sub.start(names)
        refused = await run.result(timeout=LONG)
        assert refused == ["substantial.ended"] * 2 + ["a note"] * 2 + [""] * 2


async def test_a_run_cancels_itself(store: Store) -> None:
    @workflow
    async def quitter(c: Context) -> None:
        await c.step("first", lambda: None)
        await c.cancel("enough")

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [quitter]):
        run = await sub.start(quitter)
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
        assert failed.value.status == RunStatus.CANCELLED
        assert failed.value.error.message == "enough"


async def test_published_values_are_read_by_clients(store: Store) -> None:
    @workflow
    async def publisher(c: Context) -> bool:
        c.publish("progress", {"done": 1})
        await c.step("work", lambda: None)
        c.publish("progress", {"done": 2})
        await c.receive("go")
        try:
            c.publish("Not A Key", 0)
        except InvalidArgument:
            return True
        return False

    sub = await Substantial._create(store, partitions=1)
    run = await sub.start(publisher, id="pub-1")
    assert await run.read("progress") is None
    async with script(sub, [publisher]):

        async def published() -> bool:
            return await run.read("progress") == {"done": 2}

        await eventually("the last value published", published)
        await run.send("go")
        assert await run.result(timeout=LONG) is True
    assert await run.read("progress") == {"done": 2}
    assert await run.read("other") is None
    with pytest.raises(InvalidArgument):
        await run.read("Not A Key")


def versioned(version: int | None):
    @workflow(name="versioned", version=version)
    async def code(c: Context) -> str:
        return "none" if version is None else f"v{version}"

    return code


async def test_runs_are_replayed_by_the_code_of_their_version(store: Store) -> None:
    from substantial import ConfigError

    sub = await Substantial._create(store, partitions=1)
    with pytest.raises(ConfigError):
        script(sub, [versioned(None), versioned(2), versioned(2)])
    none = await sub.start(versioned(None))
    async with script(sub, [versioned(1)]):
        one = await sub.start(versioned(1))
        assert await one.result(timeout=LONG) == "v1"
        await asyncio.sleep(0.3)
        assert await none.status() == RunStatus.PENDING, "a version-only worker leaves it"
    async with script(sub, [versioned(None), versioned(1)]):
        two = await sub.start(versioned(2))
        assert await none.result(timeout=LONG) == "none"
        await asyncio.sleep(0.3)
        assert await two.status() == RunStatus.PENDING
        async with script(sub, [versioned(None), versioned(1), versioned(2)]):
            assert await two.result(timeout=LONG) == "v2"


def changed(first: bool):
    """Version 1 steps then sleeps, version 2 sleeps then steps: the same name, no version."""

    async def one() -> int:
        return 1

    @workflow(name="changed")
    async def code(c: Context) -> int:
        if first:
            await c.step("one", one)
            await c.sleep(timedelta(milliseconds=300))
        else:
            await c.sleep(timedelta(milliseconds=300))
            await c.step("one", one)
        return 1

    return code


async def test_an_incompatible_change_fails_the_run_with_non_determinism(store: Store) -> None:
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [changed(True)]):
        run = await sub.start(changed(True))

        async def suspended() -> bool:
            return await run.status() == RunStatus.SUSPENDED

        await eventually("the sleep", suspended)
    async with script(sub, [changed(False)]):
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
    assert failed.value.status == "failed"
    assert failed.value.error.type == "NonDeterminism"
    assert str(failed.value).endswith("; an incompatible change needs a new version (version=)")
