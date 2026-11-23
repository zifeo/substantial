"""Time, step keys, handlers, waits, sends, cancellation from inside, publishing and versions, end
to end."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from conftest import LONG, blocked_with, eventually, script

from substantial import (
    Cancelled,
    Context,
    InvalidArgument,
    RolledBack,
    RunEnded,
    RunFailed,
    RunNotFound,
    RunStatus,
    Substantial,
    step,
    step_key,
    workflow,
)
from substantial.stores import Store


async def test_now_sleep_until_and_step_keys(store: Store) -> None:
    @workflow
    async def timed(c: Context) -> list[object]:
        start = await c.now()
        await c.sleep_until(start + timedelta(milliseconds=300))
        end = await c.now()
        keys = [await c.step("keyed", step_key) for _ in range(2)]
        return [(end - start).total_seconds(), keys]

    sub = await Substantial._create(store)
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
    sub = await Substantial._create(store)
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
    sub = await Substantial._create(store)
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

    sub = await Substantial._create(store)
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

    sub = await Substantial._create(store)
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

    sub = await Substantial._create(store)
    run = await sub.start(ordered)
    for name, n in [("add", 1), ("remove", 1), ("add", 2), ("remove", 2)]:
        await run.send(name, n)
    await run.send("go")
    async with script(sub, [ordered]):
        # § n4zyj94mr@hwd6e
        assert await run.result(timeout=LONG) == {
            "seen": ["add:1", "remove:1", "add:2", "remove:2"],
            "adds": [1, 2],
        }


async def test_a_wait_returns_false_when_its_timeout_passes(store: Store) -> None:
    @workflow
    async def impatient(c: Context) -> list[bool]:
        names = approvals(c)
        late = await c.wait_until(lambda: len(names) >= 1, timeout=timedelta(milliseconds=200))
        c.publish("late", late)
        on_time = await c.wait_until(lambda: len(names) >= 1, timeout=timedelta(seconds=30))
        return [late, on_time]

    sub = await Substantial._create(store)
    async with script(sub, [impatient]):
        run = await sub.start(impatient)

        async def late() -> bool:
            return await run.read("late") is False

        await eventually("the first wait's end", late)
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
                except InvalidArgument as e:
                    stated = "must match [A-Za-z0-9._-]{1,128}" in str(e)
                    refused.append(f"{bad} {stated}")
        return refused

    sub = await Substantial._create(store)
    async with script(sub, [names]):
        run = await sub.start(names)
        refused = await run.result(timeout=LONG)
        assert refused == ["substantial.ended False"] * 2 + ["a note True"] * 2 + [" True"] * 2


async def test_reserved_names_are_refused_to_a_client_send_and_a_step(store: Store) -> None:
    with pytest.raises(InvalidArgument, match="reserved"):

        @step(name="substantial.charge")
        async def reserved() -> None: ...

    @workflow
    async def stepper(c: Context) -> bool:
        try:
            c.step("substantial.charge", lambda: None)
        except InvalidArgument:
            return True
        return False

    sub = await Substantial._create(store)
    run = await sub.start(stepper)
    with pytest.raises(InvalidArgument, match="reserved"):
        await run.send("substantial.cancel", "forged")
    async with script(sub, [stepper]):
        assert await run.result(timeout=LONG) is True


async def test_a_run_cancels_itself(store: Store) -> None:
    @workflow
    async def quitter(c: Context) -> None:
        await c.step("first", lambda: None)
        await c.cancel("enough")

    sub = await Substantial._create(store)
    async with script(sub, [quitter]):
        run = await sub.start(quitter)
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
        assert failed.value.status == RunStatus.CANCELLED
        assert failed.value.error.message == "enough"


async def test_a_caught_cancel_or_rollback_ends_the_run_as_the_code_ends_it(store: Store) -> None:
    @workflow
    async def caught(c: Context) -> list[str]:
        seen = []
        try:
            await c.cancel("changed my mind")
        except Cancelled as e:
            seen.append(e.reason)
        try:
            await c.rollback("not this way")
        except RolledBack as e:
            seen.append(e.reason)
        return seen

    sub = await Substantial._create(store)
    async with script(sub, [caught]):
        run = await sub.start(caught)
        assert await run.result(timeout=LONG) == ["changed my mind", "not this way"]
        assert await run.status() == RunStatus.COMPLETED


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

    sub = await Substantial._create(store)
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


async def test_a_published_value_is_read_while_its_run_waits(store: Store) -> None:
    @workflow
    async def waiting_publisher(c: Context) -> int:
        c.publish("p", 1)
        return await c.receive("go")

    sub = await Substantial._create(store)
    async with script(sub, [waiting_publisher]):
        run = await sub.start(waiting_publisher)

        async def published() -> bool:
            return await run.read("p") == 1

        # § fx1tv8nqp.a@056jm
        await eventually("the published value", published)
        assert await run.status() == RunStatus.SUSPENDED
        # § end fx1tv8nqp.a
        await run.send("go", 2)
        assert await run.result(timeout=LONG) == 2


def versioned(version: int | None):
    @workflow(name="versioned", version=version)
    async def code(c: Context) -> str:
        return "none" if version is None else f"v{version}"

    return code


async def test_runs_are_replayed_by_the_code_of_their_version(store: Store) -> None:
    from substantial import ConfigError

    sub = await Substantial._create(store)
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
    """Version 1 steps then waits for `go`, version 2 waits for `go` then steps: the same name, no
    version. Waiting on a message keeps the run suspended until the test sends it."""

    async def one() -> int:
        return 1

    @workflow(name="changed")
    async def code(c: Context) -> int:
        if first:
            await c.step("one", one)
            await c.receive("go")
        else:
            await c.receive("go")
            await c.step("one", one)
        return 1

    return code


async def test_an_incompatible_change_blocks_the_run_until_the_code_is_rolled_back(
    store: Store,
) -> None:
    sub = await Substantial._create(store)
    async with script(sub, [changed(True)]):
        run = await sub.start(changed(True))

        async def suspended() -> bool:
            return await run.status() == RunStatus.SUSPENDED

        await eventually("the wait", suspended)
    # The worker that blocks the run sets when it is tried again, whatever the next worker's
    # backoff: kept short, the rolled back code runs soon however long this phase took.
    backoff = {"blocked_initial": timedelta(milliseconds=50), "blocked_max": timedelta(seconds=0.2)}
    async with script(sub, [changed(False)], **backoff):
        await run.send("go", "now")
        # § rbwstdetg@rk5m6
        error = await blocked_with(sub, run)
    assert error.type == "NonDeterminism", error
    async with script(sub, [changed(True)]):
        assert await run.result(timeout=LONG) == 1
    # § end rbwstdetg
