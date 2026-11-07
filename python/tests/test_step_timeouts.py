"""Every step attempt has a timeout: the step's own, else its workflow's, else its worker's; and
`StepFailed.cause`, the last attempt's failure."""

from __future__ import annotations

import asyncio
import secrets
import time
from datetime import timedelta

import pytest
from conftest import LONG, script
from test_replay import Harness

from substantial import (
    Context,
    Failure,
    InvalidArgument,
    Permanent,
    RunFailed,
    StepFailed,
    Substantial,
    UndoFailed,
    Workflow,
    workflow,
)
from substantial.stores.memory import MemoryStore

SHORT = timedelta(milliseconds=200)
LONGER = timedelta(seconds=2)


async def nap() -> str:
    await asyncio.sleep(0.5)
    return "ok"


async def napping(c: Context, own: float | None) -> str:
    try:
        return await c.step("nap", nap, timeout=None if own is None else timedelta(seconds=own))
    except StepFailed as e:
        return e.cause.type


@workflow
async def plain(c: Context, own: float | None = None) -> str:
    return await napping(c, own)


@workflow(step_timeout=SHORT)
async def short(c: Context, own: float | None = None) -> str:
    return await napping(c, own)


@workflow(step_timeout=LONGER)
async def longer(c: Context, own: float | None = None) -> str:
    return await napping(c, own)


@pytest.mark.parametrize(
    ("w", "own", "expected"),
    [
        (plain, None, "StepTimeout"),
        (longer, None, "ok"),
        (short, LONGER.total_seconds(), "ok"),
        (longer, SHORT.total_seconds(), "StepTimeout"),
    ],
    ids=["the worker's", "the workflow's over the worker's", "own larger", "own smaller"],
)
async def test_a_step_timeout_is_its_own_else_its_workflow_s_else_its_worker_s(
    w: Workflow, own: float | None, expected: str
) -> None:
    sub = await Substantial._create(MemoryStore(f"timeouts-{secrets.token_hex(4)}"), partitions=1)
    async with script(sub, [w], step_timeout=SHORT):
        run = await sub.start(w, {"own": own})
        assert await run.result(timeout=LONG) == expected


def test_a_workflow_step_timeout_is_a_positive_timedelta() -> None:
    for invalid in [timedelta(0), timedelta(seconds=-1), 5]:
        with pytest.raises(InvalidArgument):
            workflow(step_timeout=invalid)(napping)  # type: ignore[arg-type]


class Declined(Permanent):
    pass


async def test_step_failed_holds_the_last_attempt_s_failure_as_its_cause() -> None:
    async def decline() -> None:
        raise Declined("card declined")

    @workflow(step_timeout=SHORT)
    async def failing(c: Context) -> list[list[object]]:
        causes = []
        for name, fn in [("decline", decline), ("nap", nap)]:
            try:
                await c.step(name, fn)
            except StepFailed as e:
                causes.append([e.cause.type, e.cause.message, e.cause.retryable])
        return causes

    assert (await Harness(failing).replay(1_000)).completed() == [
        ["Declined", "card declined", False],
        ["StepTimeout", "attempt past 200 ms", True],
    ]


async def test_a_failed_run_holds_the_cause_of_the_step_that_failed_it() -> None:
    def decline() -> None:
        raise Declined("card declined")

    @workflow
    async def paying(c: Context) -> None:
        await c.step("charge", decline)

    @workflow
    async def refusing(c: Context) -> None:
        raise Declined("not today")

    @workflow
    async def ordering(c: Context) -> list[object]:
        child = await c.start(paying)
        try:
            await child.result()
        except RunFailed as e:
            assert e.cause is not None
            return [e.error.type, e.cause.type, e.cause.message, e.cause.retryable]
        raise AssertionError("the child did not fail")

    sub = await Substantial._create(MemoryStore(f"causes-{secrets.token_hex(4)}"), partitions=1)
    async with script(sub, [paying, refusing, ordering]):
        with pytest.raises(RunFailed) as paid:
            await (await sub.start(paying)).result(timeout=LONG)
        assert paid.value.error.type == "StepFailed"
        assert paid.value.cause == Failure("Declined", "card declined", False)
        with pytest.raises(RunFailed) as refused:
            await (await sub.start(refusing)).result(timeout=LONG)
        assert refused.value.error.type == "Declined"
        assert refused.value.cause is None
        ordered = await (await sub.start(ordering)).result(timeout=LONG)
        assert ordered == ["StepFailed", "Declined", "card declined", False]


async def test_an_undo_has_the_timeout_of_its_step() -> None:
    async def undo_nap(value: str) -> None:
        await nap()

    @workflow(step_timeout=SHORT)
    async def undone(c: Context) -> str:
        await c.step("quick", lambda: "done", undo=undo_nap)
        try:
            await c.rollback("why")
        except UndoFailed as e:
            return str(e)
        raise AssertionError("the undo did not time out")

    message = (await Harness(undone).replay(1_000)).completed()
    assert message.endswith("attempt past 200 ms"), message


async def test_a_timed_out_async_step_is_interrupted() -> None:
    reached: list[str] = []

    async def hang() -> None:
        try:
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            reached.append("interrupted")
            raise
        reached.append("after the await")

    @workflow
    async def hanging(c: Context) -> str:
        try:
            await c.step("hang", hang, timeout=SHORT)
        except StepFailed as e:
            return e.cause.type
        return "returned"

    sub = await Substantial._create(MemoryStore(f"hang-{secrets.token_hex(4)}"), partitions=1)
    async with script(sub, [hanging]):
        assert await (await sub.start(hanging)).result(timeout=LONG) == "StepTimeout"
    await asyncio.sleep(0.6)
    assert reached == ["interrupted"]


async def test_an_uninterruptible_step_runs_to_its_end_with_its_outcome_dropped() -> None:
    finished: list[str] = []

    def block() -> str:
        time.sleep(0.4)
        finished.append("ended")
        return "late"

    @workflow
    async def blocking(c: Context) -> str:
        return await c.step("block", block, timeout=SHORT)

    sub = await Substantial._create(MemoryStore(f"block-{secrets.token_hex(4)}"), partitions=1)
    async with script(sub, [blocking]):
        with pytest.raises(RunFailed) as failed:
            await (await sub.start(blocking)).result(timeout=LONG)
    assert failed.value.cause is not None and failed.value.cause.type == "StepTimeout"
    await asyncio.sleep(0.6)
    assert finished == ["ended"]
