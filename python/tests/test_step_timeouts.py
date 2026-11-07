"""Every step attempt has a timeout: the step's own, else its workflow's, else its worker's; and
`StepFailed.cause`, the last attempt's failure."""

from __future__ import annotations

import asyncio
import secrets
from datetime import timedelta

import pytest
from conftest import LONG, script
from test_replay import Harness

from substantial import (
    Context,
    InvalidArgument,
    Permanent,
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
