"""Undo, rollback and sagas (I-10), end to end."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from conftest import LONG, script

from substantial import (
    Context,
    Failure,
    InvalidArgument,
    Permanent,
    RunFailed,
    RunStatus,
    StepFailed,
    Substantial,
    Workflow,
    step,
    workflow,
)
from substantial.step import SAGA
from substantial.stores import Store


def undoable(c: Context, log: list[Any], name: str, *args: Any) -> Any:
    """A step returning `name` whose undo logs what it was given: the value, then the
    arguments."""

    def undo(value: str, *given: Any) -> None:
        log.append([value, *given] if given else value)

    return c.step(name, lambda *_: name, *args, undo=undo)


async def rolled_back(sub: Substantial, w: Workflow) -> Failure:
    async with script(sub, [w]):
        run = await sub.start(w)
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
    assert failed.value.status == RunStatus.FAILED
    return failed.value.error


async def test_a_rollback_undoes_every_step_most_recent_first(store: Store) -> None:
    log: list[Any] = []

    @workflow
    async def rolling(c: Context) -> None:
        await undoable(c, log, "a")
        await c.step("plain", lambda: None)
        # A replay after the sleep binds the undos again from the recorded steps.
        await c.sleep(timedelta(milliseconds=100))
        await undoable(c, log, "c", 1, "x")
        await c.rollback("why")

    sub = await Substantial._create(store, partitions=1)
    error = await rolled_back(sub, rolling)
    assert (error.type, error.message) == ("RolledBack", "why")
    assert log == [["c", 1, "x"], "a"]


async def test_a_saga_undoes_its_own_steps_then_raises_again(store: Store) -> None:
    log: list[Any] = []

    def boom() -> None:
        raise Permanent("no stock")

    @workflow
    async def saga(c: Context) -> None:
        await undoable(c, log, "a")
        try:
            async with c.saga():
                await undoable(c, log, "b")
                await undoable(c, log, "c")
                await c.step("boom", boom)
        except StepFailed as e:
            assert e.failure.message.endswith("no stock"), e.failure
        else:
            raise AssertionError("the saga did not raise again")
        async with c.saga():
            await c.step("fine", lambda: None)
        await c.rollback("after the saga")

    sub = await Substantial._create(store, partitions=1)
    error = await rolled_back(sub, saga)
    assert error.type == "RolledBack", error
    assert log == ["c", "b", "a"]


async def test_an_undo_without_attempts_left_fails_the_run(store: Store) -> None:
    def refuse(payment: int, card: str) -> None:
        raise Permanent("refund refused")

    @step(undo=refuse)
    def charge(card: str) -> int:
        return 1

    @workflow
    async def stuck(c: Context) -> None:
        await charge("visa")
        await c.rollback("fraud")

    sub = await Substantial._create(store, partitions=1)
    error = await rolled_back(sub, stuck)
    assert error.type == "UndoFailed", error
    assert "charge.undo" in error.message, error


async def test_a_step_of_a_saga_that_ended_is_invalid(store: Store) -> None:
    """The core refuses a step that names a saga no longer open."""

    @workflow
    async def late(c: Context) -> str:
        saga = c.saga()
        async with saga:
            pass
        SAGA.set(saga._id)
        try:
            await c.step("late", lambda: 1)
        except InvalidArgument:
            return "refused"
        return "accepted"

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [late]):
        run = await sub.start(late)
        assert await run.result(timeout=LONG) == "refused"


async def test_an_inner_saga_ended_hands_its_steps_to_the_outer_one(store: Store) -> None:
    log: list[Any] = []

    def boom() -> None:
        raise Permanent("no stock")

    @workflow
    async def nested(c: Context) -> None:
        await undoable(c, log, "z")
        try:
            async with c.saga():
                await undoable(c, log, "a")
                async with c.saga():
                    await undoable(c, log, "b")
                await undoable(c, log, "c")
                await c.step("boom", boom)
        except StepFailed:
            pass
        else:
            raise AssertionError("the saga did not raise again")
        await c.rollback("after")

    sub = await Substantial._create(store, partitions=1)
    error = await rolled_back(sub, nested)
    assert error.type == "RolledBack", error
    assert log == ["c", "b", "a", "z"]
