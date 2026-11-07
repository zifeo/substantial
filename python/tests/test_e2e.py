"""Clients and workers end to end, on every adapter of this host."""

from __future__ import annotations

import asyncio
import math
import re
from datetime import timedelta

import pytest
from conftest import LONG, eventually, script

from substantial import (
    Context,
    InvalidArgument,
    Retry,
    RunEnded,
    RunExists,
    RunFailed,
    RunNotFound,
    RunStatus,
    Substantial,
    WaitTimeout,
    minutes,
    seconds,
    step,
    workflow,
)
from substantial.stores import Store


@step(retry=Retry(max_attempts=6, initial=seconds(1), max=minutes(1), multiplier=2))
def roulette() -> int:
    return 100


@step
def multiply(a: float, b: float) -> float:
    return a * b


@workflow
async def example(c: Context) -> float:
    """The workflow of I-3, its sleep shortened."""
    res = await roulette()
    await c.sleep(timedelta(milliseconds=200))
    res = await multiply(res, 0.5)
    n = await c.receive("by")
    res = await multiply(res, n)
    stopped: list[None] = []
    c.handle("stop", stopped.append)
    await c.wait_until(lambda: bool(stopped))
    return res


async def test_a_first_workflow(store: Store) -> None:
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [example]):
        run = await sub.start(example)
        await run.send("by", 2)
        await run.send("stop")
        assert await run.result(timeout=LONG) == 100
        assert await run.status() == RunStatus.COMPLETED


@step
async def first() -> None:
    return None


@workflow
async def waiting(c: Context) -> str:
    """Records a step, so that the run has a segment, then waits for a note."""
    await first()
    return await c.receive("note")


async def test_status_moves_from_pending_to_the_end(store: Store) -> None:
    sub = await Substantial._create(store, partitions=2)
    run = await sub.start(waiting, id="status-1")
    assert await run.status() == "pending"
    async with script(sub, [waiting]):

        async def suspended() -> bool:
            return await run.status() == RunStatus.SUSPENDED

        await eventually("a suspended run", suspended)
        with pytest.raises(WaitTimeout):
            await run.result(timeout=timedelta(milliseconds=50))
        await run.send("note", "hi", id="note-1")
        assert await run.result(timeout=LONG) == "hi"
        assert await run.status() == RunStatus.COMPLETED
        with pytest.raises(RunEnded):
            await run.send("note", "again")


async def test_a_waiting_run_is_cancelled(store: Store) -> None:
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [waiting]):
        run = await sub.start(waiting)

        async def suspended() -> bool:
            return await run.status() == RunStatus.SUSPENDED

        await eventually("a suspended run", suspended)
        await run.cancel("not needed")
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
        assert failed.value.status == RunStatus.CANCELLED
        assert failed.value.error.message == "not needed"
        assert await run.status() == RunStatus.CANCELLED


TICKS: list[int] = []


async def tick() -> None:
    TICKS.append(1)
    await asyncio.sleep(0.02)


@workflow
async def looping(c: Context) -> None:
    while True:
        await c.step("tick", tick)


async def test_a_cancel_reaches_a_run_looping_on_steps(store: Store) -> None:
    TICKS.clear()
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [looping]):
        run = await sub.start(looping)

        async def ticking() -> bool:
            return len(TICKS) > 2

        await eventually("a few steps", ticking)
        await run.cancel("stop looping")
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
        assert failed.value.status == RunStatus.CANCELLED


async def test_unknown_chains_are_not_found(store: Store) -> None:
    sub = await Substantial._create(store, partitions=1)
    run = sub.run(waiting, "nobody")
    for call in (run.send("by", 1), run.cancel("x"), run.status(), run.result(timeout=LONG)):
        with pytest.raises(RunNotFound):
            await call


async def test_starts_are_idempotent_by_id(store: Store) -> None:
    runs: list[int] = []

    @step
    def count() -> int:
        runs.append(1)
        return len(runs)

    @workflow
    async def counted(c: Context, **fields: object) -> dict[str, object]:
        await count()
        return fields

    sub = await Substantial._create(store, partitions=1)
    first = await sub.start(counted, {"x": 1}, id="once")
    again = await sub.start(counted, {"x": 1}, id="once")
    assert first.chain == again.chain
    with pytest.raises(RunExists):
        await sub.start(counted, {"x": 2}, id="once")
    with pytest.raises(InvalidArgument):
        await sub.start(counted, 3)  # type: ignore[arg-type]
    with pytest.raises(InvalidArgument):
        await sub.start(counted, {"x": object()})
    async with script(sub, [counted]):
        assert await first.result(timeout=LONG) == {"x": 1}
    assert len(runs) == 1, "one run"


async def test_inputs_are_compared_as_canonical_json(store: Store) -> None:
    @workflow
    async def echo(c: Context, **fields: object) -> dict[str, object]:
        return fields

    sub = await Substantial._create(store, partitions=1)
    first = await sub.start(echo, {"b": [1.0, {"d": 2, "c": "é"}], "a": 0.5}, id="same")
    again = await sub.start(echo, {"a": 0.5, "b": [1, {"c": "é", "d": 2.0}]}, id="same")
    assert first.chain == again.chain
    for bad in (math.nan, math.inf, -math.inf, 2**53, -(2**53)):
        with pytest.raises(InvalidArgument, match="the input is not JSON"):
            await sub.start(echo, {"x": [bad]})
    async with script(sub, [echo]):
        assert await first.result(timeout=LONG) == {"a": 0.5, "b": [1, {"c": "é", "d": 2}]}


async def test_names_outside_the_rules_are_invalid_arguments(store: Store) -> None:
    sub = await Substantial._create(store, partitions=1)
    long = "a" * 101
    key_rule = re.escape("must match [a-z0-9._-]{1,100}")
    for bad in ["Upper", ".", "..", "a/b", long]:
        with pytest.raises(InvalidArgument, match=f"run id .* {key_rule}"):
            await sub.start(waiting, id=bad)
        run = sub.run(waiting, bad)
        for call in (
            run.send("note", "x"),
            run.status(),
            run.cancel("x"),
            run.result(timeout=LONG),
        ):
            with pytest.raises(InvalidArgument):
                await call
    run = await sub.start(waiting, id="fine")
    for bad in ["Upper", "..", long]:
        with pytest.raises(InvalidArgument, match=f"message id .* {key_rule}"):
            await run.send("note", "x", id=bad)
    with pytest.raises(InvalidArgument, match=re.escape("must match [A-Za-z0-9._-]{1,128}")):
        await run.send("a note", "x")

    with pytest.raises(InvalidArgument, match=f"workflow name 'Upper' {key_rule}"):

        @workflow(name="Upper")
        async def upper(c: Context) -> None:
            return None


async def test_ids_of_a_hundred_bytes_work_end_to_end(adapter: str, store: Store) -> None:
    if adapter == "memory":
        pytest.skip("names reach store keys only on stores with paths")
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [waiting]):
        run = await sub.start(waiting, id="r" * 100)
        await run.send("note", "hi", id="m" * 100)
        assert await run.result(timeout=LONG) == "hi"


async def test_a_step_name_outside_the_rules_fails_the_run(store: Store) -> None:
    @workflow(name="bad-step")
    async def bad(c: Context) -> int:
        return await c.step("a step", lambda: 1)

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [bad]):
        run = await sub.start(bad)
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
        assert failed.value.status == RunStatus.FAILED
        assert failed.value.error.type == "InvalidArgument"


async def test_a_message_no_handler_can_decode_fails_the_run(store: Store) -> None:
    @workflow
    async def typed(c: Context) -> None:
        seen: list[int] = []

        def take(n: int) -> None:
            seen.append(n)

        c.handle("n", take)
        await c.wait_until(lambda: len(seen) >= 2)

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [typed]):
        run = await sub.start(typed)
        await run.send("n", 1)
        await run.send("n", "not a number")
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
        assert failed.value.status == RunStatus.FAILED
        assert failed.value.error.type == "MessageDecode", failed.value


CHARGES: list[str] = []


async def charge(card: str) -> str:
    CHARGES.append(card)
    if len(CHARGES) == 1:
        raise ConnectionError("declined, try again")
    return f"pay-{card}"


@workflow
async def lambda_checkout(c: Context) -> list[str]:
    retry = Retry(max_attempts=3, initial=timedelta(milliseconds=10), max=seconds(1), multiplier=2)
    payment = await c.step("charge", lambda: charge("visa"), retry=retry)
    return [payment, await c.receive("go")]


async def test_a_lambda_returning_a_coroutine_is_a_step(store: Store) -> None:
    CHARGES.clear()
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [lambda_checkout]):
        run = await sub.start(lambda_checkout)

        async def charged() -> bool:
            return len(CHARGES) == 2 and await run.status() == RunStatus.SUSPENDED

        await eventually("the second attempt recorded", charged)
        await run.send("go", "now")
        assert await run.result(timeout=LONG) == ["pay-visa", "now"]
    assert CHARGES == ["visa", "visa"], "one failed attempt, one recorded, none on replay"
