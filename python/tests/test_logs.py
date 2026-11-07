"""`c.log` (I-14): muted while a replay walks the recorded history."""

from __future__ import annotations

import logging
from datetime import timedelta

import pytest
from conftest import LONG, script

from substantial import Context, Permanent, RunFailed, Substantial, step, workflow
from substantial.stores import Store


@step
async def work(n: int) -> int:
    return n


@workflow
async def logging_workflow(c: Context) -> int:
    c.log.info("before")
    a = await work(1)
    await c.sleep(timedelta(milliseconds=100))
    b = await work(2)
    c.log.warning("after %d", a + b)
    return a + b


async def test_logs_are_muted_while_a_replay_walks_the_history(
    store: Store, caplog: pytest.LogCaptureFixture
) -> None:
    sub = await Substantial._create(store, partitions=1)
    with caplog.at_level(logging.INFO, logger="substantial.workflow"):
        async with script(sub, [logging_workflow]):
            run = await sub.start(logging_workflow)
            assert await run.result(timeout=LONG) == 3
    records = [r for r in caplog.records if r.name == "substantial.workflow"]
    assert [r.getMessage() for r in records] == ["before", "after 3"]
    assert all(r.run == run.chain for r in records)  # type: ignore[attr-defined]
    assert all(r.filename == "test_logs.py" for r in records), "logged where the code calls"


async def test_a_run_that_fails_or_is_cancelled_logs_one_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    @step
    async def boom() -> None:
        raise Permanent("no stock")

    @workflow
    async def failing(c: Context) -> None:
        await c.sleep(timedelta(milliseconds=100))
        await boom()

    @workflow
    async def waiting(c: Context) -> None:
        await c.receive("never")

    sub = await Substantial.create("memory://logs-ends", partitions=1)
    with caplog.at_level(logging.WARNING, logger="substantial"):
        async with script(sub, [failing, waiting]):
            failed = await sub.start(failing, id="fails")
            cancelled = await sub.start(waiting, id="cancelled")
            await cancelled.cancel("not needed")
            for run in (failed, cancelled):
                with pytest.raises(RunFailed):
                    await run.result(timeout=LONG)
    ends = [r.getMessage() for r in caplog.records if r.name == "substantial"]
    # The failing run replays twice, after its sleep: still one line.
    assert sorted(ends) == [
        "run cancelled cancelled: not needed",
        "run fails failed: StepFailed: step boom failed after 1 attempt: no stock",
    ], ends
    assert all(r.levelno == logging.WARNING for r in caplog.records if r.name == "substantial")
