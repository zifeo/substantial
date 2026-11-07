"""The cap on every value the host encodes: past it, `ValueTooLarge`, and the core never sees it."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from conftest import LONG, Recording, script
from test_replay import Harness

from substantial import (
    Context,
    RunFailed,
    StepFailed,
    Substantial,
    ValueTooLarge,
    step,
    workflow,
)
from substantial._proto import MAX_VALUE
from substantial.stores.memory import MemoryStore

# Its JSON encoding, quoted, is two bytes past the cap.
LARGE = "x" * MAX_VALUE
FITS = "x" * (MAX_VALUE - 2)


async def test_a_step_value_past_the_cap_fails_the_step_at_once() -> None:
    attempts: list[int] = []

    @step
    async def big(value: str) -> str:
        attempts.append(1)
        return value

    @workflow
    async def stepping(c: Context) -> list[str]:
        assert len(await big(FITS)) == len(FITS)
        try:
            await big(LARGE)
        except StepFailed as e:
            return [e.failure.type, e.cause.type, e.failure.message]
        raise AssertionError("the step did not fail")

    kind, cause, message = (await Harness(stepping).replay(1_000)).completed()
    assert (kind, cause) == ("StepFailed", "ValueTooLarge")
    # The core keeps the message of the attempt's failure, not its type.
    assert message.endswith(f"bytes is past the limit of {MAX_VALUE} bytes"), message
    assert len(attempts) == 2, "a permanent failure, not retried"


@dataclass
class Holder:
    value: str = ""


@pytest.mark.parametrize("where", ["result", "publish", "send", "start", "state"])
async def test_values_of_the_code_past_the_cap_raise_in_the_code(where: str) -> None:
    @workflow
    async def child(c: Context) -> None:
        return None

    @workflow
    async def large(c: Context) -> str:
        if where == "result":
            return LARGE
        try:
            if where == "publish":
                c.publish("k", LARGE)
            elif where == "send":
                await c.send("other", "note", LARGE)
            elif where == "start":
                await c.start(child, {"v": LARGE})
            else:
                async for _ in c.iterate(Holder(LARGE)):
                    break
        except ValueTooLarge as e:
            return str(e)
        raise AssertionError(f"{where} took a value past the cap")

    replayed = await Harness(large).replay(1_000)
    if where == "result":
        failure = replayed.failed()
        assert failure is not None and failure.type == "ValueTooLarge", failure
        message = failure.message
    else:
        message = replayed.completed()
    assert f"bytes is past the limit of {MAX_VALUE} bytes" in message, message


async def test_a_state_past_the_cap_fails_the_compression() -> None:
    @workflow
    async def growing(c: Context) -> None:
        async for s in c.iterate(Holder()):
            s.value = LARGE

    sub = await Substantial.create("memory://values-state", partitions=1)
    async with script(sub, [growing], continue_as_new_after=0):
        run = await sub.start(growing)
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
    assert failed.value.error.type == "ValueTooLarge"


async def test_a_client_refuses_a_value_past_the_cap_before_the_store() -> None:
    @workflow
    async def waiting(c: Context) -> str:
        return await c.receive("note")

    recording = Recording(MemoryStore("values-client"))
    sub = await Substantial._create(recording, partitions=1)
    run = await sub.start(waiting, id="w")
    before = (list(recording.created), list(recording.listed))
    with pytest.raises(ValueTooLarge, match=f"limit of {MAX_VALUE} bytes"):
        await sub.start(waiting, {"v": LARGE})
    with pytest.raises(ValueTooLarge):
        await run.send("note", LARGE)
    assert (recording.created, recording.listed) == before, "no store operation"
    async with script(sub, [waiting]):
        await run.send("note", "fits")
        assert await run.result(timeout=LONG) == "fits"


async def test_a_run_whose_result_is_past_the_cap_fails() -> None:
    @workflow
    async def huge(c: Context) -> str:
        return LARGE

    sub = await Substantial.create("memory://values-result", partitions=1)
    async with script(sub, [huge]):
        run = await sub.start(huge)
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
    assert failed.value.error.type == "ValueTooLarge"
