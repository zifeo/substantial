"""The cap on every value the host encodes: past it, `ValueTooLarge`, and the core never sees it."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import timedelta

import pytest
from conftest import LONG, Recording, script
from test_replay import Harness

from substantial import (
    Context,
    InvalidArgument,
    RunFailed,
    StepFailed,
    Substantial,
    ValueTooLarge,
    _proto,
    child,
    step,
    workflow,
)
from substantial._proto import MAX_VALUE
from substantial.durations import millis
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

    sub = await Substantial.create("memory://values-state")
    async with script(sub, [growing], continue_as_new_after=0):
        run = await sub.start(growing)
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
    assert failed.value.error.type == "ValueTooLarge"


async def test_the_core_refuses_a_value_past_the_cap_and_records_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The host's own check left out, so the value reaches the core, which checks it again.
    monkeypatch.setattr(_proto, "checked", lambda data: data)
    monkeypatch.setattr(child, "checked", lambda data: data)

    @workflow
    async def large(c: Context) -> str:
        try:
            c.publish("k", LARGE)
        except ValueTooLarge as e:
            message = str(e)
        else:
            raise AssertionError("published")
        c.publish("k", "fits")
        return message

    sub = await Substantial.create("memory://values-core")
    async with script(sub, [large]):
        run = await sub.start(large)
        message = await run.result(timeout=LONG)
    # § w5rs0gck0@wanp2
    assert str(len(LARGE) + 2) in message and str(MAX_VALUE) in message, message
    published = [e.data for e in await run.history() if e.kind == "published"]
    assert [p["value"] for p in published] == ["fits"], published
    assert await run.read("k") == "fits"
    # § end w5rs0gck0


async def test_a_client_refuses_a_value_past_the_cap_before_the_store() -> None:
    @workflow
    async def waiting(c: Context) -> str:
        return await c.receive("note")

    recording = Recording(MemoryStore("values-client"))
    sub = await Substantial._create(recording)
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

    sub = await Substantial.create("memory://values-result")
    async with script(sub, [huge]):
        run = await sub.start(huge)
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
    assert failed.value.error.type == "ValueTooLarge"


# What `json` would write as another value: a key that is not a string, a NaN, a tuple, an
# integer a JavaScript reader rounds.
NOT_JSON = [
    {1: "a"},
    [{"ok": {None: 1}}],
    (math.nan,),
    {"x": math.inf},
    ("a", 1),
    {"t": [(1, 2)]},
    [2**53],
    {"n": -(2**53)},
]


async def test_a_value_json_cannot_hold_is_refused_never_written_as_another() -> None:
    @workflow
    async def holding(c: Context, i: int) -> object:
        try:
            c.publish("k", NOT_JSON[i])
        except InvalidArgument:
            pass
        else:
            raise AssertionError("published")
        try:
            await c.step("value", lambda: NOT_JSON[i])
        except StepFailed as e:
            assert e.cause.type == "Json", e.cause
        else:
            raise AssertionError("stepped")
        return NOT_JSON[i]

    sub = await Substantial.create("memory://values-not-json")
    run = await sub.start(holding, {"i": 0})
    for bad in NOT_JSON:
        with pytest.raises(InvalidArgument, match="is not JSON"):
            await run.send("note", bad)
    async with script(sub, [holding]):
        for i in range(len(NOT_JSON)):
            with pytest.raises(RunFailed) as failed:
                await (await sub.start(holding, {"i": i})).result(timeout=LONG)
            assert failed.value.error.type == "Json", failed.value


def test_a_duration_past_the_longest_is_refused() -> None:
    # § b60bm4szk@28h7n
    assert millis(timedelta(milliseconds=2**53 - 1)) == 2**53 - 1
    with pytest.raises(InvalidArgument, match="longer than the longest"):
        millis(timedelta(milliseconds=2**53))
