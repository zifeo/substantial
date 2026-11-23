"""`run.retry(keep=n)`: a reset of an ended run, which replays its first events and runs the rest
again. The TypeScript and Rust hosts port these tests under the same names."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

import substantial as s

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)
RAN: list[str] = []


def note(name: str) -> str:
    RAN.append(name)
    return name


@s.workflow
async def ordered(c: s.Context) -> list[str]:
    a = await c.step("a", note, "a")
    go = await c.receive("go")
    b = await c.step("b", note, "b")
    return [a, go, b]


async def test_a_reset_replays_the_first_events_of_an_ended_run() -> None:
    RAN.clear()
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([ordered], preset="script"),
    ):
        run = await env.start(ordered, id="o")
        await env.settle()
        await run.send("go", "now")
        assert await run.result() == ["a", "now", "b"]
        kinds = [e.kind for e in await run.history()]
        assert kinds == ["step", "message", "step", "completed"]
        # § grzez328c@t0pws
        await run.retry(keep=1)
        assert await run.result() == ["a", "now", "b"], "the message after the point given back"
        assert RAN == ["a", "b", "b"], "the kept step not again"
        # § end grzez328c


async def test_a_reset_refuses_a_point_past_the_end_and_a_live_run() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([ordered], preset="script"),
    ):
        run = await env.start(ordered)
        await env.settle()
        # § grzez328c.a@jttys
        with pytest.raises(s.InvalidArgument):
            await run.retry(keep=0)
        await run.send("go", "now")
        await run.result()
        for keep in (99, -1, True, 1.0):
            with pytest.raises(s.InvalidArgument):
                await run.retry(keep=keep)  # type: ignore[arg-type]
        # § end grzez328c.a
