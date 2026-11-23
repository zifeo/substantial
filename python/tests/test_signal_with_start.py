"""`start(..., message=)`: a start that sends its run a message once it is created or found. The
TypeScript and Rust hosts port these tests under the same names."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

import substantial as s

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)


@s.workflow
async def collecting(c: s.Context, n: int) -> list[str]:
    got = []
    for _ in range(n):
        got.append(await c.receive("add"))
    return got


async def test_a_start_with_a_message_sends_it_once_the_run_is_created_or_found() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([collecting], preset="script"),
    ):
        # § fqv5ts8vg@kbhng
        first = s.Message("add", "a", id="m-a")
        run = await env.start(collecting, {"n": 2}, id="cart", message=first)
        await env.start(collecting, {"n": 2}, id="cart", message=first)
        await env.start(collecting, {"n": 2}, id="cart", message=s.Message("add", "b"))
        assert await run.result() == ["a", "b"], "the same id delivered once"
        with pytest.raises(s.RunEnded):
            await env.start(collecting, {"n": 2}, id="cart", message=s.Message("add", "c"))
        # § end fqv5ts8vg


async def test_a_start_refuses_a_message_outside_the_rules() -> None:
    async with s.testing.Env.open(now=T0) as env:
        # § fqv5ts8vg.a@krqke
        for message in (s.Message("substantial.x"), s.Message("add", id="Upper"), "add"):
            with pytest.raises(s.InvalidArgument):
                await env.start(collecting, {"n": 1}, message=message)  # type: ignore[arg-type]
        # § end fqv5ts8vg.a
        with pytest.raises(s.InvalidArgument, match="not JSON"):
            await env.start(collecting, {"n": 1}, message=s.Message("add", {1: "a"}))
