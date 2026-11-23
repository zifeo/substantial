"""A loop that upgrades continues into the highest version of its workflow its worker serves, and
`sub.schedule` with a newer version moves a running schedule to it. The TypeScript and Rust hosts
port these tests under the same names."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import substantial as s

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)


def looping(version: int, upgrade: bool) -> s.Workflow[..., dict[str, Any]]:
    """Each iteration notes its version, then waits for `next`, three times."""

    @s.workflow(name="looping", version=version)
    async def code(c: s.Context) -> dict[str, Any]:
        async for st in c.iterate({"versions": []}, upgrade=upgrade):
            st["versions"].append(version)
            if len(st["versions"]) == 3:
                break
            await c.receive("next")
        return st

    return code


async def versions_of(upgrade: bool) -> list[int]:
    async with s.testing.Env.open(now=T0) as env:
        async with env.worker([looping(1, upgrade)], preset="script"):
            run = await env.start(looping(1, upgrade))
            await env.settle()
        async with env.worker([looping(1, upgrade), looping(2, upgrade)], preset="script"):
            await run.send("next")
            await env.settle()
            [info] = [i async for i in env.runs(looping(1, upgrade))]
            await run.send("next")
            result = await run.result()
        return [info.version, *result["versions"]]


async def test_a_loop_that_upgrades_continues_into_the_highest_version_served() -> None:
    # § zsyv7772c@r1a0z
    assert await versions_of(upgrade=True) == [2, 1, 2, 2]
    # § end zsyv7772c
    assert await versions_of(upgrade=False) == [1, 1, 1, 1]


def ticking(version: int) -> s.Workflow[..., None]:
    @s.workflow(name="ticking", version=version)
    async def code(c: s.Context) -> None:
        async for _ in c.schedule(upgrade=True):
            await c.step("tick", lambda: version)

    return code


async def test_a_schedule_of_a_newer_version_moves_the_running_one() -> None:
    hourly = s.Cron("0 * * * *", "UTC")
    async with s.testing.Env.open(now=T0) as env:
        async with env.worker([ticking(1)], preset="service"):
            run = await env.schedule(ticking(1), "tick", hourly, revision=1)
            await env.settle()
        async with env.worker([ticking(1), ticking(2)], preset="service"):
            # § sfzkmddwj@jmswh
            moved = await env.schedule(ticking(2), "tick", hourly, revision=2)
            assert moved.chain == run.chain
            await env.advance(s.hours(1))
            [info] = [i async for i in env.runs(ticking(2))]
            assert info.version == 2
            # § end sfzkmddwj
            assert (await run.read("schedule"))["revision"] == 2
