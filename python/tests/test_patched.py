"""`c.patched(id)`: new code branches on a patch without a new version, its answer recorded. The
TypeScript and Rust hosts port these tests under the same names."""

from __future__ import annotations

from datetime import UTC, datetime

import substantial as s

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)


def patching(patched: bool) -> s.Workflow[..., list[bool]]:
    """Waits for `a`, steps, waits for `b`; patched, it asks for a patch after each wait."""

    @s.workflow(name="patching")
    async def code(c: s.Context) -> list[bool]:
        seen = []
        await c.receive("a")
        if patched:
            seen.append(c.patched("fix-1"))
        await c.step("x", lambda: 1)
        await c.receive("b")
        if patched:
            seen.append(c.patched("fix-2"))
        return seen

    return code


async def test_a_patch_is_true_live_and_on_replay_and_false_in_an_older_history() -> None:
    async with s.testing.Env.open(now=T0) as env:
        async with env.worker([patching(False)], preset="script"):
            older = await env.start(patching(False), id="older")
            await older.send("a")
            await env.settle()
        async with env.worker([patching(True)], preset="script"):
            newer = await env.start(patching(True), id="newer")
            await newer.send("a")
            await env.settle()
            for run in (older, newer):
                await run.send("b")
            # § hbsrjm0w2@wgnp3
            assert await older.result() == [False, True], "past its point, then live"
            assert await newer.result() == [True, True], "recorded, then live"
            patches = [e.data for e in await newer.history() if e.kind == "patched"]
            assert patches == [{"id": "fix-1"}, {"id": "fix-2"}]
            # § end hbsrjm0w2


@s.workflow
async def misnamed(c: s.Context) -> str:
    try:
        c.patched("Not a label")
    except s.InvalidArgument as e:
        return str(e)
    return "accepted"


async def test_a_patch_id_outside_the_rules_is_refused() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([misnamed], preset="script"),
    ):
        run = await env.start(misnamed)
        # § hbsrjm0w2.a@strx8
        assert 'patch id "Not a label" is invalid' in await run.result()
        # § end hbsrjm0w2.a
