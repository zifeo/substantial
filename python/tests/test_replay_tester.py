"""The replay tester (I-19): runs recorded on a store, replayed by new code in an environment.
The ports of `rust/tests/replay_tester.rs`, under the same names."""

from __future__ import annotations

import secrets

import pytest
from conftest import LONG, eventually, script, value

import substantial as s
from substantial import (
    Context,
    InvalidArgument,
    LayoutError,
    NonDeterminism,
    RunNotFound,
    Substantial,
    Workflow,
)
from substantial.stores import Store, open_store
from substantial.testing import Replayed, replay


def order(reserving: bool, ran: list[str]) -> Workflow:
    """`charge`, a sleep of an hour, `ship`; `reserve` first when `reserving`. Counts the attempts
    of its steps in `ran`."""

    def step(name: str) -> None:
        ran.append(name)

    async def order(c: Context) -> str:
        if reserving:
            await c.step("reserve", step, "reserve")
        await c.step("charge", step, "charge")
        await c.sleep(s.hours(1))
        await c.step("ship", step, "ship")
        return "shipped"

    return Workflow(order, name="order")


@s.workflow
async def actor(c: Context) -> int:
    return await c.receive("go")


@s.workflow
async def quick(c: Context) -> int:
    return 1


async def keys(store: Store) -> list[tuple[str, bytes | None]]:
    listed = await store.list("", start_after=None, max_keys=10_000, wait=None)
    return [(key, await value(store, key)) for key in listed.keys]


def url() -> str:
    return f"memory://replay-{secrets.token_hex(8)}"


async def test_new_code_replays_recorded_runs_and_writes_nothing_to_them() -> None:
    source = url()
    ran: list[str] = []
    sub = await Substantial.create(source)
    async with sub, script(sub, [order(False, ran), actor, quick]):
        suspended = await sub.start(order(False, ran), {}, id="a")
        waiting = await sub.start(actor, {}, id="b")
        ended = await sub.start(quick, {}, id="c")
        assert await ended.result(timeout=LONG) == 1
        for run in (suspended, waiting):
            await eventually("a suspended run", lambda r=run: _suspended(r))
    store = open_store(source, None)
    before = await keys(store)

    replayed_ran: list[str] = []
    same = [order(False, replayed_ran), actor, quick]
    replayed = await replay(source, same, runs=["a", "b", "c"])
    # § awhhtbr5n@35jw6
    assert replayed == [
        Replayed("a", "matched", None),
        Replayed("b", "matched", None),
        Replayed("c", "ended", None),
    ]
    # § end awhhtbr5n
    assert replayed_ran == [], "no step ran"

    changed = order(True, replayed_ran)
    [diverged] = await replay(source, [changed], runs=["a"])
    assert (diverged.run, diverged.outcome) == ("a", "diverged")
    assert isinstance(diverged.error, NonDeterminism)
    assert "charge" in str(diverged.error)
    # § awhhtbr5n.b@pvaxr
    assert replayed_ran == [], "no step ran"
    # § end awhhtbr5n.b
    # § awhhtbr5n.d@d53y1
    assert await keys(store) == before, "the source is as it was"


async def _suspended(run: s.Run) -> bool:
    return await run.status() == "suspended"


async def test_a_replay_names_runs_its_workflows_serve() -> None:
    source = url()
    async with await Substantial.create(source) as sub:
        await sub.start(quick, {}, id="c")
    # § awhhtbr5n.c@hzrn9
    with pytest.raises(InvalidArgument):
        await replay(source, [quick], runs=[])
    with pytest.raises(RunNotFound):
        await replay(source, [quick], runs=["nope"])
    with pytest.raises(InvalidArgument):
        await replay(source, [actor], runs=["c"])
    with pytest.raises(InvalidArgument):
        await replay(source, [Workflow(quick.fn, name="quick", version=2)], runs=["c"])
    with pytest.raises(LayoutError):
        await replay(url(), [quick], runs=["c"])
    [replayed] = await replay(source, [quick], runs=["c"])
    assert replayed.outcome == "matched"
