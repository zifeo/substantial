"""Retention on every store, on the real clock: an ended run's head is written with its retention
as its expiry, which the store then removes it by."""

from datetime import UTC, datetime

import pytest
from conftest import LONG, Recording, eventually, script

import substantial as s
from substantial import Substantial
from substantial.stores import Store


@s.workflow(retention=1)
async def quick(c: s.Context) -> int:
    return 1


@s.workflow
async def other(c: s.Context) -> int:
    return 2


async def keys(store: Store, prefix: str) -> list[str]:
    return (await store.list(prefix, start_after=None, max_keys=1000, wait=None)).keys


async def test_an_ended_run_is_written_to_expire_after_its_retention(store: Store) -> None:
    recording = Recording(store)
    sub = await Substantial._create(recording)
    async with script(sub, [quick]):
        run = await sub.start(quick, id="q")
        assert await run.result(timeout=LONG) == 1
    expiries = [expiry for key, expiry in recording.writes if key == "runs/q"]
    *before, end = expiries
    assert end == 1, expiries
    assert all(e is None for e in before), expiries
    assert await run.status() == "completed"
    assert await run.result() == 1
    with pytest.raises(s.RunExists) as e:
        await sub.start(other, id="q")
    assert e.value.id == "q"


def late_wakeup(workflow: str) -> str:
    """A wakeup of `q` due now, as a late sender or a crashed worker leaves one."""
    now = datetime.now(UTC)
    return f"queues/{workflow}/due/{now:%Y%m%dt%H%M%S}{now.microsecond // 1000:03d}z~q~s0000000001"


@s.workflow(retention=1)
async def publishing(c: s.Context) -> int:
    c.publish("p", 1)
    return await c.step("s", lambda: 1)


async def test_the_keys_of_a_removed_run_never_come_back(store: Store) -> None:
    sub = await Substantial._create(store)
    async with script(sub, [publishing]) as worker:
        run = await sub.start(publishing, id="q")
        assert await run.result(timeout=LONG) == 1
        assert await run.read("p") == 1
        # As its expiry removes it.
        await store.delete("runs/q")
        late = late_wakeup("publishing")
        await store.create(late, b"", expiry=None)
        worker._wake()

        async def served() -> bool:
            return late not in await keys(store, "queues/publishing/due/")

        await eventually("the late wakeup served", served, within=10)
        assert await run.read("p") is None
        assert await keys(store, "runs/q") == []
        assert await keys(store, "archives/") == []
