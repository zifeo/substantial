"""Retention on every store, on the real clock: the collection's deletes and the tombstone,
with retention and tombstone shortened through the settings a worker does not take publicly."""

import dataclasses
from datetime import timedelta

import pytest
from conftest import LONG, eventually

import substantial as s
from substantial import Substantial
from substantial.settings import Settings
from substantial.stores import Store

SHORT = {
    "retention": timedelta(milliseconds=300),
    "retention_min": timedelta(milliseconds=300),
    "tombstone": timedelta(seconds=2),
    "recheck": timedelta(milliseconds=200),
}


@s.workflow
async def quick(c: s.Context) -> int:
    return 1


async def keys(store: Store, prefix: str) -> list[str]:
    return (await store.list(prefix, start_after=None, max_keys=1000, wait=None)).keys


async def test_a_run_is_collected_and_its_id_taken_until_the_tombstone_ends(store: Store) -> None:
    sub = await Substantial._create(store, partitions=1)
    preset = Settings.preset("script", waiting_store=store.capabilities.wait)
    settings = dataclasses.replace(preset, max_idle=timedelta(milliseconds=50), **SHORT)
    async with sub._worker([quick], settings):
        run = await sub.start(quick, id="q")
        assert await run.result(timeout=LONG) == 1

        async def collected() -> bool:
            return await keys(store, "runs/q/") == ["runs/q/gone"]

        await eventually("the collection", collected, within=10)
        with pytest.raises(s.RunNotFound) as e:
            await run.status()
        assert e.value.collected
        with pytest.raises(s.RunExists) as exists:
            await sub.start(quick, id="q")
        assert exists.value.collected

        async def ended() -> bool:
            return await keys(store, "runs/q/") == []

        await eventually("the tombstone's end", ended, within=10)
        again = await sub.start(quick, id="q")
        assert await again.result(timeout=LONG) == 1
