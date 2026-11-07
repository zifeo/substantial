"""Waiting listings of the S3 adapter on `h3+http://`, against the fake h3 server of
`fake_h3`, down to a worker waiting on them."""

from __future__ import annotations

import asyncio
import secrets
import time
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
from conftest import eventually
from fake_h3 import CREDENTIALS, FakeH3

from substantial import Context, Substantial, minutes, workflow
from substantial.stores import ListWait, Store, StoreFailure, open_store


@pytest.fixture
async def h3() -> AsyncIterator[FakeH3]:
    async with FakeH3() as h:
        yield h


@pytest.fixture
async def store(h3: FakeH3) -> AsyncIterator[Store]:
    s = open_store(h3.url(secrets.token_hex(8)), CREDENTIALS)
    yield s
    await s.close()


def signed(headers: dict[str, str]) -> list[str]:
    return headers["authorization"].split("SignedHeaders=")[1].split(",")[0].split(";")


async def test_an_immediate_answer_carries_the_etag(h3: FakeH3, store: Store) -> None:
    assert store.capabilities.wait
    await store.create("q/a", b"a")
    plain = await store.list("q/", start_after=None, max_keys=10, wait=None)
    assert plain.etag is None
    assert "x-substantial-wait-seconds" not in h3.listed[-1].headers
    started = time.monotonic()
    first = await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0))
    assert first.keys == ["q/a"]
    assert first.etag is not None
    headers = h3.listed[-1].headers
    assert headers["x-substantial-wait-seconds"] == "0"
    assert "x-substantial-wait-etag" not in headers
    assert "x-substantial-wait-seconds" in signed(headers)
    again = await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0, first.etag))
    assert again == first
    assert time.monotonic() - started < 1


async def test_a_held_wait_is_answered_by_a_change(h3: FakeH3, store: Store) -> None:
    await store.create("q/a", b"a")
    etag = (await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0))).etag
    held = asyncio.create_task(
        store.list("q/", start_after=None, max_keys=10, wait=ListWait(10, etag))
    )

    async def holding() -> bool:
        return h3.holds == 1

    await eventually("a held listing", holding)
    started = time.monotonic()
    await store.create("q/b", b"b")
    woken = await held
    assert time.monotonic() - started < 2
    assert woken.keys == ["q/a", "q/b"]
    assert woken.etag is not None and woken.etag != etag
    listed = h3.listed[-1]
    assert listed.woken
    assert (
        listed.headers["x-substantial-wait-seconds"],
        listed.headers["x-substantial-wait-etag"],
    ) == (
        "10",
        etag,
    )
    assert {"x-substantial-wait-etag", "x-substantial-wait-seconds"} <= set(signed(listed.headers))


async def test_a_wait_that_ends_unchanged_keeps_the_etag(h3: FakeH3, store: Store) -> None:
    await store.create("q/a", b"a")
    await store.create("other/x", b"x")
    before = await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0))
    started = time.monotonic()
    held = asyncio.create_task(
        store.list("q/", start_after=None, max_keys=10, wait=ListWait(2, before.etag))
    )

    async def holding() -> bool:
        return h3.holds == 1

    await eventually("a held listing", holding)
    await store.create("other/y", b"y")
    assert await held == before
    assert time.monotonic() - started >= 2
    assert not h3.listed[-1].woken


async def test_a_missing_etag_is_a_store_failure(h3: FakeH3, store: Store) -> None:
    h3.omit_etag = True
    with pytest.raises(StoreFailure, match="x-substantial-listing-etag"):
        await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0))
    assert len(h3.listed) == 1, "a missing etag is not retried"
    await store.list("q/", start_after=None, max_keys=10, wait=None)


async def test_a_start_from_another_client_comes_through_the_wait(h3: FakeH3) -> None:
    @workflow
    async def answer(c: Context) -> int:
        return 42

    url = h3.url("e2e")
    sub = await Substantial.create(url, partitions=1, credentials=CREDENTIALS)
    # Past max_idle only a waiting listing can bring the run to the worker.
    async with sub.worker([answer], preset="script", max_idle=minutes(5)):

        async def holding() -> bool:
            return h3.holds >= 1

        await eventually("a held listing", holding)
        other = await Substantial.open(url, credentials=CREDENTIALS)
        started = time.monotonic()
        run = await other.start(answer)
        assert await run.result(timeout=timedelta(seconds=10)) == 42
        assert time.monotonic() - started < 10
        assert any(listed.woken for listed in h3.listed)
        await other.close()
    await sub.close()


@workflow
async def countdown(c: Context, n: int) -> str:
    await c.sleep(timedelta(seconds=1))
    if n > 0:
        await c.continue_as_new({"n": n - 1})
    return "liftoff"


async def test_a_result_is_waited_on_its_key_through_a_continuation(h3: FakeH3) -> None:
    sub = await Substantial.create(h3.url("results"), partitions=1, credentials=CREDENTIALS)
    async with sub.worker([countdown], preset="script"):
        run = await sub.start(countdown, {"n": 1})
        assert await run.result(timeout=timedelta(seconds=10)) == "liftoff"
    waits = [listed for listed in h3.listed if listed.prefix.endswith("/result")]
    runs = list(dict.fromkeys(listed.prefix for listed in waits))
    assert len(runs) == 2, f"the first run, then the one it continued into: {runs}"
    assert len(waits) <= 4, f"a listing for the etag, then one held, per run: {waits}"
    assert all("x-substantial-wait-seconds" in listed.headers for listed in waits)
    assert any(listed.woken for listed in waits), waits
    await sub.close()
