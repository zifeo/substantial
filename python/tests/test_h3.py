"""Waiting listings of the S3 adapter on `h3+http://`, against the fake h3 server of
`fake_h3`, down to a worker waiting on them."""

from __future__ import annotations

import asyncio
import secrets
import time
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
from conftest import eventually, value
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
    await store.create("q/a", b"a", expiry=None)
    plain = await store.list("q/", start_after=None, max_keys=10, wait=None)
    assert plain.etag is None
    assert "x-herald-wait-seconds" not in h3.listed[-1].headers
    started = time.monotonic()
    first = await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0))
    assert first.keys == ["q/a"]
    assert first.etag is not None
    headers = h3.listed[-1].headers
    assert headers["x-herald-wait-seconds"] == "0"
    assert "x-herald-wait-etag" not in headers
    assert "x-herald-wait-seconds" in signed(headers)
    again = await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0, first.etag))
    assert again == first
    assert time.monotonic() - started < 1


async def test_a_held_wait_is_answered_by_a_change(h3: FakeH3, store: Store) -> None:
    await store.create("q/a", b"a", expiry=None)
    etag = (await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0))).etag
    held = asyncio.create_task(
        store.list("q/", start_after=None, max_keys=10, wait=ListWait(10, etag))
    )

    async def holding() -> bool:
        return h3.holds == 1

    await eventually("a held listing", holding)
    started = time.monotonic()
    await store.create("q/b", b"b", expiry=None)
    woken = await held
    assert time.monotonic() - started < 2
    assert woken.keys == ["q/a", "q/b"]
    assert woken.etag is not None and woken.etag != etag
    listed = h3.listed[-1]
    assert listed.woken
    assert (
        listed.headers["x-herald-wait-seconds"],
        listed.headers["x-herald-wait-etag"],
    ) == (
        "10",
        etag,
    )
    assert {"x-herald-wait-etag", "x-herald-wait-seconds"} <= set(signed(listed.headers))


async def test_a_wait_that_ends_unchanged_keeps_the_etag(h3: FakeH3, store: Store) -> None:
    await store.create("q/a", b"a", expiry=None)
    await store.create("other/x", b"x", expiry=None)
    before = await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0))
    started = time.monotonic()
    held = asyncio.create_task(
        store.list("q/", start_after=None, max_keys=10, wait=ListWait(2, before.etag))
    )

    async def holding() -> bool:
        return h3.holds == 1

    await eventually("a held listing", holding)
    await store.create("other/y", b"y", expiry=None)
    assert await held == before
    assert time.monotonic() - started >= 2
    assert not h3.listed[-1].woken


async def test_a_missing_etag_is_a_store_failure(h3: FakeH3, store: Store) -> None:
    h3.omit_etag = True
    with pytest.raises(StoreFailure, match="x-herald-listing-etag"):
        await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0))
    assert len(h3.listed) == 1, "a missing etag is not retried"
    await store.list("q/", start_after=None, max_keys=10, wait=None)


async def test_a_start_from_another_client_comes_through_the_wait(h3: FakeH3) -> None:
    @workflow
    async def answer(c: Context) -> int:
        return 42

    url = h3.url("e2e")
    sub = await Substantial.create(url, credentials=CREDENTIALS)
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


async def test_a_result_is_waited_on_its_head_through_a_continuation(h3: FakeH3) -> None:
    sub = await Substantial.create(h3.url("results"), credentials=CREDENTIALS)
    async with sub.worker([countdown], preset="script"):
        run = await sub.start(countdown, {"n": 1})
        assert await run.result(timeout=timedelta(seconds=10)) == "liftoff"
    head = f"results/runs/{run.chain}"
    waits = [listed for listed in h3.listed if listed.prefix.startswith("results/runs/")]
    assert {listed.prefix for listed in waits} == {head}, "the chain's head, continued in place"
    assert waits[0].headers["x-herald-wait-seconds"] == "0", "the first takes the etag"
    assert all(listed.headers["x-herald-wait-seconds"] == "20" for listed in waits[1:]), waits
    assert any(listed.woken for listed in waits), "a commit of the head wakes the wait"
    await sub.close()


async def test_a_get_is_not_queued_behind_waiting_listings(h3: FakeH3, store: Store) -> None:
    first = await store.list("p0/", start_after=None, max_keys=1, wait=ListWait(0))
    # As 16 workers serving 4 workflows each hold 64.
    held = [
        asyncio.create_task(
            store.list(f"p{i}/", start_after=None, max_keys=1, wait=ListWait(2, first.etag))
        )
        for i in range(64)
    ]

    async def holding() -> bool:
        return h3.holds >= 64

    await eventually("64 held listings", holding, within=1)
    started = time.monotonic()
    assert await value(store, "k") is None
    took = time.monotonic() - started
    await asyncio.gather(*held)
    assert took < 0.5, f"a get took {took:.2f} s behind 64 held listings"


async def test_a_run_ends_promptly_with_4_workflows_waiting(h3: FakeH3) -> None:
    async def one(c: Context) -> int:
        return 1

    flows = [workflow(name=f"pool-{n}")(one) for n in "abcd"]
    async with (
        Substantial.create(h3.url("pool"), credentials=CREDENTIALS) as sub,
        sub.worker(flows, preset="script"),
    ):

        async def holding() -> bool:
            return h3.holds >= 4

        await eventually("a held listing per workflow", holding)
        started = time.monotonic()
        run = await sub.start(flows[0])
        assert await run.result(timeout=timedelta(seconds=10)) == 1
        took = time.monotonic() - started
    assert took < 2, f"a run took {took:.2f} s"


async def test_a_bucket_is_used_whatever_its_versioning(h3: FakeH3) -> None:
    # The fake answers no versioning request: an adapter reading it would fail to open.
    url = h3.url("unversioned")
    # § x79k6r9e5@6td1h
    await (await Substantial.create(url, credentials=CREDENTIALS)).close()
    await (await Substantial.open(url, credentials=CREDENTIALS)).close()
    assert not [target for _, target in h3.requests if "versioning" in target]
