"""Retries of transient failures and the ambiguous-create rule (I-26), shared by every adapter."""

import dataclasses
from collections.abc import Awaitable, Callable
from datetime import timedelta

import pytest

from substantial.stores import StoreFailure
from substantial.stores import retry as r
from substantial.stores.attempts import measured
from substantial.stores.retry import Ambiguous, Permanent, StoreRetry, Transient

POLICY = StoreRetry(attempts=3, initial=timedelta(milliseconds=1), max=timedelta(milliseconds=5))


def script(steps: list[object]) -> Callable[[], Awaitable[object]]:
    queue = list(steps)

    async def step() -> object:
        assert queue, "unexpected call"
        outcome = queue.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return step


async def test_transient_failures_are_retried_then_fail() -> None:
    assert await r.with_retry(POLICY, script([Transient("a"), 7])) == 7
    with pytest.raises(StoreFailure, match=r"down \(after 3 attempts\)"):
        await r.with_retry(POLICY, script([Transient("down")] * 3))
    once = dataclasses.replace(POLICY, attempts=1)
    with pytest.raises(StoreFailure, match=r"down \(after 1 attempt\)$"):
        await r.with_retry(once, script([Transient("down")]))


async def test_permanent_failures_are_not_retried() -> None:
    with pytest.raises(StoreFailure, match="bad key"):
        await r.with_retry(POLICY, script([Permanent("bad key"), 1]))


async def resolve(creates: list[object], gets: list[object]) -> str:
    return await r.create(POLICY, b"mine", script(creates), script(gets))


async def test_ambiguous_creates_are_resolved_with_a_get() -> None:
    def lost() -> Ambiguous:
        return Ambiguous("timeout")

    assert await resolve([lost()], [b"mine"]) == "created"
    assert await resolve([lost()], [b"theirs"]) == "exists"
    assert await resolve([lost(), "created"], [None]) == "created"
    assert await resolve([lost(), "exists"], [None, b"mine"]) == "created", "landed late"
    assert await resolve([lost()], [Transient("get"), b"mine"]) == "created"
    with pytest.raises(StoreFailure):
        await resolve([lost(), lost()], [None, Transient("x")])
    assert await resolve(["exists"], []) == "exists", "no ambiguity, no get"
    assert await resolve([Transient("409"), "created"], []) == "created"


async def test_every_request_is_counted_the_read_back_too() -> None:
    # § czph3f9yv.a@ka9h6
    with measured() as sent:
        assert await r.with_retry(POLICY, script([Transient("a"), 7])) == 7
    assert sent.n == 2
    with measured() as sent:
        assert await resolve([Ambiguous("timeout")], [b"mine"]) == "created"
    assert sent.n == 2
    with pytest.raises(StoreFailure), measured() as sent:
        await r.with_retry(POLICY, script([Transient("down")] * 3))
    assert sent.n == 3, "a failed operation counts its requests too"
    with measured() as outer:
        with measured() as inner:
            pass
        assert inner.n == 1, "an adapter without retries sends one request"
    assert outer.n == 1, "an operation measured inside another counts in both"
