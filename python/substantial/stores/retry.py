"""Retries of transient failures and resolution of ambiguous creates, shared by every adapter."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta

from substantial.stores.base import Create, StoreFailure


@dataclass(frozen=True)
class StoreRetry:
    """Adapter retries of transient failures: `attempts` in all, backoff doubling from `initial`
    to `max`."""

    attempts: int
    initial: timedelta
    max: timedelta


# The policy of every adapter (I-26).
STORE_RETRY = StoreRetry(attempts=5, initial=timedelta(milliseconds=100), max=timedelta(seconds=5))


class Fault(Exception):
    """How one attempt of an operation failed."""


class Transient(Fault):
    """Nothing happened; trying again may succeed."""


class Ambiguous(Fault):
    """A create that may or may not have happened: a timeout, a 5xx, a lost connection."""


class Permanent(Fault):
    """Trying again cannot help: an invalid key, a refused permission."""


# How a backoff waits, in seconds: `asyncio.sleep` on the real clock.
Sleep = Callable[[float], Awaitable[None]]


class _Backoff:
    def __init__(self, policy: StoreRetry, sleep: Sleep) -> None:
        self.policy = policy
        self.sleep = sleep
        self.delay = policy.initial
        self.attempt = 1

    async def next(self, error: str, *, sleep: bool) -> None:
        """Waits before the next attempt, or fails once every attempt is spent."""
        if self.attempt >= self.policy.attempts:
            unit = "attempt" if self.attempt == 1 else "attempts"
            raise StoreFailure(f"{error} (after {self.attempt} {unit})")
        self.attempt += 1
        if sleep:
            await self.sleep(self.delay / timedelta(seconds=1))
            self.delay = min(self.delay * 2, self.policy.max)


async def with_retry[T](
    policy: StoreRetry, op: Callable[[], Awaitable[T]], sleep: Sleep = asyncio.sleep
) -> T:
    backoff = _Backoff(policy, sleep)
    while True:
        try:
            return await op()
        except Permanent as e:
            raise StoreFailure(str(e)) from None
        except (Transient, Ambiguous) as e:
            await backoff.next(str(e), sleep=True)


async def create(
    policy: StoreRetry,
    data: bytes,
    attempt: Callable[[], Awaitable[Create]],
    get: Callable[[], Awaitable[bytes | None]],
    sleep: Sleep = asyncio.sleep,
) -> Create:
    """A create that never assumes it won or lost: an ambiguous outcome is followed by a `get`,
    the same bytes meaning "created", other bytes "exists", and a missing key creating again."""
    backoff = _Backoff(policy, sleep)
    ambiguous = False
    # An earlier attempt left unanswered may have landed after the get, so a later "exists" is
    # checked too.
    was_ambiguous = False
    while True:
        if ambiguous:
            try:
                stored = await get()
            except Permanent as e:
                raise StoreFailure(str(e)) from None
            except (Transient, Ambiguous) as e:
                await backoff.next(str(e), sleep=True)
                continue
            if stored is not None:
                return "created" if stored == data else "exists"
            ambiguous = False
        try:
            outcome = await attempt()
        except Permanent as e:
            raise StoreFailure(str(e)) from None
        except Transient as e:
            await backoff.next(str(e), sleep=True)
            continue
        except Ambiguous as e:
            ambiguous = was_ambiguous = True
            await backoff.next(str(e), sleep=False)
            continue
        if outcome == "exists" and was_ambiguous:
            ambiguous = True
            await backoff.next("exists after an ambiguous create", sleep=False)
            continue
        return outcome
