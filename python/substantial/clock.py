"""The host's clock: the time every machine is given, and every wait on it (step timeouts, timers,
backoffs). A worker and a client take one; `substantial.testing` gives them a virtual one."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine, Iterable
from typing import Any

from substantial._proto import layout, run
from substantial.durations import now, until

# What an attempt runs: its body to its outcome, the body's errors included.
Attempt = Callable[[], Awaitable[run.Outcome]]


class Clock:
    """The real clock."""

    def now(self) -> layout.Time:
        return now()

    def elapsed(self) -> int:
        """Monotonic milliseconds, for durations a step of the wall clock must not change."""
        return time.monotonic_ns() // 1_000_000

    async def wait(self, futures: Iterable[asyncio.Future[Any]], at: layout.Time | None) -> None:
        """Returns once one of `futures` is done, or once `at` has come."""
        pending = set(futures)
        timeout = None if at is None else until(at, self.now())
        if not pending:
            await asyncio.sleep(0 if timeout is None else timeout)
            return
        await asyncio.wait(pending, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)

    def spawn[T](self, coro: Coroutine[Any, Any, T], name: str) -> asyncio.Task[T]:
        return asyncio.create_task(coro, name=name)

    async def attempt(
        self, name: str, key: str, body: Attempt, timeout_ms: int | None
    ) -> run.Outcome | None:
        """Runs one attempt of the step `name`; `None` when its timeout passed first."""
        timeout = asyncio.timeout(None if timeout_ms is None else timeout_ms / 1000)
        try:
            async with timeout:
                return await body()
        except TimeoutError:
            if timeout.expired():
                return None
            raise

    def failure_read(self, chain: str) -> None:
        """The code was given the failure of `chain`, a child it started; the testing
        environment counts it read."""


REAL = Clock()


class Inbox[T]:
    """A queue whose arrivals a clock can wait on: `signal()` is done once it holds an item."""

    def __init__(self) -> None:
        self._items: deque[T] = deque()
        self._signal: asyncio.Future[None] | None = None

    def put_nowait(self, item: T) -> None:
        self._items.append(item)
        if self._signal is not None and not self._signal.done():
            self._signal.set_result(None)

    def empty(self) -> bool:
        return not self._items

    def get_nowait(self) -> T:
        return self._items.popleft()

    def signal(self) -> asyncio.Future[None]:
        fut = asyncio.get_running_loop().create_future()
        if self._items:
            fut.set_result(None)
        else:
            self._signal = fut
        return fut
