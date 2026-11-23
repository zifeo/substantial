"""The host's clock: the time every machine is given, and every wait on it (step timeouts, timers,
backoffs). A worker and a client take one; `substantial.testing` gives them a virtual one."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine, Iterable
from dataclasses import dataclass
from typing import Any, Literal

from substantial._proto import layout, run
from substantial.durations import now, until

# What an attempt runs: its body to its outcome, the body's errors included.
Attempt = Callable[[], Awaitable[run.Outcome]]
# Why an attempt ended before its body returned: past its timeout, or silent past its heartbeat
# timeout since its last heartbeat, or since it started.
Late = Literal["timeout", "silent"]


@dataclass
class Heartbeat:
    """An attempt's heartbeat timeout, `None` when unset, and the clock's `elapsed` at its last
    heartbeat, or at its start."""

    timeout_ms: int | None
    beat: int

    def silent_at(self) -> int | None:
        """When the attempt is silent for too long, unless it beats again before."""
        return None if self.timeout_ms is None else self.beat + self.timeout_ms

    def late(self, now: int, deadline: int | None) -> Late | None:
        """Whether an attempt of `deadline` is late at `now`, its timeout first."""
        if deadline is not None and now >= deadline:
            return "timeout"
        silent = self.silent_at()
        return "silent" if silent is not None and now >= silent else None


class Clock:
    """The real clock."""

    def now(self) -> layout.Time:
        return now()

    def elapsed(self) -> int:
        """Monotonic milliseconds, for durations a step of the wall clock must not change."""
        return time.monotonic_ns() // 1_000_000

    def seconds(self) -> float:
        """Monotonic seconds, for the durations an observer is told."""
        return time.monotonic()

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

    @property
    def checks_only(self) -> bool:
        """Whether a replay stops at the end of its history, as the replay tester's do."""
        return False

    def checked(self, run: str, diverged: str | None) -> None:
        """The replay of `run` reached the end of its history, or diverged before it."""

    async def attempt(
        self, name: str, key: str, body: Attempt, timeout_ms: int | None, heartbeat: Heartbeat
    ) -> run.Outcome | Late:
        """Runs one attempt of the step `name`; `Late` when its timeout or its heartbeat timeout
        passed first."""
        if heartbeat.timeout_ms is not None:
            return await self._watched(body, timeout_ms, heartbeat)
        # § aym52h8ch@5s7aw
        timeout = asyncio.timeout(None if timeout_ms is None else timeout_ms / 1000)
        try:
            # § one v4n6ja42e.a@xa33m
            async with timeout:
                return await body()
        except TimeoutError:
            if timeout.expired():
                return "timeout"
            raise

    async def _watched(
        self, body: Attempt, timeout_ms: int | None, heartbeat: Heartbeat
    ) -> run.Outcome | Late:
        """An attempt that also ends once its heartbeat timeout passed since its last heartbeat."""
        # § vvp37rp40@ph45t
        deadline = None if timeout_ms is None else self.elapsed() + timeout_ms
        task = asyncio.ensure_future(body())
        try:
            while True:
                at = min(t for t in (deadline, heartbeat.silent_at()) if t is not None)
                left = max(at - self.elapsed(), 0) / 1000
                done, _ = await asyncio.wait({task}, timeout=left)
                if done:
                    return task.result()
                late = heartbeat.late(self.elapsed(), deadline)
                if late is not None:
                    return late
        finally:
            # Abandoned, late or cancelled: interrupted at its `await`, as past a timeout.
            if not task.done():
                task.cancel()

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
