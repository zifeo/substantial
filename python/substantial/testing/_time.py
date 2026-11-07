"""The virtual time of an environment: its instant, the deadlines its tasks wait on, the gate that
`settle` and `advance` open, and how they know that nothing more can happen.

Every task of a worker (its loop, its store operations, its replays and their attempts) is
spawned here and waits only through `wait`, so the environment knows at each moment which ones
are parked and on what. Nothing can happen without the clock moving once every such task is
parked on a future not done and a deadline not come, and no step body runs.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from collections.abc import Callable, Coroutine, Iterable
from contextvars import ContextVar
from typing import Any, Protocol

from substantial.errors import SubstantialError
from substantial.worker import REPLAY

# A step body that has not returned after this many real seconds stops `settle`.
STEP_LIMIT = 10.0
# Replays of one run in one settle past which runs are taken to wake each other forever.
REPLAY_LIMIT = 100


class Stuck(SubstantialError):
    """A run waits on something the environment cannot bring, or a settle cannot end."""


# Set in the tasks of the workers and inherited by the step bodies they start: a settle there
# would wait on the task that asks for it.
_INSIDE: ContextVar[bool] = ContextVar("inside", default=False)


def _outside() -> None:
    if _INSIDE.get():
        raise Stuck(
            "a step cannot wait for the environment to settle (result, settle or advance), "
            "which waits for the step itself: start a child with c.start and await its result, "
            "or pass the value in"
        )


class Owner(Protocol):
    """The worker a task belongs to: a paused one does not act."""

    @property
    def paused(self) -> bool: ...


class Waiter:
    """A task parked until one of `futures` is done or the clock reaches `at`, in ms."""

    __slots__ = ("at", "futures", "kick", "owner")

    def __init__(self, futures: set[asyncio.Future[Any]], at: int | None, owner: Owner) -> None:
        self.futures = futures
        self.at = at
        self.owner = owner
        self.kick: asyncio.Future[None] | None = None

    def ready(self, now: int) -> bool:
        if self.owner.paused:
            return False
        return (self.at is not None and self.at <= now) or any(f.done() for f in self.futures)


class Time:
    def __init__(self, ms: int) -> None:
        self.ms = ms
        self.open = False
        self.parked: dict[asyncio.Task[Any], Waiter] = {}
        self.tasks: set[asyncio.Task[Any]] = set()
        # Step bodies running, with the real time they started at.
        self.bodies: dict[asyncio.Task[Any], tuple[str, float]] = {}
        self.replayed: Counter[str] = Counter()
        # Moves past the scans that cannot find anything, up to an instant (`World.skip_scans`).
        self.skip: Callable[[int], None] = lambda _: None
        self._changed: asyncio.Future[None] | None = None
        self._lock = asyncio.Lock()

    def _signal(self, *_: object) -> None:
        if self._changed is not None and not self._changed.done():
            self._changed.set_result(None)

    async def wait(
        self, owner: Owner, futures: Iterable[asyncio.Future[Any]], at: int | None
    ) -> None:
        task = asyncio.current_task()
        assert task is not None
        w = Waiter(set(futures), at, owner)
        self.parked[task] = w
        self._signal()
        try:
            if self.open and w.ready(self.ms):
                # A wait yields at least once, as on the real clock, so a loop of waits already
                # due lets the other tasks run.
                await asyncio.sleep(0)
            while not (self.open and w.ready(self.ms)):
                w.kick = asyncio.get_running_loop().create_future()
                if any(f.done() for f in w.futures):
                    await w.kick
                else:
                    await asyncio.wait({*w.futures, w.kick}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            del self.parked[task]

    def kick(self) -> None:
        """Has every parked task check again whether it may go on."""
        for w in self.parked.values():
            if w.kick is not None and not w.kick.done():
                w.kick.set_result(None)
        self._signal()

    async def passing(self, owner: Owner) -> None:
        """Returns once the gate is open and `owner` not paused."""
        if not self.open or owner.paused:
            done = asyncio.get_running_loop().create_future()
            done.set_result(None)
            await self.wait(owner, {done}, None)

    def spawn[T](self, owner: Owner, coro: Coroutine[Any, Any, T], name: str) -> asyncio.Task[T]:
        """A task of a worker, which starts once the gate is open."""

        async def gated() -> T:
            _INSIDE.set(True)
            await self.passing(owner)
            return await coro

        task = asyncio.create_task(gated(), name=name)
        self.tasks.add(task)
        task.add_done_callback(self._finished)
        # A task cancelled before it started never ran `coro`.
        task.add_done_callback(lambda _: coro.close())
        if name.startswith(REPLAY):
            self.replayed[name.removeprefix(REPLAY)] += 1
        return task

    def _finished(self, task: asyncio.Task[Any]) -> None:
        self.tasks.discard(task)
        self._signal()

    def body[T](self, coro: Coroutine[Any, Any, T], name: str) -> asyncio.Task[T]:
        """A step body, waited for by `settle`; an interrupted one ends at its next `await`."""
        task = asyncio.create_task(coro, name=name)
        self.bodies[task] = (name, time.perf_counter())
        task.add_done_callback(self._body_done)
        return task

    def _body_done(self, task: asyncio.Task[Any]) -> None:
        del self.bodies[task]
        self._signal()

    def quiet(self) -> bool:
        if self.bodies:
            return False
        for task in self.tasks:
            if task.done():
                continue
            w = self.parked.get(task)
            if w is None or w.ready(self.ms):
                return False
        return True

    def active(self) -> list[str]:
        """What keeps a settle from ending."""
        names = [name for name, _ in self.bodies.values()]
        for task in self.tasks:
            w = self.parked.get(task)
            if not task.done() and (w is None or w.ready(self.ms)):
                names.append(task.get_name())
        return sorted(names)

    async def settle(self) -> None:
        _outside()
        async with self._lock:
            await self._settle()

    async def advance(self, ms: int) -> None:
        _outside()
        async with self._lock:
            target = self.ms + ms
            await self._settle()
            while self.ms < target:
                self.skip(target)
                deadlines = [
                    w.at for w in self.parked.values() if w.at is not None and not w.owner.paused
                ]
                nxt = min([target, *deadlines])
                if nxt <= self.ms:
                    raise RuntimeError(f"a deadline at {nxt} ms is not after now, {self.ms} ms")
                self.ms = nxt
                await self._settle()

    async def _settle(self) -> None:
        # Replays are counted per settle: the test's own calls between settles wake runs too.
        self.replayed.clear()
        self.open = True
        self.kick()
        try:
            while not self.quiet():
                self._changed = asyncio.get_running_loop().create_future()
                await asyncio.wait({self._changed}, timeout=self._budget())
                self._check()
        finally:
            self.open = False

    def _budget(self) -> float | None:
        if not self.bodies:
            return None
        oldest = min(started for _, started in self.bodies.values())
        return max(0.0, oldest + STEP_LIMIT - time.perf_counter())

    def _check(self) -> None:
        now = time.perf_counter()
        late = [name for name, started in self.bodies.values() if now - started >= STEP_LIMIT]
        if late:
            raise Stuck(
                f"a step body has not returned after {STEP_LIMIT:.0f} s: {', '.join(late)}; "
                f"still active: {', '.join(self.active())}"
            )
        [(run, n)] = self.replayed.most_common(1) or [("", 0)]
        if n > REPLAY_LIMIT:
            raise Stuck(
                f"run {run} was replayed more than {REPLAY_LIMIT} times without the clock "
                "moving: runs keep waking each other"
            )
