"""The workers of an environment: their clock on the virtual time, `crash`, `pause` and `resume`,
the holds of step attempts, and the crashes Chaos draws."""

from __future__ import annotations

import asyncio
import dataclasses
from collections import Counter
from collections.abc import Coroutine
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from substantial._proto import layout, run
from substantial.clock import Attempt, Clock, Inbox
from substantial.settings import Settings
from substantial.step import THREADED
from substantial.testing._time import Stuck
from substantial.worker import Worker
from substantial.workflow import Workflows

if TYPE_CHECKING:
    from substantial.testing._world import World


class WorkerClock(Clock):
    """The clock of one incarnation of a worker: a crash ends it, a Chaos replacement gets a new
    one."""

    def __init__(self, world: World, worker: EnvWorker) -> None:
        self.world = world
        self.worker = worker
        self.id = worker._id
        self.tasks: set[asyncio.Task[Any]] = set()
        self.agent: asyncio.Task[Any] | None = None
        self.dead = False
        # How many times this incarnation tried each operation on each key, for Chaos.
        self.tries: Counter[tuple[str, str]] = Counter()
        # The instant of its last scan, the store's writes then, and the keys its listings drew
        # Chaos faults for (`World.skip_scans`).
        self.scanned_at: int | None = None
        self.seen = -1
        self.scan_keys: set[str] = set()

    @property
    def paused(self) -> bool:
        return self.dead or self.worker._paused

    def listed(self, key: str, writes: int) -> None:
        """Records a listing of due entries, drawn as `key`, made when the store had seen
        `writes` writes."""
        now = self.world.time.ms
        if self.scanned_at != now:
            self.scanned_at, self.seen, self.scan_keys = now, writes, {key}
        else:
            self.seen = min(self.seen, writes)
            self.scan_keys.add(key)

    def now(self) -> layout.Time:
        return layout.Time(ms=self.world.time.ms)

    def elapsed(self) -> int:
        return self.world.time.ms

    async def wait(self, futures: Any, at: layout.Time | None) -> None:
        await self.world.time.wait(self, futures, None if at is None else at.ms)

    def spawn[T](self, coro: Coroutine[Any, Any, T], name: str) -> asyncio.Task[T]:
        task = self.world.time.spawn(self, coro, name)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def attempt(
        self, name: str, key: str, body: Attempt, timeout_ms: int | None
    ) -> run.Outcome | None:
        world = self.world
        deadline = None if timeout_ms is None else layout.Time(ms=world.time.ms + timeout_ms)
        task = world.time.body(body(), f"step {key}")
        try:
            await self.wait({task}, deadline)
        finally:
            # Abandoned, by its timeout or its worker's crash or shutdown: interrupted, as in
            # production, unless it runs in a thread, which `settle` waits for.
            if not task.done() and task not in THREADED:
                task.cancel()
        if not task.done():
            return None
        if world.draw("crash", key, self):
            world.counts.crashed += 1
            self.worker._replace()
            raise asyncio.CancelledError
        hold = world.hold_of(name)
        if hold is not None:
            held = Held(self, asyncio.get_running_loop().create_future(), key.split("/")[0])
            hold._held.append(held)
            try:
                await self.wait({held.released}, deadline)
            finally:
                if held in hold._held:
                    hold._held.remove(held)
            if not held.released.done():
                return None
        return task.result()

    def failure_read(self, chain: str) -> None:
        self.world.checked.add(chain)

    def crash(self) -> None:
        self.dead = True
        for task in self.tasks:
            task.cancel()
            self.world.time.tasks.discard(task)
        self.world.cancelled.update(self.tasks)


@dataclasses.dataclass(eq=False)
class Held:
    clock: WorkerClock
    released: asyncio.Future[None]
    run: str


class Hold:
    """The next `times` attempts of a step, held once their body returned or raised."""

    def __init__(self, step: str, times: int) -> None:
        self._step = step
        self._left = times
        self._held: list[Held] = []

    def __repr__(self) -> str:
        return f"<hold of {self._step}: {self.held} held, {self._left} to come>"

    @property
    def held(self) -> int:
        """The number of attempts held now."""
        return sum(1 for h in self._held if not h.clock.dead)

    def release(self) -> None:
        """Hands the outcomes of the attempts held to their workers."""
        for h in self._held:
            h.released.set_result(None)
        self._held.clear()

    def crash(self) -> None:
        """Crashes the workers that hold the attempts held."""
        for h in list(self._held):
            if not h.clock.dead and h.clock is h.clock.worker._clock:
                h.clock.worker.crash()
        self._held.clear()


class EnvWorker(Worker):
    """A worker of an environment: `async with` it, then `crash`, `pause`, `resume` it."""

    def __init__(self, world: World, workflows: Workflows, settings: Settings) -> None:
        self._world = world
        self._place = len(world.workers)
        self._replaced = 0
        self._paused = False
        self._crashed = False
        world.workers.append(self)
        super().__init__(world.store(None), workflows, world.partitions, settings, world.died)

    def _start(self) -> asyncio.Task:
        world = self._world
        self._id = world.worker_id(self._place, self._replaced)
        self._seed = world.core_seed
        self._clock = clock = WorkerClock(world, self)
        self._store = world.store(clock)
        self._events = Inbox()
        self._wake_pending = False
        task = super()._start()
        clock.agent = task
        return task

    def _replace(self) -> None:
        """A fresh worker of the same workflows and settings in place of this one, crashed."""
        self._clock.crash()  # type: ignore[attr-defined]
        self._replaced += 1
        self._task = None
        self._start()

    @property
    def alive(self) -> bool:
        return not self._crashed and super().alive

    def crash(self) -> None:
        """Kills the worker at once, as a SIGKILL would: its claims stay until they expire, and
        its steps in flight are abandoned, their bodies interrupted, those in a thread left to
        run to their end."""
        if self._task is None:
            raise RuntimeError(f"worker {self._id} was never started")
        self._crashed = True
        self._clock.crash()  # type: ignore[attr-defined]

    def pause(self) -> None:
        """Freezes the worker: no claim renewed, no store reached; its step bodies go on."""
        self._paused = True

    def resume(self) -> None:
        """Lets a paused worker go on from where it was."""
        self._paused = False
        self._world.time.kick()

    async def __aexit__(self, exc_type: object, *_: object) -> None:
        # A test that failed inside gets its own error, not one from settling after it.
        if exc_type is not None:
            if self.alive:
                self.crash()
            return
        await self.shutdown()

    async def shutdown(self) -> None:
        """Settles, then leaves as a SIGTERM does: steps that returned are recorded, held ones
        abandoned, and the runs it leaves have their claims deleted, for another worker."""
        if self._task is None:
            raise RuntimeError(f"worker {self._id} was never started")
        if self._crashed:
            return
        await self._world.env.settle()
        if self._task.done():
            await self._task
            return
        self._settings = dataclasses.replace(self._settings, shutdown_grace=timedelta(0))
        self._events.put_nowait(("shutdown",))
        await self._world.env.settle()
        if not self._task.done():
            raise Stuck(f"worker {self._id} could not leave: it is paused")
        await self._task
