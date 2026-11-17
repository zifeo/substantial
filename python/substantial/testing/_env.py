"""The environment: a `Substantial` on a namespace in memory and a virtual clock, its clients, and
what a test asks of them."""

from __future__ import annotations

import asyncio
import secrets
import time
from collections.abc import AsyncIterator, Iterable
from contextlib import aclosing
from datetime import UTC, datetime, timedelta
from typing import Any

from substantial._proto import layout, plan
from substantial.client import (
    Opening,
    Run,
    RunInfo,
    RunStatus,
    Substantial,
    _check_timeout,
    _max_interval,
)
from substantial.cost import Prices
from substantial.durations import millis
from substantial.errors import InvalidArgument, RunFailed, SubstantialError, WaitTimeout
from substantial.observe import Metering, Observe
from substantial.settings import Settings
from substantial.testing import _time
from substantial.testing._store import Chaos, Faults
from substantial.testing._time import Stuck
from substantial.testing._workers import EnvWorker, Hold
from substantial.testing._world import ClientClock, World, due_ms
from substantial.workflow import Workflow, Workflows

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class Unchecked(SubstantialError):
    """Runs ended failed without the test reading their failure through the environment."""


class _Client(Substantial):
    """A `Substantial` of an environment: its workers and its runs are the environment's."""

    _world: World

    def _attach(self, world: World) -> None:
        self._world = world
        self._clock = ClientClock(world.time)
        self._ulid = world.ulids

    def _worker(self, workflows: Iterable[Workflow[..., Any]], settings: Settings) -> EnvWorker:
        worker = EnvWorker(self._world, Workflows(workflows, settings.retention_min), settings)
        self._workers.add(worker)
        return worker

    def _handle[R](self, workflow: Workflow[..., R], chain: str) -> Run[R]:
        return _Run(self, workflow, chain)

    async def _plan(self, data: plan.PlanInput) -> tuple[str, object]:
        self._world.check_loop()
        return await super()._plan(data)

    async def runs(self, workflow: Workflow[..., Any]) -> AsyncIterator[RunInfo]:
        self._world.check_loop()
        async with aclosing(super().runs(workflow)) as infos:
            async for info in infos:
                yield info


class _Run[R](Run[R]):
    _sub: _Client

    async def result(
        self, *, timeout: timedelta | None = None, max_interval: timedelta | None = None
    ) -> R:
        """Settles, then the value the chain ended with; `Stuck` naming what it waits on if it
        has not ended, `WaitTimeout` instead when a `timeout` is given. The clock does not
        move, and nothing polls: `max_interval` is only checked."""
        _check_timeout(timeout)
        _max_interval(max_interval)
        world = self._sub._world
        await world.env.settle()
        try:
            ended, value = await self._ended(None)
        except RunFailed as e:
            if self._sub is world.env and e.status == RunStatus.FAILED:
                world.checked.add(self.chain)
            raise
        if ended:
            return value
        if timeout is not None:
            raise WaitTimeout(f"run {self.chain} did not end within {timeout}")
        raise Stuck(f"run {self.chain} has not ended: it waits on {_waits_on(world, value.run)}")

    async def status(self) -> RunStatus:
        status = await super().status()
        world = self._sub._world
        if self._sub is world.env and status == RunStatus.FAILED:
            world.checked.add(self.chain)
        return status


def _serves(world: World, start: layout.Start) -> bool:
    version = start.version if start.HasField("version") else None
    return any(
        w.alive and w._workflows.get(start.workflow, version) is not None for w in world.workers
    )


def _start(world: World, run: str) -> layout.Start:
    return layout.Start.FromString(world.memory._objects[f"runs/{run}/start"])


def _named(start: layout.Start) -> str:
    version = f" version {start.version}" if start.HasField("version") else ""
    return f"{start.workflow}{version}"


def _waits_on(world: World, run: str) -> str:
    start = _start(world, run)
    what = []
    if not _serves(world, start):
        what.append(f"workflow {_named(start)}, which no worker serves")
    for hold in world.holds:
        what += [f"step {hold._step}, held" for h in hold._held if h.run == run]
    for key in world.keys(f"queues/{start.workflow}/due/"):
        entry = key.rpartition("/")[2]
        if entry.split("~")[1] == run:
            at = _EPOCH + timedelta(milliseconds=due_ms(entry))
            what.append(f"a timer due at {at.isoformat()}")
    if not what:
        what.append("a message, or the end of a run it started")
    unserved = [
        f"{other} ({_named(s)})"
        for other in _pending(world)
        if other != run and not _serves(world, s := _start(world, other))
    ]
    if unserved:
        what.append(f"while runs of workflows no worker serves wait: {', '.join(unserved)}")
    return "; ".join(what)


def _pending(world: World) -> list[str]:
    runs = {key.split("/")[1] for key in world.keys("runs/") if key.endswith("/start")}
    return sorted(r for r in runs if f"runs/{r}/result" not in world.memory._objects)


class Env(_Client):
    """A `Substantial` for tests: a namespace in memory, on a virtual clock, whose workers run in
    the test's process and move only in `settle` and `advance`."""

    @classmethod
    def open(  # type: ignore[override]
        cls,
        *,
        now: datetime,
        chaos: Chaos | None = None,
        prices: Prices | None = None,
        observe: Observe | None = None,
    ) -> Opening:
        """A fresh namespace in memory whose clock starts at `now`; `async with` it. `prices` and
        `observe` as for `Substantial.create`, for the environment, its clients and its
        workers."""
        if not isinstance(now, datetime) or now.utcoffset() is None:
            raise InvalidArgument(f"an environment starts at an aware datetime, not {now!r}")
        if now.microsecond % 1000:
            raise InvalidArgument(
                f"an environment's clock counts whole milliseconds, not {now.isoformat()}"
            )
        if chaos is not None and not isinstance(chaos, Chaos):
            raise InvalidArgument(f"chaos is a Chaos, not {chaos!r}")
        metering = Metering(prices, observe)
        return Opening(cls._opened_env(millis(now - _EPOCH), chaos, metering))

    @classmethod
    async def _opened_env(cls, ms: int, chaos: Chaos | None, metering: Metering) -> Env:
        world = World(f"testing-{secrets.token_hex(8)}", ms, chaos, metering)
        store = world.store(None)
        env = await cls._create(store, prices=metering.prices, observe=metering.observe)
        assert isinstance(env, Env)
        env._attach(world)
        world.env = env
        return env

    def client(self) -> Opening:
        """Another `Substantial` on the same namespace and clock, as another pod or the
        application under test would hold; await it, or `async with` it."""
        return Opening(self._client())

    async def _client(self) -> _Client:
        m = self._world.metering
        client = await _Client._open(self._world.store(None), prices=m.prices, observe=m.observe)
        assert isinstance(client, _Client)
        client._attach(self._world)
        return client

    @property
    def now(self) -> datetime:
        """The virtual time."""
        return _EPOCH + timedelta(milliseconds=self._world.time.ms)

    @property
    def faults(self) -> Faults:
        """What Chaos injected so far."""
        c = self._world.counts
        return Faults(failed=c.failed, ambiguous=c.ambiguous, crashed=c.crashed)

    async def settle(self) -> None:
        """Lets every worker do all it can at the current time, until nothing can happen without
        the clock moving."""
        self._world.check_loop()
        await self._world.time.settle()
        self._raise_died()

    async def advance(self, d: timedelta) -> None:
        """Settles, then moves the clock forward by `d`, settling at each moment a worker has
        something due."""
        self._world.check_loop()
        await self._world.time.advance(millis(d))
        self._raise_died()

    def _raise_died(self) -> None:
        errors = self._world.errors
        if errors:
            raise errors.pop(0)

    def outage(self, d: timedelta) -> None:
        """Every store operation fails for the next `d` of virtual time."""
        self._world.outage_until = self._world.time.ms + millis(d)

    def hold(self, step: str, times: int = 1) -> Hold:
        """Holds the next `times` attempts of `step` (`"<step>.undo"` for its undo) once their
        body returned or raised."""
        if not isinstance(step, str):
            raise InvalidArgument(f"hold takes a step name, not {step!r}")
        if isinstance(times, bool) or not isinstance(times, int) or times <= 0:
            raise InvalidArgument(f"times is a positive int, not {times!r}")
        hold = Hold(step, times)
        self._world.holds.append(hold)
        return hold

    async def check(self) -> None:
        """Raises `Unchecked` naming every run that ended failed without the test reading its
        failure through the environment."""
        world = self._world
        unread = [
            failure
            for chain, failure in (world.failed[run] for run in sorted(world.failed))
            if chain not in world.checked
        ]
        if unread:
            raise Unchecked(f"runs ended failed and the test never read it: {'; '.join(unread)}")

    async def close(self) -> None:
        """Kills the workers as a SIGKILL would, waits up to 10 s for the step bodies still
        running, and drops the namespace."""
        world = self._world
        for worker in world.workers:
            if worker.alive:
                worker.crash()
        # A hung body is left running past the bound of `Stuck`, so that leaving never hangs.
        until = time.perf_counter() + _time.STEP_LIMIT
        try:
            while world.cancelled or world.time.bodies:
                pending = {*world.cancelled, *world.time.bodies}
                world.cancelled.clear()
                left = until - time.perf_counter()
                if left <= 0:
                    break
                await asyncio.wait(pending, timeout=left)
        finally:
            world.drop()
