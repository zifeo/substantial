"""What an environment and its clients share: the namespace, the virtual time, the workers, the
holds, the seed and what Chaos injected."""

from __future__ import annotations

import asyncio
import calendar
from datetime import timedelta
from typing import TYPE_CHECKING, NamedTuple

from substantial._proto import layout
from substantial.clock import Clock
from substantial.errors import InvalidArgument, ProtocolError
from substantial.observe import Metering
from substantial.plan import Ulids
from substantial.stores.memory import _NAMESPACES, MemoryStore
from substantial.testing._store import Chaos, Faults, FaultStore, derive, unit
from substantial.testing._time import Time
from substantial.testing._workers import EnvWorker, Hold, WorkerClock

if TYPE_CHECKING:
    from substantial.testing._env import Env


class ClientClock(Clock):
    """A client's clock: the virtual time; it never waits."""

    def __init__(self, time: Time) -> None:
        self._time = time

    def now(self) -> layout.Time:
        return layout.Time(ms=self._time.ms)


_U64 = (1 << 64) - 1


def idle_phase(seed: bytes, max_idle: int) -> int:
    """The phase the core draws from its seed, a worker's first idle wait, `max_idle` the others:
    SplitMix64 of the seed's first 8 bytes, big-endian, modulo `max_idle`."""
    z = (int.from_bytes(seed[:8], "big") + 0x9E3779B97F4A7C15) & _U64
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _U64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _U64
    return (z ^ (z >> 31)) % max_idle


def _loop(loop: asyncio.AbstractEventLoop) -> str:
    return f"{type(loop).__name__} at {id(loop):#x}"


class Due(NamedTuple):
    """The earliest due entries of a workflow: of any kind, and of those that wake a run, a
    timer or a message, which every due entry is."""

    any: int
    wake: int | None


def due_ms(entry: str) -> int:
    """The time of a due entry, `<yyyymmdd>t<hhmmssmmm>z~<chain>~<tag>`."""
    t = entry.partition("~")[0]
    seconds = calendar.timegm(
        (int(t[0:4]), int(t[4:6]), int(t[6:8]), int(t[9:11]), int(t[11:13]), int(t[13:15]))
    )
    return seconds * 1000 + int(t[15:18])


def head_of(world: World, id: str) -> layout.Head:
    """The head of a run, its chain's through the pointer of a later run."""
    found = layout.Run.FromString(_object(world, f"runs/{id}"))
    if found.WhichOneof("kind") == "pointer":
        found = layout.Run.FromString(_object(world, f"runs/{found.pointer.chain}"))
    if found.WhichOneof("kind") != "head":
        raise ProtocolError(f"runs/{id}: no head")
    return found.head


def _object(world: World, key: str) -> bytes:
    data = world.memory.object(key)
    if data is None:
        raise ProtocolError(f"{key} is missing")
    return data


def last_events(head: layout.Head) -> list[layout.Event]:
    return list(head.segments[-1].events) if head.segments else []


_ENDS = ("completed", "failed", "cancelled", "continued")


def ended(head: layout.Head) -> bool:
    """Whether a head's run ended: its last segment holds an end."""
    return any(e.WhichOneof("kind") in _ENDS for e in last_events(head))


class World:
    def __init__(self, ns: str, ms: int, chaos: Chaos | None, metering: Metering) -> None:
        self.ns = ns
        # Every task of the environment runs on the loop that opened it: its futures belong there.
        self.loop = asyncio.get_running_loop()
        # The prices and the observer of the environment and its clients, which its workers take.
        self.metering = metering
        self.time = Time(ms)
        self.memory = MemoryStore(ns, clock=lambda: self.time.ms)
        self.time.skip = self.skip_scans
        self.chaos = chaos
        self.seed = 0 if chaos is None else chaos.seed
        # One core seed for every worker: a run's random then depends on the run alone.
        # § hz0ve24df@ca7c0
        self.core_seed = derive(self.seed, "core")
        self.ulids = Ulids(lambda ms: int.from_bytes(derive(self.seed, "ulid", ms)[:10]))
        # § end hz0ve24df
        self.counts = Faults()
        self.writes = 0
        self.outage_until = ms
        self.workers: list[EnvWorker] = []
        self.holds: list[Hold] = []
        self.cancelled: set[asyncio.Task[object]] = set()
        self.errors: list[BaseException] = []
        # The chains whose failure the test read through the environment.
        self.checked: set[str] = set()
        # Each run that ended failed, its chain and its failure, from the write of its result.
        self.failed: dict[str, tuple[str, str]] = {}
        self.env: Env
        self._due: tuple[int, dict[str, Due]] = (-1, {})
        # Off only in the test that compares a scenario with and without skipping.
        self.skipping = True
        self.skipped = 0
        # For the replay tester: each run its replays checked against their history, with the
        # divergence they met; `None` in every other environment, whose replays go on.
        self.checks: dict[str, str | None] | None = None

    def check_loop(self) -> None:
        """Refuses a call from another event loop than the environment's, which would wait on
        futures no loop runs, as an app run by a thread-portal test client does."""
        current = asyncio.get_running_loop()
        if current is not self.loop:
            raise InvalidArgument(
                f"the test environment was opened on the event loop {_loop(self.loop)} and is "
                f"called from the event loop {_loop(current)}: run the code under test on the "
                "test's own loop (for an ASGI app, its lifespan and httpx.ASGITransport, not a "
                "test client that runs the app in a thread)"
            )

    def store(self, clock: WorkerClock | None) -> FaultStore:
        return FaultStore(self, self.memory, clock)

    def written(self, key: str, data: bytes) -> None:
        """Records a failed end as its head is written: its expiry may remove it before
        `check`."""
        id = key.removeprefix("runs/")
        if id == key or "/" in id:
            return
        found = layout.Run.FromString(data)
        if found.WhichOneof("kind") != "head":
            return
        head = found.head
        failed = [e.failed.error for e in last_events(head) if e.HasField("failed")]
        if failed:
            error = failed[0]
            failure = f"{head.run} ({error.type}: {error.message})"
            self.failed[head.run] = (head.start.chain, failure)

    def drop(self) -> None:
        _NAMESPACES.pop(self.ns, None)

    def died(self, error: BaseException) -> None:
        self.errors.append(error)

    def worker_id(self, place: int, replaced: int) -> str:
        # § hz0ve24df@kc8a9
        return derive(self.seed, "worker", place, replaced).hex()[:16]

    def draw(self, kind: str, key: str, clock: WorkerClock) -> bool:
        """Whether Chaos faults this try of `kind` on `key` by the worker of `clock`."""
        if self.chaos is None:
            return False
        clock.tries[(kind, key)] += 1
        return self._faults(kind, key, clock, clock.tries[(kind, key)])

    def _faults(self, kind: str, key: str, clock: WorkerClock, n: int) -> bool:
        """Whether Chaos faults the `n`th try of `kind` on `key` by the worker of `clock`."""
        assert self.chaos is not None
        # § one hz0ve24df.a@0yxte
        return unit(self.seed, kind, key, clock.id, n) < self.chaos.rate

    def _succeeding(self, clock: WorkerClock, scans: int) -> int:
        """How many of the next `scans` scans of `clock` Chaos lets through, one after the
        other: those whose every listing succeeds at its first try."""
        if self.chaos is None:
            return scans
        for i in range(scans):
            for key in clock.scan_keys:
                if self._faults("list", key, clock, clock.tries[("list", key)] + i + 1):
                    return i
        return scans

    def hold_of(self, step: str) -> Hold | None:
        for hold in self.holds:
            if hold._step == step and hold._left > 0:
                hold._left -= 1
                return hold
        return None

    def keys(self, prefix: str) -> list[str]:
        return self.memory.keys(prefix)

    def earliest_due(self) -> dict[str, Due]:
        """The earliest due entries of each workflow, as of the last write."""
        if self._due[0] != self.writes:
            due: dict[str, Due] = {}
            for key in self.keys("queues/"):
                _, workflow, kind, entry = key.split("/")
                # A rest marker is no due entry: no scan lists it.
                if kind != "due":
                    continue
                at = due_ms(entry)
                was = due.get(workflow, Due(at, None))
                wake = at if was.wake is None else min(was.wake, at)
                due[workflow] = Due(min(was.any, at), wake)
            self._due = (self.writes, due)
        return self._due[1]

    def skip_scans(self, target: int) -> None:
        """Moves past the scans that cannot find anything before `target` or the next deadline
        of anything else, keeping the instants of the scans left.

        A worker's scan is skipped when the worker is idle, its deadline is the one its last scan
        set (`max_idle` after it, or the worker's phase after its first), nothing was written
        since that scan, and no due entry it lists is due by then: the listing would return the
        keys it returned, none of them due, and set the deadline that skipping sets.

        Under Chaos, a scan is skipped only when the draw of each of its listings succeeds, and
        the skip counts their tries as the scan would: a scan whose draw fails runs, and may back
        off, which moves the scans after it. Never in an outage, where every scan fails."""
        if not self.skipping or self.time.ms < self.outage_until:
            return
        time, due = self.time, self.earliest_due()
        idle: list[tuple[WorkerClock, int, Due | None]] = []
        bound = target
        for task, w in time.parked.items():
            if w.at is None or w.owner.paused:
                continue
            clock = w.owner
            if isinstance(clock, WorkerClock) and task is clock.agent:
                mi = clock.worker._settings.max_idle // timedelta(milliseconds=1)
                served = [due[n] for n, _, _ in clock.worker._workflows.served() if n in due]
                wakes = [d.wake for d in served if d.wake is not None]
                first = (
                    Due(min(d.any for d in served), min(wakes, default=None)) if served else None
                )
                if (
                    clock.scanned_at is not None
                    and w.at - clock.scanned_at in (mi, idle_phase(self.core_seed, mi))
                    and clock.seen == self.writes
                    and clock.worker._events.empty()
                    and all(t is task or t.done() for t in clock.tasks)
                    and (first is None or first.any > w.at)
                ):
                    idle.append((clock, mi, first))
                    if first is not None and first.wake is not None:
                        bound = min(bound, first.wake)
                    continue
            bound = min(bound, w.at)
        for clock, mi, first in idle:
            w = time.parked[clock.agent]  # type: ignore[index]
            assert w.at is not None
            limit = bound if first is None else min(bound, first.any - 1)
            if limit < w.at:
                continue
            scans = self._succeeding(clock, (limit - w.at) // mi + 1)
            if scans == 0:
                continue
            for key in clock.scan_keys:
                clock.tries[("list", key)] += scans
            self.skipped += scans
            last = w.at + (scans - 1) * mi
            clock.scanned_at = last
            after = last + mi
            w.at = after if first is None or first.wake is None else min(after, first.wake)
