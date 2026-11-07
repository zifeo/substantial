"""What an environment and its clients share: the namespace, the virtual time, the workers, the
holds, the seed and what Chaos injected."""

from __future__ import annotations

import calendar
from datetime import timedelta
from typing import TYPE_CHECKING

from substantial._proto import layout
from substantial.clock import Clock
from substantial.plan import Ulids
from substantial.stores.memory import _NAMESPACES, MemoryStore
from substantial.testing._store import Chaos, Faults, FaultStore, derive, unit
from substantial.testing._time import Time
from substantial.testing._workers import EnvWorker, Hold, WorkerClock

if TYPE_CHECKING:
    import asyncio

    from substantial.testing._env import Env


class ClientClock(Clock):
    """A client's clock: the virtual time; it never waits."""

    def __init__(self, time: Time) -> None:
        self._time = time

    def now(self) -> layout.Time:
        return layout.Time(ms=self._time.ms)


def due_ms(entry: str) -> int:
    """The time of a due entry, `<yyyymmdd>t<hhmmssmmm>z~<run>~<tag>`."""
    t = entry.partition("~")[0]
    seconds = calendar.timegm(
        (int(t[0:4]), int(t[4:6]), int(t[6:8]), int(t[9:11]), int(t[11:13]), int(t[13:15]))
    )
    return seconds * 1000 + int(t[15:18])


class World:
    def __init__(self, ns: str, ms: int, chaos: Chaos | None) -> None:
        self.ns = ns
        self.memory = MemoryStore(ns)
        self.time = Time(ms)
        self.time.skip = self.skip_scans
        self.chaos = chaos
        self.seed = 0 if chaos is None else chaos.seed
        # One core seed for every worker: a run's random then depends on the run alone.
        self.core_seed = derive(self.seed, "core")
        self.ulids = Ulids(lambda ms: int.from_bytes(derive(self.seed, "ulid", ms)[:10]))
        self.counts = Faults()
        self.writes = 0
        self.outage_until = ms
        self.partitions = 1
        self.workers: list[EnvWorker] = []
        self.holds: list[Hold] = []
        self.cancelled: set[asyncio.Task[object]] = set()
        self.errors: list[BaseException] = []
        # The chains whose failure the test read through the environment.
        self.checked: set[str] = set()
        # Each run that ended failed, its chain and its failure, from the write of its result.
        self.failed: dict[str, tuple[str, str]] = {}
        self.env: Env
        self._due: tuple[int, dict[str, int]] = (-1, {})
        # Off only in the test that compares a scenario with and without skipping.
        self.skipping = True
        self.skipped = 0

    def store(self, clock: WorkerClock | None) -> FaultStore:
        return FaultStore(self, self.memory, clock)

    def written(self, key: str, data: bytes) -> None:
        """Records a failed end as its result is written: retention may collect it before
        `check`."""
        parts = key.split("/")
        if len(parts) != 3 or parts[0] != "runs" or parts[2] != "result":
            return
        result = layout.Result.FromString(data)
        if result.status == layout.STATUS_FAILED:
            run = parts[1]
            chain = layout.Start.FromString(self.memory._objects[f"runs/{run}/start"]).chain
            self.failed[run] = (chain, f"{run} ({result.error.type}: {result.error.message})")

    def drop(self) -> None:
        _NAMESPACES.pop(self.ns, None)

    def died(self, error: BaseException) -> None:
        self.errors.append(error)

    def worker_id(self, place: int, replaced: int) -> str:
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
        return sorted(k for k in self.memory._objects if k.startswith(prefix))

    def earliest_due(self) -> dict[str, int]:
        """The earliest due entry of each workflow, as of the last write."""
        if self._due[0] != self.writes:
            due: dict[str, int] = {}
            for key in self.keys("queues/"):
                workflow, entry = key.split("/")[1], key.rpartition("/")[2]
                due[workflow] = min(due.get(workflow, due_ms(entry)), due_ms(entry))
            self._due = (self.writes, due)
        return self._due[1]

    def skip_scans(self, target: int) -> None:
        """Moves past the scans that cannot find anything before `target` or the next deadline
        of anything else, keeping the instants of the scans left.

        A worker's scan is skipped when the worker is idle, its deadline is the one its last scan
        set (`max_idle` after it), nothing was written since that scan, and no due entry it lists
        is due by then: the listing would return the keys it returned, none of them due, and set
        the deadline that skipping sets.

        Under Chaos, a scan is skipped only when the draw of each of its listings succeeds, and
        the skip counts their tries as the scan would: a scan whose draw fails runs, and may back
        off, which moves the scans after it. Never in an outage, where every scan fails."""
        if not self.skipping or self.time.ms < self.outage_until:
            return
        time, due = self.time, self.earliest_due()
        idle: list[tuple[WorkerClock, int, int | None]] = []
        bound = target
        for task, w in time.parked.items():
            if w.at is None or w.owner.paused:
                continue
            clock = w.owner
            if isinstance(clock, WorkerClock) and task is clock.agent:
                mi = clock.worker._settings.max_idle // timedelta(milliseconds=1)
                served = [due[n] for n, _, _ in clock.worker._workflows.served() if n in due]
                first = min(served, default=None)
                if (
                    clock.scanned_at is not None
                    and w.at == clock.scanned_at + mi
                    and clock.seen == self.writes
                    and clock.worker._events.empty()
                    and all(t is task or t.done() for t in clock.tasks)
                    and (first is None or first > w.at)
                ):
                    idle.append((clock, mi, first))
                    if first is not None:
                        bound = min(bound, first)
                    continue
            bound = min(bound, w.at)
        for clock, mi, first in idle:
            w = time.parked[clock.agent]  # type: ignore[index]
            limit = bound if first is None else min(bound, first - 1)
            if limit < w.at:  # type: ignore[operator]
                continue
            scans = self._succeeding(clock, (limit - w.at) // mi + 1)  # type: ignore[operator]
            if scans == 0:
                continue
            for key in clock.scan_keys:
                clock.tries[("list", key)] += scans
            self.skipped += scans
            last = w.at + (scans - 1) * mi  # type: ignore[operator]
            clock.scanned_at = last
            w.at = last + mi if first is None else min(last + mi, first)
