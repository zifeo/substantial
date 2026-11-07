"""The store of an environment: its namespace in memory, seen by each worker and client through
the faults the test asks for, an outage or Chaos."""

from __future__ import annotations

import dataclasses
import hashlib
import math
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from substantial.errors import InvalidArgument
from substantial.stores.base import Capabilities, Create, Listing, ListWait, refuse_wait
from substantial.stores.memory import MemoryStore
from substantial.stores.retry import STORE_RETRY, Ambiguous, Transient, create, with_retry

if TYPE_CHECKING:
    from substantial.testing._workers import WorkerClock
    from substantial.testing._world import World


@dataclasses.dataclass(frozen=True)
class Chaos:
    """Faults drawn from `seed`: each worker store operation fails with probability `rate`, half
    of the failed creates applied anyway, and each step attempt that returns crashes its worker
    with probability `rate`."""

    seed: int
    rate: float

    def __post_init__(self) -> None:
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise InvalidArgument(f"a Chaos seed is an int, not {self.seed!r}")
        rate = self.rate
        if isinstance(rate, bool) or not isinstance(rate, int | float) or not 0 <= rate <= 1:
            raise InvalidArgument(f"a Chaos rate is a number from 0 to 1, not {rate!r}")


@dataclasses.dataclass
class Faults:
    """What Chaos injected: store operations `failed`, creates applied with their answer lost
    (`ambiguous`), and workers `crashed` after a step returned."""

    failed: int = 0
    ambiguous: int = 0
    crashed: int = 0


def derive(seed: int, *parts: object) -> bytes:
    """32 bytes from `seed` and what they name, never from a place in a sequence."""
    return hashlib.blake2b(repr((seed, *parts)).encode(), digest_size=32).digest()


def unit(seed: int, *parts: object) -> float:
    """A draw in [0, 1) from `seed` and what it names."""
    return int.from_bytes(derive(seed, *parts)[:8]) / 2**64


class FaultStore:
    """The namespace as a worker (`clock`) or a client (`None`) sees it."""

    capabilities = Capabilities(wait=False)

    def __init__(self, world: World, memory: MemoryStore, clock: WorkerClock | None) -> None:
        self._world = world
        self._memory = memory
        self._clock = clock

    async def _sleep(self, seconds: float) -> None:
        """A worker's backoff waits in virtual time; a client's takes none."""
        if self._clock is not None:
            at = self._world.time.ms + math.ceil(seconds * 1000)
            await self._world.time.wait(self._clock, (), at)

    async def _try[T](self, kind: str, key: str, apply: Callable[[], Awaitable[T]]) -> T:
        world, clock = self._world, self._clock
        if clock is not None:
            # A paused worker does not reach the store.
            await world.time.passing(clock)
        if world.time.ms < world.outage_until:
            raise Transient("the store is out")
        if clock is not None and world.draw(kind, key, clock):
            n = clock.tries[(kind, key)]
            applied = kind == "create" and unit(world.seed, "applied", key, clock.id, n) < 0.5
            # A create that found its key applied nothing: it counts as failed.
            if applied and await apply() == "created":
                world.counts.ambiguous += 1
                raise Ambiguous("chaos: a create applied, its answer lost")
            world.counts.failed += 1
            raise Transient(f"chaos: a {kind} failed")
        return await apply()

    async def _create(self, key: str, data: bytes) -> Create:
        created = await self._memory.create(key, data)
        if created == "created":
            self._world.writes += 1
            self._world.written(key, data)
        return created

    async def create(self, key: str, data: bytes) -> Create:
        return await create(
            STORE_RETRY,
            data,
            lambda: self._try("create", key, lambda: self._create(key, data)),
            lambda: self._try("get", key, lambda: self._memory.get(key)),
            self._sleep,
        )

    async def get(self, key: str) -> bytes | None:
        return await self._retry("get", key, lambda: self._memory.get(key))

    async def list(
        self, prefix: str, *, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        refuse_wait(wait)
        writes = self._world.writes
        key = prefix if start_after is None else f"{prefix}>{start_after}"
        listing = await self._retry(
            "list",
            key,
            lambda: self._memory.list(
                prefix, start_after=start_after, max_keys=max_keys, wait=None
            ),
        )
        if self._clock is not None and prefix.startswith("queues/"):
            self._clock.listed(key, writes)
        return listing

    async def delete(self, key: str) -> None:
        async def apply() -> None:
            await self._memory.delete(key)
            self._world.writes += 1

        await self._retry("delete", key, apply)

    async def _retry[T](self, kind: str, key: str, apply: Callable[[], Awaitable[T]]) -> T:
        return await with_retry(STORE_RETRY, lambda: self._try(kind, key, apply), self._sleep)

    async def open(self) -> None:
        await self._memory.open()

    async def close(self) -> None:
        pass
