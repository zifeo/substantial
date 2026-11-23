from __future__ import annotations

import bisect
import itertools
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from substantial.errors import ConfigError
from substantial.stores.base import (
    DAY_MS,
    Capabilities,
    Cas,
    Create,
    Created,
    Listing,
    ListWait,
    Object,
    Written,
    check_all_days,
    check_days,
    digest_etag,
    in_range,
    refuse_wait,
)


@dataclass(frozen=True)
class _Entry:
    data: bytes
    # The instant it expires at, ms since the epoch.
    expires: int | None


_NAMESPACES: dict[str, dict[str, _Entry]] = {}
_LOCK = threading.Lock()

# Milliseconds since the epoch: the wall clock, or a test's.
Clock = Callable[[], int]


def wall_clock() -> int:
    return time.time_ns() // 1_000_000


class MemoryStore:
    """`memory://<ns>`: a namespace held by the process, for tests. Every store opened on the same
    `<ns>` in a process shares its objects, its expiries judged by `clock`."""

    capabilities = Capabilities(wait=False)

    def __init__(self, ns: str, *, clock: Clock = wall_clock) -> None:
        if not isinstance(ns, str) or not ns:
            raise ConfigError(f"a memory store needs a namespace, not {ns!r}")
        self._clock = clock
        # § one e9ern38bz.d@8rspp
        with _LOCK:
            self._objects = _NAMESPACES.setdefault(ns, {})

    def _live(self, key: str) -> _Entry | None:
        """The entry of `key` unless it expired: exact, as a store that expires keys itself."""
        # § r4rsq6spn.b@7et2p
        entry = self._objects.get(key)
        if entry is None or (entry.expires is not None and self._clock() >= entry.expires):
            return None
        return entry

    def _entry(self, data: bytes, expiry: int | None) -> _Entry:
        # § r4rsq6spn.a@nhygt
        check_days(expiry)
        expires = None if expiry is None else self._clock() + expiry * DAY_MS
        return _Entry(bytes(data), expires)

    def keys(self, prefix: str) -> list[str]:
        """The keys under `prefix` that did not expire, in order, as a test sees what the store
        keeps."""
        return sorted(k for k in self._objects if k.startswith(prefix) and self._live(k))

    def object(self, key: str) -> bytes | None:
        """The value of `key` unless it expired, as a test reads it."""
        entry = self._live(key)
        return None if entry is None else entry.data

    async def create(self, key: str, data: bytes, *, expiry: int | None) -> Create:
        entry = self._entry(data, expiry)
        # § dfd6vt90h@w7am1
        if self._live(key) is not None:
            return "exists"
        self._objects[key] = entry
        return Created(digest_etag(entry.data))

    async def cas(self, key: str, data: bytes, etag: str, *, expiry: int | None) -> Cas:
        entry = self._entry(data, expiry)
        # § kkad6aqxp@9zts6
        current = self._live(key)
        # § one kkad6aqxp.a@my0zc
        if current is None or digest_etag(current.data) != etag:
            return "mismatch"
        self._objects[key] = entry
        return Written(digest_etag(entry.data))

    async def get(self, key: str) -> Object | None:
        entry = self._live(key)
        return None if entry is None else Object(entry.data, digest_etag(entry.data))

    async def list(
        self, prefix: str, *, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        refuse_wait(wait)
        keys = sorted(self._objects)
        lower = max(prefix, start_after) if start_after is not None else prefix
        after = keys[bisect.bisect_left(keys, lower) :]
        under = itertools.takewhile(lambda k: k.startswith(prefix), after)
        return Listing(keys=in_range(under, prefix, start_after, max_keys))

    async def delete(self, key: str) -> None:
        self._objects.pop(key, None)

    async def check_expiry(self, days: Sequence[int]) -> None:
        check_all_days(days)

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass
