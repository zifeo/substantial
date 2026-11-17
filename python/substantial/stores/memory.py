from __future__ import annotations

import bisect
import itertools
import threading

from substantial.errors import ConfigError
from substantial.stores.base import Capabilities, Create, Listing, ListWait, in_range, refuse_wait

_NAMESPACES: dict[str, dict[str, bytes]] = {}
_LOCK = threading.Lock()


class MemoryStore:
    """`memory://<ns>`: a namespace held by the process, for tests. Every store opened on the same
    `<ns>` in a process shares its objects."""

    capabilities = Capabilities(wait=False)

    def __init__(self, ns: str) -> None:
        if not isinstance(ns, str) or not ns:
            raise ConfigError(f"a memory store needs a namespace, not {ns!r}")
        with _LOCK:
            self._objects = _NAMESPACES.setdefault(ns, {})

    async def create(self, key: str, data: bytes) -> Create:
        if key in self._objects:
            return "exists"
        self._objects[key] = bytes(data)
        return "created"

    async def get(self, key: str) -> bytes | None:
        return self._objects.get(key)

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

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass
