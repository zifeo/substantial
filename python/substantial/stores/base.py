"""The store contract (I-26): four operations, the only ones Substantial performs on a store."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal, Protocol

Create = Literal["created", "exists"]


@dataclass(frozen=True)
class Capabilities:
    wait: bool


@dataclass(frozen=True)
class Listing:
    keys: list[str]
    etag: str | None = None


@dataclass(frozen=True)
class ListWait:
    seconds: int
    etag: str | None = None


class StoreFailure(Exception):
    """The store failed after the adapter's own retries."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class Store(Protocol):
    """An adapter scoped to its namespace: keys are relative to it."""

    @property
    def capabilities(self) -> Capabilities: ...

    async def open(self) -> None:
        """Connects, raising `ConfigError` on a store the adapter cannot use; the operations
        connect too when it was not called."""
        ...

    async def create(self, key: str, data: bytes) -> Create:
        """Atomic and exclusive: of concurrent creates of a key, exactly one gets "created"."""
        ...

    async def get(self, key: str) -> bytes | None: ...

    async def list(
        self, prefix: str, *, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        """Keys under `prefix` and after `start_after`, in byte order of the full key."""
        ...

    async def delete(self, key: str) -> None:
        """Removes a key; deleting a missing key succeeds."""
        ...

    async def close(self) -> None:
        """Releases what the adapter holds, its connections; it is not used after."""
        ...


def in_range(keys: Iterable[str], prefix: str, start_after: str | None, max_keys: int) -> list[str]:
    """The keys of a sorted run that lie under `prefix` and after `start_after`, up to
    `max_keys`."""
    out: list[str] = []
    for key in keys:
        if len(out) >= max_keys:
            break
        # Code point order is the byte order of UTF-8, so strings compare as their keys do.
        if key.startswith(prefix) and (start_after is None or key > start_after):
            out.append(key)
    return out


def refuse_wait(wait: ListWait | None) -> None:
    if wait is not None:
        raise StoreFailure("this store cannot wait on a listing")
