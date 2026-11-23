"""The store contract (I-26): six operations, the only ones Substantial performs on a store, the
expiry in whole days being part of every write."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

from substantial.errors import ConfigError


@dataclass(frozen=True)
class Created:
    """A create that wrote, with the ETag of the value written, which a later `cas` names."""

    etag: str


Create = Created | Literal["exists"]

# The longest expiry a write may carry, 100 years of days.
EXPIRY_MAX_DAYS = 36_500
DAY_MS = 86_400_000


@dataclass(frozen=True)
class Object:
    """A key's value and its ETag, which a later `cas` names."""

    data: bytes
    etag: str


@dataclass(frozen=True)
class Written:
    etag: str


# "mismatch": the key's value is not the one named, or there is none.
Cas = Written | Literal["mismatch"]


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

    # § karttrcdb@js0s3
    @property
    def capabilities(self) -> Capabilities: ...

    async def open(self) -> None:
        """Connects, raising `ConfigError` on a store the adapter cannot use; the operations
        connect too when it was not called."""
        ...

    async def create(self, key: str, data: bytes, *, expiry: int | None) -> Create:
        """Atomic and exclusive: of concurrent creates of a key, exactly one gets `Created`."""
        ...

    # § one kkad6aqxp@8p5h0
    async def cas(self, key: str, data: bytes, etag: str, *, expiry: int | None) -> Cas:
        """Replaces the value whose ETag is `etag`, atomically with every other operation on
        `key`."""
        ...

    async def get(self, key: str) -> Object | None: ...

    async def list(
        self, prefix: str, *, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        """Keys under `prefix` and after `start_after`, in byte order of the full key."""
        ...

    async def delete(self, key: str) -> None:
        """Removes a key; deleting a missing key succeeds."""
        ...

    async def check_expiry(self, days: Sequence[int]) -> None:
        """Raises `ConfigError` unless the store expires a key written with each of `days`."""
        ...

    async def close(self) -> None:
        """Releases what the adapter holds, its connections; it is not used after."""
        ...


def check_days(expiry: int | None) -> None:
    """Refuses an expiry outside 1 to `EXPIRY_MAX_DAYS` days, which no store could keep."""
    # § r4rsq6spn.c@n7jbg
    if expiry is not None and not (
        isinstance(expiry, int) and not isinstance(expiry, bool) and 1 <= expiry <= EXPIRY_MAX_DAYS
    ):
        raise StoreFailure(f"an expiry of {expiry!r} days is outside 1 to {EXPIRY_MAX_DAYS}")


def check_all_days(days: Sequence[int]) -> None:
    """`check_days` for every expiry of `days`, as `ConfigError`."""
    for d in days:
        try:
            check_days(d)
        except StoreFailure as e:
            raise ConfigError(e.message) from None


def digest_etag(data: bytes) -> str:
    """The ETag of the stores that make their own: the hex of the first 16 bytes of the SHA-256
    of the value."""
    return hashlib.sha256(data).digest()[:16].hex()


def in_range(keys: Iterable[str], prefix: str, start_after: str | None, max_keys: int) -> list[str]:
    """The keys of a sorted run that lie under `prefix` and after `start_after`, up to
    `max_keys`."""
    # § dfd6vt90h.b@2grfn
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
