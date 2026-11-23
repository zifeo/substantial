"""Stores and helpers shared by the tests. The S3 and Redis stores are those of
`dev/compose.yaml`, at the URLs of `dev/stores.env` unless the environment sets them; a test that
needs one it cannot reach fails."""

from __future__ import annotations

import asyncio
import dataclasses
import fcntl
import functools
import os
import secrets
import socket
import tempfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import timedelta
from pathlib import Path

import pytest

from substantial import Failure, Run, RunInfo, RunStatus, Substantial, Workflow
from substantial._proto import layout
from substantial.settings import PUBLIC, Settings
from substantial.stores import Store, open_store
from substantial.stores.fs import FsStore
from substantial.stores.memory import MemoryStore
from substantial.worker import Worker

STORES = Path(__file__).parents[2] / "dev" / "stores.env"
UP = "docker compose -f dev/compose.yaml up -d --wait"
S3_URL = "SUBSTANTIAL_S3_URL"
# The versioned bucket, which RustFS serves short of the store contract: only to check that.
S3_VERSIONED_URL = "SUBSTANTIAL_S3_VERSIONED_URL"
LONG = timedelta(seconds=10)
REDIS_URLS = {
    "redis": "SUBSTANTIAL_REDIS_URL",
    "redis-sentinel": "SUBSTANTIAL_REDIS_SENTINEL_URL",
    "redis-cluster": "SUBSTANTIAL_REDIS_CLUSTER_URL",
}
ADAPTERS = ["memory", "file", "s3", *REDIS_URLS]
# The adapters a test applies to, so no case is generated only to be skipped.
PLAIN_LISTINGS = ["memory", "file", "s3"]
KEYED_BY_NAME = ["file", "s3", *REDIS_URLS]
# Shared by every test process on the machine, as the Redis containers are.
FAILOVER_LOCK = Path(tempfile.gettempdir()) / "substantial-tests-failover.lock"


@pytest.fixture(autouse=True)
def failover_exclusion(request: pytest.FixtureRequest) -> Iterator[None]:
    """A test marked `failover` stops shared Redis containers or changes their configuration: it
    holds the lock exclusively, every other test holds it shared, across the xdist workers and any
    other session."""
    exclusive = request.node.get_closest_marker("failover") is not None
    with FAILOVER_LOCK.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield


def load_stores() -> None:
    """Sets each variable of `dev/stores.env` the environment does not."""
    for n, line in enumerate(STORES.read_text().splitlines(), 1):
        if not line or line.startswith("#"):
            continue
        name, sep, value = line.partition("=")
        if not sep:
            raise AssertionError(f"{STORES}:{n} is not NAME=value: {line!r}")
        os.environ.setdefault(name, value)


load_stores()


@functools.cache
def reached(variable: str) -> str:
    """The URL `variable` names, once each of its hosts accepts a connection."""
    url = os.environ.get(variable)
    if not url:
        pytest.fail(f"{variable} is not set: see dev/stores.env and run `{UP}`", pytrace=False)
    for address in url.partition("://")[2].partition("/")[0].split(","):
        host, _, port = address.rpartition(":")
        if not host or not port.isdigit():
            pytest.fail(f"{variable} names {address!r}, not a host:port", pytrace=False)
        try:
            socket.create_connection((host, int(port)), timeout=2).close()
        except OSError as e:
            pytest.fail(
                f"{variable} names {address}, which does not answer ({e}): run `{UP}`",
                pytrace=False,
            )
    return url


def require_s3(scheme: str = "s3", variable: str = S3_URL) -> str:
    """A fresh namespace under the one `variable` names, as a URL of `scheme` (`s3` or `h3`, its
    `+http` kept)."""
    url = reached(variable)
    given, _, rest = url.partition("://")
    plain = {"s3": "", "s3+http": "+http"}.get(given)
    if plain is None:
        raise AssertionError(f"{variable} is not an s3:// or s3+http:// URL: {url!r}")
    path, _, query = rest.partition("?")
    if not query:
        raise AssertionError(f"{variable} has no query: {url!r}")
    return f"{scheme}{plain}://{path}/{secrets.token_hex(8)}?{query}"


def require_redis(topology: str) -> str:
    """A fresh namespace under the one the variable of `topology` names."""
    path, _, query = reached(REDIS_URLS[topology]).partition("?")
    return f"{path}/{secrets.token_hex(8)}?{query}"


def fresh_store(adapter: str, tmp_path: Path) -> Store:
    """A fresh namespace in an adapter of this host."""
    if adapter == "memory":
        return MemoryStore(f"test-{secrets.token_hex(8)}")
    if adapter == "file":
        return FsStore(tmp_path / f"ns-{secrets.token_hex(4)}")
    if adapter in REDIS_URLS:
        return open_store(require_redis(adapter), None)
    return open_store(require_s3(), "env")


def kind(outcome: object) -> str:
    """A create's outcome without the ETag it carries."""
    return outcome if isinstance(outcome, str) else "created"


async def value(store: Store, key: str) -> bytes | None:
    """The value of `key`, without its ETag."""
    found = await store.get(key)
    return None if found is None else found.data


@pytest.fixture(params=ADAPTERS)
def adapter(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
async def store(adapter: str, tmp_path: Path) -> AsyncIterator[Store]:
    s = fresh_store(adapter, tmp_path)
    yield s
    await s.close()


def script(sub: Substantial, workflows: list[Workflow], **settings: object) -> Worker:
    """The script preset, listing often so that tests need not wait for `max_idle`; the settings
    a worker does not take publicly are set through the internal path."""
    settings.setdefault("max_idle", timedelta(milliseconds=50))
    hidden = {name: settings.pop(name) for name in list(settings) if name not in PUBLIC}
    preset = Settings.preset("script", waiting_store=sub._store.capabilities.wait)
    resolved = dataclasses.replace(preset.override(**settings), **hidden)
    return sub._worker(workflows, resolved)


async def eventually(what: str, check: Callable[[], Awaitable[bool]], within: float = 5) -> None:
    deadline = time.monotonic() + within
    while not await check():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.01)


async def blocked_with(sub: Substantial, run: Run, within: float = 5) -> Failure:
    """Waits until `run` reads blocked and `runs` lists it so, and returns the error it lists:
    the block creates its hint before its queue's marker, so `runs` may list it a moment later."""
    infos: list[RunInfo] = []

    async def blocked() -> bool:
        if await run.status() != RunStatus.BLOCKED:
            return False
        infos[:] = [i async for i in sub.runs(run.workflow) if i.chain == run.chain]
        return len(infos) == 1 and infos[0].waiting_on == "blocked"

    await eventually(f"run {run.chain} blocked", blocked, within)
    assert infos[0].error is not None and infos[0].due_at is not None, infos
    return infos[0].error


def _holds_a_step(key: str, data: bytes) -> bool:
    """Whether `data` written at `key` is a head whose segments recorded a step."""
    if not key.startswith("runs/"):
        return False
    run = layout.Run.FromString(data)
    segments = run.head.segments if run.WhichOneof("kind") == "head" else []
    return any(e.WhichOneof("kind") == "step" for s in segments for e in s.events)


class Recording:
    """A store around another that records what it is asked, and can fail or freeze on demand."""

    def __init__(self, inner: Store) -> None:
        self.inner = inner
        self.listed: list[str] = []
        self.created: list[tuple[str, str]] = []
        # Each cas and its outcome, `written` or `mismatch`.
        self.cased: list[tuple[str, str]] = []
        # The key and expiry of each create and cas asked, in order.
        self.writes: list[tuple[str, int | None]] = []
        # The days of each `check_expiry`, in order.
        self.checked: list[list[int]] = []
        self.failing = False
        # When set, the first write of a head holding a step freezes every operation until this
        # is set.
        self.freeze_on_segment: asyncio.Event | None = None
        self._frozen: asyncio.Event | None = None
        # When set, a delete of this key waits until its event is set.
        self.hold_delete: tuple[str, asyncio.Event] | None = None

    @property
    def capabilities(self):
        return self.inner.capabilities

    async def _gate(self) -> None:
        from substantial.stores import StoreFailure

        if self._frozen is not None:
            await self._frozen.wait()
        if self.failing:
            raise StoreFailure("injected")

    async def create(self, key: str, data: bytes, *, expiry: int | None) -> str:
        self.writes.append((key, expiry))
        await self._gate()
        outcome = await self.inner.create(key, data, expiry=expiry)
        self.created.append((key, outcome))
        return outcome

    async def cas(self, key: str, data: bytes, etag: str, *, expiry: int | None):
        if self.freeze_on_segment is not None and _holds_a_step(key, data):
            self._frozen = self.freeze_on_segment
            self.freeze_on_segment = None
        self.writes.append((key, expiry))
        await self._gate()
        outcome = await self.inner.cas(key, data, etag, expiry=expiry)
        self.cased.append((key, "mismatch" if outcome == "mismatch" else "written"))
        return outcome

    async def check_expiry(self, days) -> None:
        self.checked.append(list(days))
        await self.inner.check_expiry(days)

    async def get(self, key: str):
        await self._gate()
        return await self.inner.get(key)

    async def list(self, prefix: str, *, start_after, max_keys, wait):
        self.listed.append(prefix)
        await self._gate()
        return await self.inner.list(prefix, start_after=start_after, max_keys=max_keys, wait=wait)

    async def delete(self, key: str) -> None:
        if self.hold_delete is not None and self.hold_delete[0] == key:
            await self.hold_delete[1].wait()
        await self._gate()
        await self.inner.delete(key)

    async def open(self) -> None:
        await self.inner.open()

    async def close(self) -> None:
        await self.inner.close()
