"""Stores and helpers shared by the tests; S3 only when `SUBSTANTIAL_S3_URL` is set (see
`dev/rustfs.compose.yaml`)."""

from __future__ import annotations

import asyncio
import dataclasses
import os
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import timedelta
from pathlib import Path

import pytest

from substantial import Substantial, Workflow
from substantial.settings import PUBLIC, Settings
from substantial.stores import Store, open_store
from substantial.stores.fs import FsStore
from substantial.stores.memory import MemoryStore
from substantial.worker import Worker

S3_URL = "SUBSTANTIAL_S3_URL"
LONG = timedelta(seconds=10)
ADAPTERS = ["memory", "file", "s3"]


def s3_url(scheme: str = "s3") -> str | None:
    """A fresh namespace under the one `SUBSTANTIAL_S3_URL` names, as a URL of `scheme` (`s3` or
    `h3`, its `+http` kept); `None` when the variable is not set."""
    url = os.environ.get(S3_URL)
    if url is None:
        return None
    given, _, rest = url.partition("://")
    plain = {"s3": "", "s3+http": "+http"}.get(given)
    if plain is None:
        raise AssertionError(f"{S3_URL} is not an s3:// or s3+http:// URL: {url!r}")
    path, _, query = rest.partition("?")
    if not query:
        raise AssertionError(f"{S3_URL} has no query: {url!r}")
    return f"{scheme}{plain}://{path}/{secrets.token_hex(8)}?{query}"


def require_s3(scheme: str = "s3") -> str:
    url = s3_url(scheme)
    if url is None:
        pytest.skip(f"{S3_URL} is not set")
    return url


def fresh_store(adapter: str, tmp_path: Path) -> Store:
    """A fresh namespace in an adapter of this host."""
    if adapter == "memory":
        return MemoryStore(f"test-{secrets.token_hex(8)}")
    if adapter == "file":
        return FsStore(tmp_path / f"ns-{secrets.token_hex(4)}")
    return open_store(require_s3(), "env")


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


class Recording:
    """A store around another that records what it is asked, and can fail or freeze on demand."""

    def __init__(self, inner: Store) -> None:
        self.inner = inner
        self.listed: list[str] = []
        self.created: list[tuple[str, str]] = []
        self.failing = False
        # When set, the first create of a segment freezes every operation for this many seconds.
        self.freeze_on_segment: float | None = None
        self._frozen_until: float | None = None

    @property
    def capabilities(self):
        return self.inner.capabilities

    async def _gate(self) -> None:
        from substantial.stores import StoreFailure

        if self._frozen_until is not None:
            await asyncio.sleep(max(0.0, self._frozen_until - time.monotonic()))
        if self.failing:
            raise StoreFailure("injected")

    async def create(self, key: str, data: bytes) -> str:
        if "/log/" in key and self.freeze_on_segment is not None:
            self._frozen_until = time.monotonic() + self.freeze_on_segment
            self.freeze_on_segment = None
        await self._gate()
        outcome = await self.inner.create(key, data)
        self.created.append((key, outcome))
        return outcome

    async def get(self, key: str) -> bytes | None:
        await self._gate()
        return await self.inner.get(key)

    async def list(self, prefix: str, *, start_after, max_keys, wait):
        self.listed.append(prefix)
        await self._gate()
        return await self.inner.list(prefix, start_after=start_after, max_keys=max_keys, wait=wait)

    async def delete(self, key: str) -> None:
        await self._gate()
        await self.inner.delete(key)

    async def close(self) -> None:
        await self.inner.close()
