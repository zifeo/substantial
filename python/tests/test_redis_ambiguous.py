"""Ambiguous creates on Redis (I-26), injected by a proxy that drops a connection; only when
`SUBSTANTIAL_REDIS_URL` names a standalone server (see `dev/redis.compose.yaml`)."""

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest
from conftest import require_redis
from redis.asyncio import Redis

from substantial.stores import RedisLocation, StoreFailure, open_store, redis_store


class Dropping:
    """A TCP proxy to the server that drops the connection of the next request holding
    `marker`: after the server got it, so it runs and its answer is lost, or before."""

    def __init__(self, host: str, port: int) -> None:
        self._target = (host, port)
        self.marker: bytes | None = None
        self.forward = False
        self.dropped = 0
        self.port = 0

    def drop(self, marker: bytes, *, forward: bool) -> None:
        self.marker, self.forward = marker, forward

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        up_reader, up_writer = await asyncio.open_connection(*self._target)
        muted = False

        async def upstream() -> None:
            nonlocal muted
            while data := await reader.read(65536):
                if self.marker is not None and self.marker in data:
                    self.marker = None
                    self.dropped += 1
                    muted = True
                    if self.forward:
                        up_writer.write(data)
                        await up_writer.drain()
                        await asyncio.sleep(0.2)
                    return
                up_writer.write(data)
                await up_writer.drain()

        async def downstream() -> None:
            while data := await up_reader.read(65536):
                if not muted:
                    writer.write(data)
                    await writer.drain()

        down = asyncio.create_task(downstream())
        with contextlib.suppress(ConnectionError):
            await upstream()
        down.cancel()
        for w in (writer, up_writer):
            w.close()

    @contextlib.asynccontextmanager
    async def serving(self) -> AsyncIterator[None]:
        server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = server.sockets[0].getsockname()[1]
        async with server:
            yield
            server.close_clients()


@pytest.fixture
async def proxied() -> AsyncIterator[tuple[str, Dropping, Redis, str]]:
    url = require_redis("redis")
    location = RedisLocation.parse(url)
    host, port = location.nodes[0]
    proxy = Dropping(host, port)
    admin = Redis(host=host, port=port)
    async with proxy.serving():
        through = url.replace(f"{host}:{port}", f"127.0.0.1:{proxy.port}", 1)
        yield through, proxy, admin, f"{{{location.namespace}}}:f"
    await admin.aclose()


async def fence(admin: Redis, key: str) -> int:
    return int(await admin.get(key) or 0)


@pytest.mark.parametrize("forward", [True, False], ids=["landed", "lost"])
async def test_an_unanswered_create_is_resolved_by_a_fenced_get(
    proxied: tuple[str, Dropping, Redis, str], forward: bool
) -> None:
    url, proxy, admin, fence_key = proxied
    store = open_store(url, None)
    await store.open()
    before = await fence(admin, fence_key)
    proxy.drop(b"EVALSHA", forward=forward)
    assert await store.create("runs/a/log/0", b"one") == "created"
    assert proxy.dropped == 1
    assert await fence(admin, fence_key) == before + 1, "the get resolving it wrote the fence"
    assert await store.get("runs/a/log/0") == b"one"
    assert await store.create("runs/a/log/0", b"two") == "exists"
    await store.close()


async def test_a_create_the_replicas_never_fsync_is_not_acknowledged(
    proxied: tuple[str, Dropping, Redis, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    url, _, admin, fence_key = proxied
    monkeypatch.setattr(redis_store, "WAITAOF_MS", 50)
    store = open_store(url.replace("replicas=0", "replicas=1"), None)
    with pytest.raises(StoreFailure, match=r"WAITAOF 1 1 answered \[1, 0\]"):
        await store.create("runs/a/log/0", b"one")
    landed = await admin.get(fence_key.replace(":f", ":o:runs/a/log/0"))
    assert landed == b"one", "it landed, never acknowledged"
    await store.close()


async def test_reads_and_deletes_answer_once_what_they_saw_is_durable(
    proxied: tuple[str, Dropping, Redis, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    url, _, admin, fence_key = proxied
    store = open_store(url, None)
    await store.create("runs/a/start", b"one")
    before = await fence(admin, fence_key)
    assert await store.get("runs/a/start") == b"one"
    assert await store.get("runs/b/start") is None
    assert (await store.list("runs/", start_after=None, max_keys=10, wait=None)).keys == [
        "runs/a/start"
    ]
    await store.delete("runs/b/start")
    assert await fence(admin, fence_key) == before + 4, "each read and a delete of nothing"
    await store.close()
    monkeypatch.setattr(redis_store, "WAITAOF_MS", 50)
    unreplicated = open_store(url.replace("replicas=0", "replicas=1"), None)
    short = r"WAITAOF 1 1 answered \[1, 0\]"
    for key in ("runs/a/start", "runs/b/start"):
        with pytest.raises(StoreFailure, match=short):
            await unreplicated.get(key)
        with pytest.raises(StoreFailure, match=short):
            await unreplicated.list(key, start_after=None, max_keys=10, wait=None)
        with pytest.raises(StoreFailure, match=short):
            await unreplicated.delete(key)
    await unreplicated.close()
