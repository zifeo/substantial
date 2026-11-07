"""The Redis adapter, over redis-py, which the `redis` extra installs."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from typing import Any

from redis.asyncio import Redis
from redis.crc import key_slot
from redis.exceptions import AuthenticationError, NoPermissionError, RedisError, ResponseError

from substantial.errors import ConfigError
from substantial.stores import retry
from substantial.stores.base import Capabilities, Create, Listing, ListWait
from substantial.stores.credentials import CredentialsArg
from substantial.stores.redis import (
    ATTEMPT_TIMEOUT,
    MAX_WAIT_SECONDS,
    RECOUNT_EVERY,
    WAITAOF_MS,
    RedisLocation,
)
from substantial.stores.redis_connect import CREATE, DELETE, Connector, Primary, ProviderFailed
from substantial.stores.redis_notify import Notifications
from substantial.stores.retry import STORE_RETRY, Ambiguous, Fault, Permanent, Transient


class RedisStore:
    """`redis://`, `redis+sentinel://` and `redis+cluster://`: the namespace `<ns>` is the hash
    tag of its keys, so they live on one slot. A key is the string `{<ns>}:o:<key>` and a member
    of the sorted set `{<ns>}:i`, its index, listed by lex range; every create and delete
    publishes the key on `{<ns>}:c`, which waiting listings subscribe to.

    A create is the script `create.lua`, then `WAITAOF` on the same connection: acknowledged
    once fsynced on the primary and on `replicas`. The client's own retries are off:
    `STORE_RETRY` is the only retry."""

    capabilities = Capabilities(wait=True)

    def __init__(self, location: RedisLocation, credentials: CredentialsArg) -> None:
        if not isinstance(location, RedisLocation):
            raise ConfigError(f"a Redis store needs a RedisLocation, not {location!r}")
        self._location = location
        tag = f"{{{location.namespace}}}"
        self._object = f"{tag}:o:".encode()
        self._index = f"{tag}:i"
        self._fence = f"{tag}:f"
        self._channel = f"{tag}:c"
        cluster = location.topology == "cluster"
        self._publish = "SPUBLISH" if cluster else "PUBLISH"
        self._connector = Connector(location, credentials, key_slot(self._index.encode()))
        self._current: Primary | None = None
        self._lock = asyncio.Lock()
        self._closed = False
        self._closing: set[asyncio.Task] = set()
        self._notifications = Notifications(
            self._primary,
            self._lost,
            lambda p: self._connector.client(p.where),
            self._channel,
            cluster,
        )

    async def _primary(self) -> Primary:
        async with self._lock:
            if self._closed:
                raise Permanent("the store is closed")
            if self._current is None:
                self._current = await self._connector.connect((CREATE, DELETE))
            return self._current

    def _lost(self, primary: Primary) -> None:
        """Forgets a primary after a failure on it, so the next attempt finds it again."""
        if self._current is primary:
            self._current = None
            task = asyncio.create_task(primary.client.aclose())
            self._closing.add(task)
            task.add_done_callback(self._closing.discard)

    async def _attempt[T](
        self, what: str, op: Callable[[Primary], Awaitable[T]], *, create: bool = False
    ) -> T:
        """One attempt: a failure without an answer is `Ambiguous` for a create and `Transient`
        otherwise, an error answer `Transient`, a refused password `Permanent`."""
        primary = None
        try:
            # Bounded too: a provider that never answers would hold the lock of every attempt.
            async with asyncio.timeout(ATTEMPT_TIMEOUT):
                primary = await self._primary()
            async with asyncio.timeout(ATTEMPT_TIMEOUT):
                return await op(primary)
        except Fault:
            raise
        except (RedisError, OSError, TimeoutError) as e:
            text = f"{what}: {type(e).__name__}: {e}"
            cause: BaseException | None = e
            while cause is not None:
                if isinstance(cause, ProviderFailed | AuthenticationError | NoPermissionError):
                    raise Permanent(text) from e
                cause = cause.__cause__
            if primary is not None:
                self._lost(primary)
            if isinstance(e, ResponseError) or primary is None or not create:
                raise Transient(text) from e
            raise Ambiguous(text) from e

    async def _durable(self, primary: Primary, conn: Redis, what: str, *, created: bool) -> None:
        """`WAITAOF` on the connection of the last write: one short of the primary and its
        replicas leaves that write, and those before it, not durable yet.

        A separate round trip: with the write in the same pipeline, replicas acknowledge their
        fsync only at their next second."""
        fault = Ambiguous if created else Transient
        try:
            replicas = await self._replicas(primary)
        except Transient as e:
            raise fault(f"{what}: {e}") from None
        try:
            waited: Any = await conn.execute_command("WAITAOF", 1, replicas, WAITAOF_MS)
            if waited[0] >= 1 and waited[1] >= replicas:
                return
        except ResponseError as e:
            waited = e
        if self._location.replicas is None:
            await self._count(primary)
        raise fault(f"{what}: WAITAOF 1 {replicas} answered {waited!r}")

    async def _replicas(self, primary: Primary) -> int:
        """The replicas a create waits for, counted again every `RECOUNT_EVERY` for `all`.

        On Sentinel and Cluster, a primary with no replica online acknowledges nothing: just
        promoted, its replicas still syncing, it could itself be replaced by one of them, which
        would lack what it acknowledged alone."""
        if self._location.replicas is not None:
            return self._location.replicas
        if asyncio.get_running_loop().time() - primary.counted_at >= RECOUNT_EVERY:
            await self._count(primary)
        if primary.replicas == 0 and self._location.topology != "standalone":
            raise Transient(f"{primary.where} has no replica online")
        return primary.replicas

    async def _count(self, primary: Primary) -> None:
        info = await primary.client.execute_command("INFO", "replication")
        primary.replicas = self._connector.replicas(info)
        primary.counted_at = asyncio.get_running_loop().time()

    def _key(self, key: str) -> bytes:
        return self._object + key.encode()

    async def _put(self, primary: Primary, key: str, data: bytes) -> Create:
        await self._replicas(primary)
        async with primary.client.client() as conn:
            keys = (self._key(key), self._index, self._fence)
            created = await conn.evalsha(
                CREATE.sha, 3, *keys, key, data, self._channel, self._publish
            )
            outcome: Create = "created" if created == 1 else "exists"
            await self._durable(primary, conn, f"create {key}", created=outcome == "created")
            return outcome

    async def _read(self, primary: Primary, what: str, *command: Any, resolving: bool) -> Any:
        """A read answered once what it saw is durable: a fence written after it on this
        connection, then fsynced, covers it, as the AOF and the replication stream are one ordered
        log. A key once read never disappears in a failover, nor does one once missing come back,
        as history keys are deleted too, by collections. `resolving` marks the get that resolves
        an ambiguous create, ambiguous again when the fence falls short."""
        async with primary.client.client() as conn:
            pipe = conn.pipeline(transaction=False)
            pipe.execute_command(*command)
            pipe.incr(self._fence)
            found, _ = await pipe.execute()
            await self._durable(primary, conn, what, created=resolving)
            return found

    async def _page(self, primary: Primary, prefix: str, after: str | None, count: int) -> list:
        lower = b"[" + prefix.encode()
        if after is not None and after >= prefix:
            lower = b"(" + after.encode()
        # UTF-8 has no byte 0xff, so every key under the prefix sorts before this bound.
        upper = b"(" + prefix.encode() + b"\xff"
        command = ("ZRANGE", self._index, lower, upper, "BYLEX", "LIMIT", 0, count)
        members = await self._read(primary, f"list {prefix}", *command, resolving=False)
        return [m.decode() for m in members]

    async def _list_once(
        self, prefix: str, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        what = f"list {prefix}"
        if wait is None:
            keys = await self._attempt(what, lambda p: self._page(p, prefix, start_after, max_keys))
            return Listing(keys=keys)
        if not 0 <= wait.seconds <= MAX_WAIT_SECONDS:
            raise Permanent(
                f"a waiting listing waits 0 to {MAX_WAIT_SECONDS} s, not {wait.seconds}"
            )
        deadline = asyncio.get_running_loop().time() + wait.seconds
        while True:
            # Watching before listing: a key landing after the page wakes the wait.
            event = await self._notifications.watch(prefix) if wait.seconds else None
            try:
                page = await self._attempt(
                    what, lambda p: self._page(p, prefix, start_after, max_keys + 1)
                )
                keys = page[:max_keys]
                digest = hashlib.sha256(repr((keys, len(page) > max_keys)).encode())
                etag = digest.hexdigest()[:16]
                if event is None or etag != wait.etag:
                    return Listing(keys=keys, etag=etag)
                try:
                    async with asyncio.timeout_at(deadline):
                        await event.wait()
                except TimeoutError:
                    return Listing(keys=keys, etag=etag)
            finally:
                if event is not None:
                    self._notifications.unwatch(prefix, event)

    async def open(self) -> None:
        async def connected(_: Primary) -> None:
            pass

        await retry.with_retry(STORE_RETRY, lambda: self._attempt("connect", connected))

    async def create(self, key: str, data: bytes) -> Create:
        data = bytes(data)
        return await retry.create(
            STORE_RETRY,
            data,
            lambda: self._attempt(f"create {key}", lambda p: self._put(p, key, data), create=True),
            lambda: self._attempt(f"get {key}", lambda p: self._get(p, key, resolving=True)),
        )

    async def _get(self, primary: Primary, key: str, *, resolving: bool) -> bytes | None:
        what = f"get {key}"
        return await self._read(primary, what, "GET", self._key(key), resolving=resolving)

    async def get(self, key: str) -> bytes | None:
        return await retry.with_retry(
            STORE_RETRY,
            lambda: self._attempt(f"get {key}", lambda p: self._get(p, key, resolving=False)),
        )

    async def list(
        self, prefix: str, *, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        return await retry.with_retry(
            STORE_RETRY, lambda: self._list_once(prefix, start_after, max_keys, wait)
        )

    async def delete(self, key: str) -> None:
        async def remove(p: Primary) -> None:
            async with p.client.client() as conn:
                keys = (self._key(key), self._index, self._fence)
                await conn.evalsha(DELETE.sha, 3, *keys, key, self._channel, self._publish)
                await self._durable(p, conn, f"delete {key}", created=False)

        await retry.with_retry(STORE_RETRY, lambda: self._attempt(f"delete {key}", remove))

    async def close(self) -> None:
        await self._notifications.close()
        async with self._lock:
            self._closed = True
            if self._current is not None:
                self._lost(self._current)
        await asyncio.gather(*self._closing, return_exceptions=True)
