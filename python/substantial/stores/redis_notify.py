"""The notifications behind the waiting listings of the Redis store."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from redis.asyncio import Redis

from substantial.stores.redis import CONNECT_TIMEOUT
from substantial.stores.retry import STORE_RETRY, Permanent, Transient

log = logging.getLogger("substantial")
# A subscription silent this long gets a PING; one silent twice as long is lost.
PING_EVERY = 10.0


class Notifications:
    """The store's one subscription to the channel of its namespace, where every create and
    delete publishes its key, demultiplexed to the waiting listings whose prefix the key has.

    A lost subscription wakes every listing waiting on it, so each lists again once subscribed
    anew: no notification is missed between a listing's page and its wait."""

    def __init__(
        self,
        primary: Callable[[], Awaitable[Any]],
        lost: Callable[[Any], None],
        client: Callable[[Any], Redis],
        channel: str,
        sharded: bool,
    ) -> None:
        self._primary = primary
        self._lost = lost
        self._client = client
        self._channel = channel
        self._subscribe = "SSUBSCRIBE" if sharded else "SUBSCRIBE"
        self._message = b"smessage" if sharded else b"message"
        self._waiters: dict[str, set[asyncio.Event]] = {}
        self._ready = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._closed = False
        self._error = "not subscribed yet"

    async def watch(self, prefix: str) -> asyncio.Event:
        """An event set once a key under `prefix` is created or deleted, or the subscription is
        lost, from now on."""
        if self._closed:
            raise Permanent("the store is closed")
        if self._task is None:
            self._task = asyncio.create_task(self._run())
        try:
            async with asyncio.timeout(2 * CONNECT_TIMEOUT):
                await self._ready.wait()
        except TimeoutError:
            raise Transient(f"no subscription to {self._channel}: {self._error}") from None
        event = asyncio.Event()
        self._waiters.setdefault(prefix, set()).add(event)
        return event

    def unwatch(self, prefix: str, event: asyncio.Event) -> None:
        events = self._waiters.get(prefix)
        if events is not None:
            events.discard(event)
            if not events:
                del self._waiters[prefix]

    def _wake(self, key: str | None) -> None:
        for prefix, events in self._waiters.items():
            if key is None or key.startswith(prefix):
                for event in events:
                    event.set()

    async def _run(self) -> None:
        delay = STORE_RETRY.initial.total_seconds()
        while True:
            primary = None
            client = None
            try:
                async with asyncio.timeout(2 * CONNECT_TIMEOUT):
                    primary = await self._primary()
                client = self._client(primary)
                await self._listen(await client.connection_pool.get_connection())
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._error = f"{type(e).__name__}: {e}"
                log.debug("subscription to %s lost: %s", self._channel, self._error)
                if primary is not None:
                    self._lost(primary)
            finally:
                if self._ready.is_set():
                    delay = STORE_RETRY.initial.total_seconds()
                self._ready.clear()
                self._wake(None)
                if client is not None:
                    await client.aclose()
            await asyncio.sleep(delay)
            delay = min(delay * 2, STORE_RETRY.max.total_seconds())

    async def _listen(self, conn: Any) -> None:
        await conn.send_command(self._subscribe, self._channel)
        reply = await conn.read_response(timeout=CONNECT_TIMEOUT)
        if not reply or reply[0] != self._subscribe.lower().encode():
            raise Transient(f"{self._subscribe} {self._channel} answered {reply!r}")
        self._ready.set()
        pinged = False
        while True:
            message = await conn.read_response(timeout=PING_EVERY)
            if message is None:
                if pinged:
                    raise Transient(f"the subscription answered no PING in {PING_EVERY} s")
                await conn.send_command("PING")
                pinged = True
            elif message[0] == self._message:
                self._wake(message[2].decode())
            elif message[0] == b"pong":
                pinged = False
            else:
                # An unsubscribe the server sent, as when the slot moved to another node.
                raise Transient(f"the subscription ended with {message!r}")

    async def close(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
