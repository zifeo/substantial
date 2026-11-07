"""What the Redis adapter does beyond the shared contract; all but URL parsing run only when
`SUBSTANTIAL_REDIS_URL` names a standalone server (see `dev/redis.compose.yaml`), or, for the
last one, `SUBSTANTIAL_REDIS_SENTINEL_URL` a Sentinel."""

import asyncio
import contextlib
import secrets
import sys
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from conftest import LONG, require_redis, script
from redis.asyncio import Redis

from substantial import ConfigError, Context, Password, StoreError, Substantial, workflow
from substantial.stores import ListWait, RedisLocation, StoreFailure, open_store, redis_store
from substantial.stores.redis_store import RedisStore


def test_urls_need_every_part() -> None:
    ok = RedisLocation.parse("redis://h:6379/ns/sub?db=2&replicas=1")
    assert ok == RedisLocation("standalone", False, (("h", 6379),), "ns/sub", 2, 1, None)
    sentinel = RedisLocation.parse("rediss+sentinel://a:1,[::1]:2/ns?master=m&db=0&replicas=all")
    assert sentinel == RedisLocation("sentinel", True, (("a", 1), ("::1", 2)), "ns", 0, None, "m")
    cluster = RedisLocation.parse("redis+cluster://a:1,b:2/ns?replicas=all")
    assert (cluster.topology, cluster.nodes, cluster.replicas) == (
        "cluster",
        (("a", 1), ("b", 2)),
        None,
    )
    assert RedisLocation.parse("redis://h:1/ns?db=0&replicas=all").replicas is None
    for url in [
        "redis://h:1/ns?db=0",
        "redis://h:1/ns?replicas=0",
        "redis://h/ns?db=0&replicas=0",
        "redis://h:1,g:2/ns?db=0&replicas=0",
        "redis://h:1/?db=0&replicas=0",
        "redis://h:1/a{b}?db=0&replicas=0",
        "redis://h:1/a//b?db=0&replicas=0",
        "redis://h:1/ns?db=x&replicas=0",
        "redis://h:1/ns?db=\u0661&replicas=0",
        "redis://h:1/ns?db=0&replicas=\u0661",
        "redis://h:\u0661/ns?db=0&replicas=0",
        "redis://h:1/ns?db=0&replicas=-1",
        "redis://h:1/ns?db=0&db=1&replicas=0",
        "redis://h:1/ns?db=0&replicas=0&timeout=1",
        "redis://u:p@h:1/ns?db=0&replicas=0",
        "redis://h:1/ns?db=0&replicas=0#x",
        "redis+sentinel://a:1/ns?db=0&replicas=all",
        "redis+sentinel://a:1/ns?master=&db=0&replicas=all",
        "redis+sentinel://a:1/ns?master=m&db=0&replicas=1",
        "redis+cluster://a:1/ns?replicas=0",
        "redis+cluster://a:1/ns?db=0&replicas=all",
        "valkey://h:1/ns?db=0&replicas=0",
    ]:
        with pytest.raises(ConfigError):
            RedisLocation.parse(url)


def test_redis_urls_without_the_extra_name_it(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [
        n
        for n in sys.modules
        if n == "redis" or n.startswith(("redis.", "substantial.stores.redis_"))
    ]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setitem(sys.modules, "redis", None)
    with pytest.raises(ConfigError, match=r'pip install "substantial\[redis\]"'):
        open_store("redis://127.0.0.1:1/ns?db=0&replicas=0", None)


def test_credentials_are_a_password(monkeypatch: pytest.MonkeyPatch) -> None:
    url = "redis://127.0.0.1:1/ns?db=0&replicas=0"
    assert "hunter2" not in repr(Password("hunter2", username="app"))
    for wrong in ["", None]:
        with pytest.raises(ConfigError):
            Password(wrong)  # type: ignore[arg-type]
    later = datetime.now(UTC) + timedelta(hours=1)
    with pytest.raises(ConfigError, match="a provider returns"):
        open_store(url, Password("p", expires_at=later))
    with pytest.raises(ConfigError, match="a Redis store takes"):
        open_store(url, "environment")  # type: ignore[arg-type]
    monkeypatch.delenv("REDIS_PASSWORD", raising=False)
    with pytest.raises(ConfigError, match="REDIS_PASSWORD"):
        open_store(url, "env")
    monkeypatch.setenv("REDIS_PASSWORD", "p")
    open_store(url, "env")
    open_store(url, None)
    with pytest.raises(ConfigError):
        open_store("s3+http://127.0.0.1:1/b/ns?region=r", Password("p"))


@pytest.fixture
async def admin() -> AsyncIterator[Redis]:
    location = RedisLocation.parse(require_redis("redis"))
    host, port = location.nodes[0]
    client = Redis(host=host, port=port)
    yield client
    await client.aclose()


@contextlib.asynccontextmanager
async def config(admin: Redis, name: str, value: str) -> AsyncIterator[None]:
    before = (await admin.config_get(name))[name]
    await admin.config_set(name, value)
    try:
        yield
    finally:
        await admin.config_set(name, before)


async def test_a_server_without_aof_is_refused(admin: Redis) -> None:
    async with config(admin, "appendonly", "no"):
        with pytest.raises(ConfigError, match="no AOF"):
            await Substantial.create(require_redis("redis"), partitions=1)


async def test_a_server_that_evicts_is_refused(admin: Redis) -> None:
    async with config(admin, "maxmemory-policy", "allkeys-lru"):
        with pytest.raises(ConfigError, match="allkeys-lru"):
            await Substantial.create(require_redis("redis"), partitions=1)


async def test_a_password_reaches_the_server(admin: Redis) -> None:
    await admin.acl_setuser(
        "substantial-test",
        enabled=True,
        passwords=["+right"],
        keys="*",
        channels="*",
        commands=["+@all"],
    )
    url = require_redis("redis")
    calls: list[int] = []

    async def provider() -> Password:
        calls.append(1)
        return Password("wrong" if len(calls) == 1 else "right", username="substantial-test")

    try:
        with pytest.raises(StoreError, match=r"WRONGPASS|invalid username-password"):
            await Substantial.create(url, partitions=1, credentials=provider)
        sub = await Substantial.create(url, partitions=1, credentials=provider)
        await sub.close()
        assert len(calls) == 2, "asked again after a failed connection, then once"
        right = Password("right", username="substantial-test")
        explicit = await Substantial.open(url, credentials=right)
        await explicit.close()
    finally:
        await admin.acl_deluser("substantial-test")


@contextlib.asynccontextmanager
async def listings(admin: Redis, url: str) -> AsyncIterator[list[int]]:
    """The listings of the namespace of `url` the server runs while in the block, counted in
    `[0]`: MONITOR, not the server's command counts, as other clients list on it meanwhile."""
    index = f"{{{RedisLocation.parse(url).namespace}}}:i"
    counted = [0]

    async def count() -> None:
        async with admin.monitor() as monitor:
            async for command in monitor.listen():
                if command["command"].split(" ")[:2] == ["ZRANGE", index]:
                    counted[0] += 1

    task = asyncio.create_task(count())
    try:
        yield counted
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_a_waiting_listing_returns_as_a_key_lands_without_polling(admin: Redis) -> None:
    url = require_redis("redis")
    store = open_store(url, None)
    async with listings(admin, url) as listed:
        await store.create("q/a", b"a")
        first = await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0))
        assert first.keys == ["q/a"] and first.etag
        before = listed[0]
        started = time.monotonic()
        unchanged = await store.list(
            "q/", start_after=None, max_keys=10, wait=ListWait(2, first.etag)
        )
        assert unchanged == first
        assert time.monotonic() - started >= 2
        assert listed[0] - before == 1, "listed once, then waited"
    held = asyncio.create_task(
        store.list("q/", start_after=None, max_keys=10, wait=ListWait(20, first.etag))
    )
    await asyncio.sleep(0.5)
    await store.create("other/x", b"x")
    await asyncio.sleep(0.2)
    assert not held.done(), "a key under another prefix does not end the wait"
    started = time.monotonic()
    await store.create("q/b", b"b")
    woken = await held
    assert time.monotonic() - started < 0.5
    assert woken.keys == ["q/a", "q/b"] and woken.etag != first.etag
    await store.close()


async def test_a_waiting_listing_on_a_closed_store_fails_at_once() -> None:
    store = open_store(require_redis("redis"), None)
    await store.list("q/", start_after=None, max_keys=10, wait=ListWait(1))
    await store.close()
    started = time.monotonic()
    with pytest.raises(StoreFailure, match="closed"):
        await store.list("q/", start_after=None, max_keys=10, wait=ListWait(1))
    assert time.monotonic() - started < 1


@contextlib.asynccontextmanager
async def user(admin: Redis, *commands: str) -> AsyncIterator[str]:
    """An ACL user of password `right` allowed `commands`, deleted after the block."""
    name = f"substantial-test-{secrets.token_hex(4)}"
    await admin.acl_setuser(
        name, enabled=True, passwords=["+right"], keys="*", channels="*", commands=list(commands)
    )
    try:
        yield name
    finally:
        await admin.acl_deluser(name)


async def test_a_user_denied_waitaof_is_refused_as_such(admin: Redis) -> None:
    async with user(admin, "+@all", "-waitaof") as name:
        with pytest.raises(StoreError, match="no permissions"):
            await Substantial.create(
                require_redis("redis"), partitions=1, credentials=Password("right", username=name)
            )


async def test_a_hung_password_provider_fails_only_its_attempt(
    admin: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(redis_store, "ATTEMPT_TIMEOUT", 0.5)
    async with user(admin, "+@all") as name:
        calls: list[int] = []

        async def provider() -> Password:
            calls.append(1)
            if len(calls) == 1:
                await asyncio.Event().wait()
            return Password("right", username=name)

        store = open_store(require_redis("redis"), provider)
        async with asyncio.timeout(10):
            assert await store.get("k") is None
        assert len(calls) == 2
        await store.close()


async def test_a_subscription_lost_again_resubscribes_at_once(admin: Redis) -> None:
    url = require_redis("redis")
    channel = f"{{{RedisLocation.parse(url).namespace}}}:c"
    async with user(admin, "+@all") as name:
        store = open_store(url, Password("right", username=name))
        await store.list("q/", start_after=None, max_keys=10, wait=ListWait(1))
        for _ in range(6):
            assert await admin.client_kill_filter(user=name) >= 1
            started = time.monotonic()
            while (await admin.pubsub_numsub(channel))[0][1] == 0:
                assert time.monotonic() - started < 1, "subscribed again within a second"
                await asyncio.sleep(0.01)
        await store.close()


async def test_a_start_from_another_client_comes_through_the_wait() -> None:
    @workflow
    async def answer(c: Context) -> int:
        return 42

    url = require_redis("redis")
    sub = await Substantial.create(url, partitions=1)
    async with sub.worker([answer], preset="script", max_idle=timedelta(minutes=5)):
        await asyncio.sleep(0.5)
        other = await Substantial.open(url)
        started = time.monotonic()
        run = await other.start(answer)
        assert await run.result(timeout=LONG) == 42
        assert time.monotonic() - started < 2
        await other.close()
    await sub.close()


async def test_a_run_ends_on_redis_through_a_script_worker() -> None:
    @workflow
    async def double(c: Context, n: int) -> int:
        return n * 2

    sub = await Substantial.create(require_redis("redis"), partitions=2)
    async with script(sub, [double]):
        assert await (await sub.start(double, {"n": 21})).result(timeout=LONG) == 42
    await sub.close()


async def test_a_primary_without_a_replica_online_answers_nothing() -> None:
    url = require_redis("redis-sentinel")
    store = open_store(url, None)
    assert isinstance(store, RedisStore)
    await store.open()
    assert store._current is not None
    # As just promoted, its replicas still syncing.
    store._current.replicas = 0
    for answer in (store.create("k", b"v"), store.get("k"), store.delete("k")):
        with pytest.raises(StoreFailure, match="no replica online"):
            await answer
    await store.close()
    counted = open_store(url, None)
    assert await counted.get("k") is None, "nothing written"
    await counted.close()
