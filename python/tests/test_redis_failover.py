"""Failovers during a stream of creates: the primary holding the namespace is killed, a replica
takes over, and every create acknowledged before or during it is there after, none acknowledged
twice. Only with the containers of `dev/redis-sentinel.compose.yaml` and
`dev/redis-cluster.compose.yaml` and their variables set; each test starts the killed container
again and waits for it to rejoin as a replica."""

import asyncio
import shutil
import subprocess
import time
from collections import defaultdict

import pytest
from conftest import require_redis
from redis.asyncio import Redis

from substantial.stores import StoreFailure, open_store
from substantial.stores.redis_store import RedisStore

WRITERS_PER_KEY = 2
KEYS_AT_ONCE = 4
AFTER_FAILOVER_S = 3.0


def container(name: str) -> str:
    if shutil.which("docker") is None:
        pytest.skip("docker is not on the PATH")
    found = subprocess.run(["docker", "inspect", name], capture_output=True)
    if found.returncode != 0:
        pytest.skip(f"no container {name}")
    return name


async def docker(*args: str) -> None:
    process = await asyncio.create_subprocess_exec("docker", *args, stdout=subprocess.DEVNULL)
    assert await process.wait() == 0, f"docker {' '.join(args)}"


async def until(what: str, check, within: float = 60) -> None:
    deadline = time.monotonic() + within
    while True:
        try:
            if await check():
                return
        except Exception:
            pass
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.2)


async def stream_through_failover(url: str, prefix: str, failed_over) -> None:
    """Creates keys, each by two writers at once with their own bytes, while the primary is
    killed, then checks each answer against the store once the old primary is back."""
    store = open_store(url, None)
    assert isinstance(store, RedisStore)
    await store.open()
    where = store._current.where
    killed = container(f"{prefix}-{where.rpartition(':')[2]}")
    answers: dict[str, list[tuple[str, bytes]]] = defaultdict(list)
    failures = 0
    stop = asyncio.Event()

    async def writer(slot: int, writer_id: int) -> None:
        nonlocal failures
        n = 0
        while not stop.is_set():
            key = f"k/{slot}-{n:06d}"
            n += 1
            data = f"{key} by {writer_id}".encode()
            try:
                answers[key].append((await store.create(key, data), data))
            except StoreFailure:
                failures += 1

    writers = [
        asyncio.create_task(writer(slot, w))
        for slot in range(KEYS_AT_ONCE)
        for w in range(WRITERS_PER_KEY)
    ]
    try:
        await asyncio.sleep(1)
        await docker("kill", killed)
        killed_at = time.monotonic()
        await until("a replica promoted", lambda: failed_over(where))
        promoted_in = time.monotonic() - killed_at
        await asyncio.sleep(AFTER_FAILOVER_S)
    finally:
        stop.set()
        await asyncio.gather(*writers)
        await docker("start", killed)
    await store.close()
    await until("the old primary back as a replica", lambda: rejoined(where))

    check = open_store(url, None)
    acknowledged = 0
    for key, outcomes in answers.items():
        created = [data for outcome, data in outcomes if outcome == "created"]
        assert len(created) <= 1, f"{key} created twice: {outcomes}"
        stored = await check.get(key)
        if created:
            acknowledged += 1
            assert stored == created[0], f"{key} acknowledged as {created[0]!r}, holds {stored!r}"
        elif outcomes:
            assert stored is not None, f"{key} answered exists, then missing: {outcomes}"
    await check.close()
    assert acknowledged > 100, f"only {acknowledged} creates acknowledged"
    print(
        f"{acknowledged} acknowledged, {failures} store failures, "
        f"a replica promoted {promoted_in:.1f} s after the kill"
    )


async def role(where: str) -> str:
    host, _, port = where.rpartition(":")
    client = Redis(host=host, port=int(port))
    try:
        info = await client.info("replication")
        return f"{info['role']}:{info.get('master_link_status', '')}"
    finally:
        await client.aclose()


async def rejoined(where: str) -> bool:
    return await role(where) == "slave:up"


async def test_a_sentinel_failover_keeps_every_acknowledged_create() -> None:
    url = require_redis("redis-sentinel")
    sentinels = url.partition("://")[2].partition("/")[0].split(",")
    master = url.partition("master=")[2].partition("&")[0]

    async def failed_over(old: str) -> bool:
        host, _, port = sentinels[0].rpartition(":")
        client = Redis(host=host, port=int(port))
        try:
            answer = await client.execute_command("SENTINEL", "GET-MASTER-ADDR-BY-NAME", master)
        finally:
            await client.aclose()
        return f"{answer[0].decode()}:{int(answer[1])}" != old

    await stream_through_failover(url, "substantial-sentinel", failed_over)


async def test_a_cluster_failover_keeps_every_acknowledged_create() -> None:
    url = require_redis("redis-cluster")

    async def failed_over(old: str) -> bool:
        replica = await replica_of(old)
        return replica is not None and (await role(replica)).startswith("master")

    replicas: dict[str, str] = {}

    async def replica_of(primary: str) -> str | None:
        if primary not in replicas:
            host, _, port = primary.rpartition(":")
            client = Redis(host=host, port=int(port))
            try:
                info = await client.info("replication")
            finally:
                await client.aclose()
            replicas[primary] = f"{info['slave0']['ip']}:{info['slave0']['port']}"
        return replicas[primary]

    store = open_store(url, None)
    await store.open()
    await replica_of(store._current.where)
    await store.close()
    await stream_through_failover(url, "substantial-cluster", failed_over)
