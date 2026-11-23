"""Failovers during a stream of creates: the primary holding the namespace is killed, a replica
takes over, creates are acknowledged again, and every create acknowledged before or during it is
there after, none acknowledged twice, on the containers of `dev/compose.yaml`, which a test fails
without; each test starts the killed container again and waits for it to rejoin as a replica."""

import asyncio
import shutil
import subprocess
import time
from collections import defaultdict

import pytest
from conftest import UP, kind, require_redis, value
from redis.asyncio import Redis

from substantial.stores import StoreFailure, open_store
from substantial.stores.redis_store import RedisStore

pytestmark = pytest.mark.failover

WRITERS_PER_KEY = 2
KEYS_AT_ONCE = 4
# Creates acknowledged before the kill, and again among those sent after the promotion: counts,
# not durations, so that a loaded machine makes the test slower, never failing.
ACKS = 50
WITHIN_S = 60.0


def container(name: str) -> str:
    if shutil.which("docker") is None:
        pytest.fail("docker is not on the PATH")
    found = subprocess.run(["docker", "inspect", name], capture_output=True)
    if found.returncode != 0:
        pytest.fail(f"no container {name}: run `{UP}`")
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


async def stream_through_failover(url: str, prefix: str, failed_over, rejoin_first: bool) -> None:
    """Creates keys, each by two writers at once with their own bytes, while the primary is
    killed, until `ACKS` creates sent after the promotion are acknowledged, then checks each
    answer against the store once the old primary is back. `rejoin_first` starts the killed
    container right after the promotion, for a primary with one replica, which acknowledges no
    create alone (I-26)."""
    store = open_store(url, None)
    assert isinstance(store, RedisStore)
    await store.open()
    where = store._current.where
    killed = container(f"{prefix}-{where.rpartition(':')[2]}")
    answers: dict[str, list[tuple[str, bytes]]] = defaultdict(list)
    failures = 0
    acked = 0
    acked_after = 0
    promoted = False
    stop = asyncio.Event()

    async def writer(slot: int, writer_id: int) -> None:
        nonlocal failures, acked, acked_after
        n = 0
        while not stop.is_set():
            key = f"k/{slot}-{n:06d}"
            n += 1
            data = f"{key} by {writer_id}".encode()
            after = promoted
            try:
                outcome = await store.create(key, data, expiry=None)
            except StoreFailure:
                failures += 1
                continue
            if kind(outcome) == "created":
                acked += 1
                acked_after += after
            answers[key].append((outcome, data))

    async def acked_before_kill() -> bool:
        return acked >= ACKS

    async def acked_after_promotion() -> bool:
        return acked_after >= ACKS

    writers = [
        asyncio.create_task(writer(slot, w))
        for slot in range(KEYS_AT_ONCE)
        for w in range(WRITERS_PER_KEY)
    ]
    try:
        await until("creates acknowledged before the kill", acked_before_kill, WITHIN_S)
        await docker("kill", killed)
        killed_at = time.monotonic()
        await until("a replica promoted", lambda: failed_over(where), WITHIN_S)
        promoted_in = time.monotonic() - killed_at
        promoted = True
        if rejoin_first:
            await docker("start", killed)
        await until("creates acknowledged after the promotion", acked_after_promotion, WITHIN_S)
        resumed_in = time.monotonic() - killed_at - promoted_in
    finally:
        stop.set()
        await asyncio.gather(*writers)
        await docker("start", killed)
    await store.close()
    await until("the old primary back as a replica", lambda: rejoined(where))

    check = open_store(url, None)
    acknowledged = 0
    for key, outcomes in answers.items():
        created = [data for outcome, data in outcomes if kind(outcome) == "created"]
        assert len(created) <= 1, f"{key} created twice: {outcomes}"
        stored = await value(check, key)
        if created:
            acknowledged += 1
            assert stored == created[0], f"{key} acknowledged as {created[0]!r}, holds {stored!r}"
        elif outcomes:
            assert stored is not None, f"{key} answered exists, then missing: {outcomes}"
    await check.close()
    print(
        f"{acknowledged} acknowledged, {failures} store failures, "
        f"a replica promoted {promoted_in:.1f} s after the kill, "
        f"{ACKS} creates sent after it acknowledged {resumed_in:.1f} s later"
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

    await stream_through_failover(url, "substantial-sentinel", failed_over, rejoin_first=False)


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
    await stream_through_failover(url, "substantial-cluster", failed_over, rejoin_first=True)
