"""Opening namespaces and running workers on an idle namespace."""

from __future__ import annotations

import asyncio
import logging
import os
import signal

import pytest
from conftest import LONG, Recording, eventually, script

from substantial import (
    ConfigError,
    Context,
    Credentials,
    InvalidArgument,
    LayoutError,
    Retry,
    Substantial,
    Workflow,
    seconds,
    step,
    workflow,
)
from substantial.errors import Trap
from substantial.machines import AgentMachine
from substantial.stores import Store
from substantial.stores.fs import FsStore
from substantial.stores.memory import MemoryStore


@workflow
async def example(c: Context) -> int:
    return 1


async def test_an_idle_worker_lists_every_partition_then_shuts_down(store: Store) -> None:
    recording = Recording(store)
    sub = await Substantial._create(recording, partitions=3)
    worker = script(sub, [example])
    async with worker:

        async def listed() -> bool:
            return len(recording.listed) >= 9

        await eventually("listings", listed)
        for p in range(3):
            prefix = f"queues/example/{p:04}/due/"
            assert recording.listed.count(prefix) >= 2, f"{prefix} listed again: {recording.listed}"
    async with asyncio.timeout(5):
        await worker.shutdown()


async def test_a_failing_store_never_stops_the_worker() -> None:
    recording = Recording(MemoryStore("worker-failing"))
    sub = await Substantial._create(recording, partitions=1)
    recording.failing = True
    async with asyncio.timeout(5), script(sub, [example]):
        before = len(recording.listed)

        async def listed() -> bool:
            return len(recording.listed) >= before + 3

        await eventually("listings", listed)


async def test_workers_refuse_invalid_configuration() -> None:
    sub = await Substantial.create("memory://worker-config", partitions=1)
    with pytest.raises(ConfigError):
        script(sub, [example, example])
    with pytest.raises(ConfigError):
        script(sub, [])
    with pytest.raises(ConfigError):
        script(sub, [lambda c: None])  # type: ignore[list-item]
    for settings in [{"concurrency": 0}, {"concurency": 1}, {"max_idle": 1}]:
        with pytest.raises(ConfigError):
            sub.worker([example], preset="script", **settings)
    with pytest.raises(ConfigError):
        sub.worker([example], preset="daemon")
    with pytest.raises(ConfigError, match="needs a preset"):
        sub.worker([example])
    with pytest.raises(ConfigError, match="needs partitions, a positive whole number"):
        await Substantial.create("memory://worker-config")
    with pytest.raises(InvalidArgument):
        Retry(max_attempts="3", initial=seconds(1), max=seconds(2), multiplier=2)  # type: ignore[arg-type]
    with pytest.raises(InvalidArgument):

        @workflow
        def not_async(c: Context) -> None:
            return None

    with pytest.raises(ConfigError, match="a worker serves a list of workflows"):
        script(sub, example)  # type: ignore[arg-type]
    with pytest.raises(ConfigError, match="a store is a URL"):
        await Substantial.create(3, partitions=1)  # type: ignore[arg-type]


async def test_declarations_refuse_bad_arguments_with_invalid_argument() -> None:
    async def body(c: Context) -> None:
        return None

    def work() -> int:
        return 1

    refused = [
        lambda: Workflow(body, name=3),  # type: ignore[arg-type]
        lambda: Workflow(body, name="myFlow"),
        lambda: step(name="a step")(work),
        lambda: step(retry=3)(work),  # type: ignore[call-overload]
        lambda: step(3),  # type: ignore[call-overload]
        lambda: seconds("1"),  # type: ignore[arg-type]
    ]
    for declare in refused:
        with pytest.raises(InvalidArgument):
            declare()
    sub = await Substantial.create("memory://declarations", partitions=1)
    with pytest.raises(InvalidArgument, match="a reason is a string"):
        await sub.run(example, "r").cancel(3)  # type: ignore[arg-type]
    for built in [
        lambda: Credentials(access_key_id=3, secret_access_key="s"),  # type: ignore[arg-type]
        lambda: MemoryStore(3),  # type: ignore[arg-type]
        lambda: FsStore(3),  # type: ignore[arg-type]
    ]:
        with pytest.raises(ConfigError):
            built()


async def test_async_with_a_client_closes_it(monkeypatch: pytest.MonkeyPatch) -> None:
    closed: list[MemoryStore] = []

    async def close(store: MemoryStore) -> None:
        closed.append(store)

    monkeypatch.setattr(MemoryStore, "close", close)
    url = "memory://async-with"
    async with (
        Substantial.create(url, partitions=1) as sub,
        sub.worker([example], preset="script"),
    ):
        assert await (await sub.start(example)).result(timeout=LONG) == 1
    assert len(closed) == 1
    async with await Substantial.open(url) as opened:
        assert opened.partitions == 1
    assert len(closed) == 2


async def test_create_states_what_is_unknown_or_not_built() -> None:
    for url in ["file//tmp/x", "/tmp/x", "ftp://host/x"]:
        with pytest.raises(ConfigError, match=r"^unknown scheme .* memory://, file:, s3://"):
            await Substantial.create(url, partitions=1)
    for url in ["sqlite:x.db", "postgres://u@h/db"]:
        with pytest.raises(ConfigError, match="not available in this host yet"):
            await Substantial.create(url, partitions=1)
    with pytest.raises(ConfigError, match="give an absolute or relative path"):
        await Substantial.create("file:~/x", partitions=1)


async def test_create_and_open_check_the_layout(store: Store) -> None:
    with pytest.raises(LayoutError):
        await Substantial._open(store)
    assert (await Substantial._create(store, partitions=4)).partitions == 4
    assert (await Substantial._create(store, partitions=4)).partitions == 4
    assert (await Substantial._open(store)).partitions == 4
    with pytest.raises(LayoutError):
        await Substantial._create(store, partitions=2)
    for partitions in (0, -1, True, 1.0):
        with pytest.raises(ConfigError):
            await Substantial._create(store, partitions=partitions)  # type: ignore[arg-type]
    with pytest.raises(ConfigError):
        await Substantial.open("s3://bucket/ns", credentials="env")


async def test_a_store_that_accepts_two_creates_fails_the_probe() -> None:
    class Overwriting(MemoryStore):
        async def create(self, key: str, data: bytes) -> str:
            await self.delete(key)
            return await super().create(key, data)

    with pytest.raises(LayoutError):
        await Substantial._create(Overwriting("probe-overwrite"), partitions=1)


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
async def test_serve_shuts_down_on_a_signal(sig: signal.Signals) -> None:
    sub = await Substantial.create(f"memory://serve-{sig.value}", partitions=1)
    worker = script(sub, [example])
    served = asyncio.create_task(worker.serve())
    run = await sub.start(example)
    assert await run.result(timeout=LONG) == 1
    os.kill(os.getpid(), sig)
    async with asyncio.timeout(5):
        await served


async def test_serve_shuts_down_when_cancelled() -> None:
    sub = await Substantial.create("memory://serve-cancelled", partitions=1)
    served = asyncio.create_task(script(sub, [example], shutdown_grace=seconds(1)).serve())
    await asyncio.sleep(0.1)
    served.cancel()
    async with asyncio.timeout(5):
        with pytest.raises(asyncio.CancelledError):
            await served


async def test_workflow_names_are_checked_when_declared() -> None:
    with pytest.raises(InvalidArgument, match="not start with substantial"):

        @workflow(name="substantial.reserved")
        async def reserved(c: Context) -> None:
            return None


async def test_a_worker_on_a_waiting_store_lists_with_waits() -> None:
    from substantial.stores import Capabilities, Listing, ListWait

    waits: list[ListWait] = []

    class Waiting(MemoryStore):
        capabilities = Capabilities(wait=True)

        async def list(self, prefix, *, start_after, max_keys, wait):  # type: ignore[override]
            listing = await super().list(
                prefix, start_after=start_after, max_keys=max_keys, wait=None
            )
            if wait is not None:
                # The worker's own: the client's `result` waits on the run's key too.
                if prefix.startswith("queues/"):
                    waits.append(wait)
                # A stand-in for an h3 server: answers after a short wait whatever changed.
                await asyncio.sleep(0.05)
            return Listing(keys=listing.keys, etag="etag")

    sub = await Substantial._create(Waiting("waiting-store"), partitions=1)
    async with script(sub, [example], wait=seconds(3)):
        assert await (await sub.start(example)).result(timeout=LONG) == 1
        await asyncio.sleep(0.5)
    # The core first lists without waiting to learn an etag, then waits on it.
    assert {(w.seconds, w.etag) for w in waits} == {(0, None), (3, "etag")}


async def test_a_worker_that_dies_logs_at_once_and_fails_the_waits(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    @workflow
    async def waiting(c: Context) -> str:
        return await c.receive("note")

    dying = False
    original = AgentMachine.next

    def next_(self: AgentMachine, now: object) -> object:
        if dying:
            raise Trap("injected")
        return original(self, now)  # type: ignore[arg-type]

    monkeypatch.setattr(AgentMachine, "next", next_)
    sub = await Substantial.create("memory://worker-dies", partitions=1)
    with pytest.raises(Trap, match="injected"):
        async with script(sub, [waiting]) as worker:
            run = await sub.start(waiting)
            pending = asyncio.create_task(run.result(timeout=LONG))
            await asyncio.sleep(0.1)
            assert worker.alive and worker.error is None
            dying = True
            async with asyncio.timeout(2):
                with pytest.raises(Trap, match="injected"):
                    await pending
            # What a readiness probe reads.
            assert not worker.alive and isinstance(worker.error, Trap)
            stopped = [r for r in caplog.records if r.levelno == logging.ERROR]
            assert [r.getMessage() for r in stopped] == [stopped[0].getMessage()]
            assert stopped[0].getMessage().endswith("stopped: injected")
