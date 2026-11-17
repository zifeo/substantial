"""Opening namespaces and running workers on an idle namespace."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from datetime import timedelta

import pytest
from conftest import LONG, Recording, eventually, script

from substantial import (
    ConfigError,
    Context,
    Credentials,
    InvalidArgument,
    LayoutError,
    Retry,
    StoreOp,
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


@workflow
async def other(c: Context) -> int:
    return 2


async def test_an_idle_worker_lists_every_workflow_then_shuts_down(store: Store) -> None:
    recording = Recording(store)
    sub = await Substantial._create(recording)
    worker = script(sub, [example, other])
    async with worker:

        async def listed() -> bool:
            return len(recording.listed) >= 4

        await eventually("listings", listed)
        for name in ("example", "other"):
            prefix = f"queues/{name}/due/"
            assert recording.listed.count(prefix) >= 2, f"{prefix} listed again: {recording.listed}"
    async with asyncio.timeout(5):
        await worker.shutdown()


async def test_a_failing_store_never_stops_the_worker() -> None:
    recording = Recording(MemoryStore("worker-failing"))
    sub = await Substantial._create(recording)
    recording.failing = True
    async with asyncio.timeout(5), script(sub, [example]):
        before = len(recording.listed)

        async def listed() -> bool:
            return len(recording.listed) >= before + 3

        await eventually("listings", listed)


async def test_workers_refuse_invalid_configuration() -> None:
    sub = await Substantial.create("memory://worker-config")
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
    with pytest.raises(InvalidArgument):
        Retry(max_attempts="3", initial=seconds(1), max=seconds(2), multiplier=2)  # type: ignore[arg-type]
    with pytest.raises(InvalidArgument):

        @workflow
        def not_async(c: Context) -> None:
            return None

    with pytest.raises(ConfigError, match="a worker serves a list of workflows"):
        script(sub, example)  # type: ignore[arg-type]
    with pytest.raises(ConfigError, match="a store is a URL"):
        await Substantial.create(3)  # type: ignore[arg-type]


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
    sub = await Substantial.create("memory://declarations")
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
        Substantial.create(url) as sub,
        sub.worker([example], preset="script"),
    ):
        assert await (await sub.start(example)).result(timeout=LONG) == 1
    assert len(closed) == 1
    async with await Substantial.open(url):
        pass
    assert len(closed) == 2


async def test_create_states_what_is_unknown_or_not_built() -> None:
    for url in ["file//tmp/x", "/tmp/x", "ftp://host/x"]:
        with pytest.raises(ConfigError, match=r"^unknown scheme .* memory://, file:, s3://"):
            await Substantial.create(url)
    for url in ["sqlite:x.db", "postgres://u@h/db"]:
        with pytest.raises(ConfigError, match="not available in this host yet"):
            await Substantial.create(url)
    with pytest.raises(ConfigError, match="give an absolute or relative path"):
        await Substantial.create("file:~/x")


async def test_create_and_open_check_the_layout(store: Store) -> None:
    with pytest.raises(LayoutError):
        await Substantial._open(store)
    await Substantial._create(store)
    await Substantial._create(store)
    await Substantial._open(store)
    assert await store.get("substantial.json") == b'{"layout":2,"encoding":"protobuf"}'
    # A namespace of the layout before, with its partitions: drained by the version that wrote
    # it, never opened by this one.
    old = MemoryStore("worker-layout-1")
    await old.create("probe/exclusive", b"")
    marker = b'{"layout":1,"encoding":"protobuf","partitions":4,"hash":"xxh3-64"}'
    await old.create("substantial.json", marker)
    for opened in (Substantial._open(old), Substantial._create(old)):
        with pytest.raises(LayoutError, match="has layout 1"):
            await opened
    with pytest.raises(ConfigError):
        await Substantial.open("s3://bucket/ns", credentials="env")


async def test_a_store_that_accepts_two_creates_fails_the_probe() -> None:
    class Overwriting(MemoryStore):
        async def create(self, key: str, data: bytes) -> str:
            await self.delete(key)
            return await super().create(key, data)

    with pytest.raises(LayoutError):
        await Substantial._create(Overwriting("probe-overwrite"))


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
async def test_serve_shuts_down_on_a_signal(sig: signal.Signals) -> None:
    sub = await Substantial.create(f"memory://serve-{sig.value}")
    worker = script(sub, [example])
    served = asyncio.create_task(worker.serve())
    run = await sub.start(example)
    assert await run.result(timeout=LONG) == 1
    os.kill(os.getpid(), sig)
    async with asyncio.timeout(5):
        await served


async def test_serve_shuts_down_when_cancelled() -> None:
    sub = await Substantial.create("memory://serve-cancelled")
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

    sub = await Substantial._create(Waiting("waiting-store"))
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
    sub = await Substantial.create("memory://worker-dies")
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


@workflow
async def a_second(c: Context) -> int:
    await c.sleep(seconds(1))
    return 1


async def test_result_polls_up_to_max_interval() -> None:
    gets: list[object] = []

    def observe(event: object) -> None:
        if isinstance(event, StoreOp) and event.op == "get":
            gets.append(event)

    url = "memory://max-interval"
    async with (
        Substantial.create(url) as sub,
        Substantial.open(url, observe=observe) as client,
        script(sub, [a_second]),
    ):
        run = await client.start(a_second)
        assert await run.result(max_interval=timedelta(milliseconds=20)) == 1
        # Every 20 ms for a second, where doubling from 10 ms up to 30 s polls 8 times.
        assert len(gets) > 20, len(gets)
        with pytest.raises(InvalidArgument, match="max_interval"):
            await run.result(max_interval=timedelta(0))
