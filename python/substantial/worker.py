"""A worker: the agent machine of I-23 driven against a store, and the replays it asks for."""

from __future__ import annotations

import asyncio
import logging
import secrets
import signal
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from substantial._proto import agent, boundary, run
from substantial.core import Core
from substantial.durations import millis, now, until
from substantial.errors import ProtocolError
from substantial.machines import AgentMachine
from substantial.replay import Channel, Replay, dropped, replay
from substantial.scheduler import Clock
from substantial.settings import Settings
from substantial.stores import Store, perform
from substantial.workflow import Workflows

log = logging.getLogger("substantial")

# How many times the core reissues an operation that still failed after the adapter's own
# retries (I-26).
OP_RETRIES = 5


def agent_config(
    worker: str, workflows: Workflows, partitions: int, s: Settings, store_waits: bool
) -> agent.AgentConfig:
    served = [agent.Served(name=n, versions=v, unversioned=u) for n, v, u in workflows.served()]
    return agent.AgentConfig(
        prefix="",
        partitions=partitions,
        worker=worker,
        workflows=served,
        concurrency=s.concurrency,
        claim_ttl_ms=millis(s.claim_ttl),
        claim_skew_ms=millis(s.claim_skew),
        max_idle_ms=millis(s.max_idle),
        due_page=s.due_page,
        due_pages=s.due_pages,
        snapshot_every=s.snapshot_every,
        op_retries=OP_RETRIES,
        seed=secrets.token_bytes(32),
        settings=run.RunSettings(
            commit_every_ms=millis(s.commit_every),
            continue_as_new_after_segments=s.continue_as_new_after.segments,
            continue_as_new_after_bytes=s.continue_as_new_after.bytes,
            dedup_window_ms=millis(s.dedup_window),
        ),
        store_waits=store_waits,
        wait_seconds=s.wait // timedelta(seconds=1),
    )


class _Replays:
    """The replays of a worker, each a task of its own talking to the agent loop."""

    def __init__(
        self,
        core: Core,
        store: Store,
        workflows: Workflows,
        clock: Clock,
        events: asyncio.Queue,
        step_timeout: timedelta,
    ):
        self.replay = Replay(core, store, clock, events.put_nowait, step_timeout)
        self.workflows = workflows
        self.events = events
        self.running: dict[str, tuple[Channel, asyncio.Task]] = {}

    def spawn(self, run_id: str, data: run.RunInput) -> None:
        start = data.start
        version = start.version if start.HasField("version") else None
        workflow = self.workflows.get(start.workflow, version)
        if workflow is None:
            log.error(
                "replay of %s: this worker does not serve %s version %s",
                run_id,
                start.workflow,
                version,
            )
            self.events.put_nowait(("ended", run_id, dropped()))
            return
        if run_id in self.running:
            log.error("a second replay of %s, in replay", run_id)
        channel = Channel()
        task = asyncio.create_task(replay(self.replay, workflow, data, channel))
        self.running[run_id] = (channel, task)

    def committed(self, c: agent.AgentCommand.Committed) -> None:
        run_id = c.run
        if run_id not in self.running:
            log.warning("committed for %s, which has no replay", run_id)
            return
        self.running[run_id][0].committed.put_nowait(c)

    def stop(self, run_id: str) -> None:
        if run_id not in self.running:
            log.warning("stop for %s, which has no replay", run_id)
            return
        self.running[run_id][0].stop.set()

    def stop_all(self) -> None:
        for channel, _ in self.running.values():
            channel.stop.set()

    def cancel_all(self) -> None:
        for _, task in self.running.values():
            task.cancel()


class Worker:
    """Runs the workflows it is given, on every partition: `async with` it, or `serve()` it."""

    def __init__(
        self,
        store: Store,
        workflows: Workflows,
        partitions: int,
        settings: Settings,
        died: Callable[[BaseException], None],
    ):
        settings.validate()
        self._id = secrets.token_hex(8)
        self._died = died
        self._store = store
        self._workflows = workflows
        self._partitions = partitions
        self._settings = settings
        self._events: asyncio.Queue[tuple[Any, ...]] = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._wake_pending = False

    async def __aenter__(self) -> Worker:
        self._start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.shutdown()

    def _start(self) -> asyncio.Task:
        if self._task is not None:
            raise RuntimeError(f"worker {self._id} was started already")
        core = Core.new()
        waits = self._store.capabilities.wait
        config = agent_config(self._id, self._workflows, self._partitions, self._settings, waits)
        machine = AgentMachine(core, config)
        self._task = asyncio.create_task(self._run(core, machine))
        self._task.add_done_callback(self._stopped)
        return self._task

    def _stopped(self, task: asyncio.Task) -> None:
        """Makes a worker that dies loud at once: its error is logged and fails the waits of
        its namespace, before `shutdown` raises it again."""
        if task.cancelled() or (error := task.exception()) is None:
            return
        log.error("worker %s stopped: %s", self._id, error, exc_info=error)
        self._died(error)

    def _wake(self) -> None:
        """Wakes the agent at once, as after a message sent from this process."""
        if not self._wake_pending:
            self._wake_pending = True
            self._events.put_nowait(("wake",))

    async def shutdown(self) -> None:
        """Starts no new replay and lets the steps in flight end within `shutdown_grace`."""
        if self._task is None:
            raise RuntimeError(f"worker {self._id} was never started")
        self._events.put_nowait(("shutdown",))
        await self._task

    async def serve(self) -> None:
        """Runs until a SIGTERM or a Ctrl-C (SIGINT), or until cancelled, then shuts down."""
        task = self._start()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._events.put_nowait, ("shutdown",))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await self.shutdown()
            raise
        finally:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(sig)

    async def _run(self, core: Core, machine: AgentMachine) -> None:
        replays = _Replays(
            core, self._store, self._workflows, now, self._events, self._settings.step_timeout
        )
        ops: set[asyncio.Task] = set()
        grace = self._settings.shutdown_grace / timedelta(seconds=1)
        grace_ends: float | None = None
        loop = asyncio.get_running_loop()

        async def store_op(op_id: int, op: boundary.StoreOp) -> None:
            self._events.put_nowait(("feed", op_id, await perform(self._store, op)))

        try:
            while True:
                command = machine.next(now())
                which = command.WhichOneof("command")
                if which == "store":
                    task = asyncio.create_task(store_op(command.store.id, command.store.op))
                    ops.add(task)
                    task.add_done_callback(ops.discard)
                elif which == "replay":
                    replays.spawn(command.replay.run, command.replay.input)
                elif which == "committed":
                    replays.committed(command.committed)
                elif which == "stop_replay":
                    replays.stop(command.stop_replay)
                elif which == "wait_until":
                    timeout = until(command.wait_until, now())
                    past_grace = grace_ends is not None and bool(replays.running)
                    if past_grace:
                        timeout = min(timeout, max(0.0, grace_ends - loop.time()))
                    try:
                        event = self._events.get_nowait()
                    except asyncio.QueueEmpty:
                        try:
                            event = await asyncio.wait_for(self._events.get(), timeout)
                        except TimeoutError:
                            if past_grace and loop.time() >= grace_ends:  # type: ignore[operator]
                                replays.stop_all()
                                grace_ends = loop.time() + grace
                            continue
                    kind = event[0]
                    if kind == "feed":
                        machine.feed(event[1], event[2])
                    elif kind == "commit":
                        machine.commit(event[1], event[2])
                    elif kind == "ended":
                        replays.running.pop(event[1], None)
                        machine.replay_ended(event[1], event[2])
                    elif kind == "wake":
                        self._wake_pending = False
                        machine.wake()
                    elif kind == "shutdown" and grace_ends is None:
                        grace_ends = loop.time() + grace
                        machine.shutdown()
                elif which == "stop":
                    return
                else:
                    raise ProtocolError(f"empty agent command: {command}")
        finally:
            for task in ops:
                task.cancel()
            replays.cancel_all()
            machine.close()
