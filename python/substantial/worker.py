"""A worker: the agent machine of I-23 driven against a store, and the replays it asks for."""

from __future__ import annotations

import asyncio
import logging
import secrets
import signal
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from substantial._proto import agent, boundary, layout, run
from substantial.clock import REAL, Clock, Inbox
from substantial.core import Core
from substantial.cost import Prices
from substantial.durations import millis
from substantial.errors import ConfigError, CoreError, InvalidArgument, ProtocolError
from substantial.machines import AgentMachine
from substantial.observe import Metering, ObserverRaised, Scan, Skewed, rfc3339, time_of
from substantial.replay import Channel, Replay, dropped, replay
from substantial.settings import Settings
from substantial.stores import Store, perform
from substantial.workflow import Workflows

log = logging.getLogger("substantial")

# The name of a replay's task, before its run.
REPLAY = "replay of "
# How many times the core reissues an operation that still failed after the adapter's own
# retries (I-26).
OP_RETRIES = 5


def retention(days: int) -> run.Retention:
    return run.Retention(days=days)


def agent_config(
    worker: str,
    workflows: Workflows,
    s: Settings,
    store_waits: bool,
    prices: Prices | None,
    seed: bytes | None = None,
) -> agent.AgentConfig:
    served = [
        agent.Served(name=n, versions=v, unversioned=u)
        if (r := workflows.retentions[n]) is None
        else agent.Served(name=n, versions=v, unversioned=u, retention=retention(r))
        for n, v, u in workflows.served()
    ]
    return agent.AgentConfig(
        prefix="",
        worker=worker,
        workflows=served,
        concurrency=s.concurrency,
        claim_ttl_ms=millis(s.claim_ttl),
        claim_skew_ms=millis(s.claim_skew),
        max_idle_ms=millis(s.max_idle),
        due_page=s.due_page,
        due_pages=s.due_pages,
        op_retries=OP_RETRIES,
        # § one gpzydmca3.a@djrjm
        seed=secrets.token_bytes(32) if seed is None else seed,
        settings=run.RunSettings(
            commit_every_ms=millis(s.commit_every),
            continue_as_new_after_segments=s.continue_as_new_after.segments,
            continue_as_new_after_bytes=s.continue_as_new_after.bytes,
            dedup_window_ms=millis(s.dedup_window),
            retention=retention(s.retention),
            prices=None if prices is None else prices.proto(),
            max_history_bytes=s.max_history_bytes,
            max_history_events=s.max_history_events,
            max_replay_inbox=s.max_replay_inbox,
            max_seen=s.max_seen,
            history_warn_percent=s.history_warn_percent,
        ),
        store_waits=store_waits,
        wait_seconds=s.wait // timedelta(seconds=1),
        blocked_initial_ms=millis(s.blocked_initial),
        blocked_max_ms=millis(s.blocked_max),
        blocked_multiplier=float(s.blocked_multiplier),
    )


class _Replays:
    """The replays of a worker, each a task of its own talking to the agent loop."""

    def __init__(
        self,
        core: Core,
        store: Store,
        workflows: Workflows,
        clock: Clock,
        events: Inbox[tuple[Any, ...]],
        step_timeout: timedelta,
        metering: Metering,
    ):
        self.replay = Replay(core, store, clock, events.put_nowait, step_timeout, metering)
        self.workflows = workflows
        self.events = events
        # By chain, as the agent names a replay.
        self.running: dict[str, tuple[Channel, asyncio.Task]] = {}

    def spawn(self, chain: str, data: run.RunInput, due_at: layout.Time | None) -> None:
        """A replay of `chain`, whose current run is `data.run`, which logs and observers name."""
        start = data.start
        # § pjrbw31h4@3swh0
        version = start.version if start.HasField("version") else None
        workflow = self.workflows.get(start.workflow, version)
        # § end pjrbw31h4
        if workflow is None:
            log.error(
                "replay of %s: this worker does not serve %s version %s",
                data.run,
                start.workflow,
                version,
            )
            self.events.put_nowait(("ended", chain, dropped()))
            return
        if chain in self.running:
            log.error("a second replay of %s, in replay", data.run)
        channel = Channel()
        task = self.replay.clock.spawn(
            replay(self.replay, chain, workflow, data, due_at, channel), f"{REPLAY}{data.run}"
        )
        self.running[chain] = (channel, task)

    def committed(self, c: agent.AgentCommand.Committed) -> None:
        if c.run not in self.running:
            log.warning("committed for chain %s, which has no replay", c.run)
            return
        self.running[c.run][0].committed.put_nowait(c)

    def claimed(self, chain: str) -> None:
        if chain not in self.running:
            log.warning("claimed for chain %s, which has no replay", chain)
            return
        self.running[chain][0].claimed.put_nowait(chain)

    def stop(self, chain: str) -> None:
        if chain not in self.running:
            log.warning("stop for chain %s, which has no replay", chain)
            return
        self.running[chain][0].stop_replay()

    def stop_all(self) -> None:
        for channel, _ in self.running.values():
            channel.stop_replay()

    def cancel_all(self) -> None:
        for _, task in self.running.values():
            task.cancel()


class Worker:
    """Runs the workflows it is given: `async with` it, or `serve()` it."""

    def __init__(
        self,
        store: Store,
        workflows: Workflows,
        settings: Settings,
        died: Callable[[BaseException], None],
        metering: Metering,
        clock: Clock = REAL,
        id: str | None = None,
        seed: bytes | None = None,
    ):
        settings.validate()
        self._id = secrets.token_hex(8) if id is None else id
        # The core's seed, each replay's derived from it; a fresh one for each worker when `None`.
        self._seed = seed
        self._clock = clock
        self._died = died
        # The prices and observer of its `Substantial`, whose store is observed already.
        self._metering = metering
        self._store = store
        self._workflows = workflows
        self._settings = settings
        self._events: Inbox[tuple[Any, ...]] = Inbox()
        self._task: asyncio.Task | None = None
        self._wake_pending = False

    @property
    def alive(self) -> bool:
        """Whether its loop runs: started, and neither stopped nor dead."""
        # § pgv9r1j12.a@1br9f
        return self._task is not None and not self._task.done()

    @property
    def error(self) -> BaseException | None:
        """The error that killed its loop, else `None`."""
        # § pgv9r1j12.a@kz0n5
        task = self._task
        if task is None or not task.done() or task.cancelled():
            return None
        return task.exception()

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
        prices = self._metering.prices
        config = agent_config(self._id, self._workflows, self._settings, waits, prices, self._seed)
        try:
            machine = AgentMachine(core, config)
        except CoreError as e:
            # The config is the worker's: one the core refuses, such as prices in a currency
            # outside ISO 4217, is a ConfigError where the worker starts.
            if e.kind == "INVALID":
                raise ConfigError(e.message) from None
            raise
        self._task = self._clock.spawn(self._run(core, machine), f"worker {self._id}")
        self._task.add_done_callback(self._stopped)
        return self._task

    def _stopped(self, task: asyncio.Task) -> None:
        """Makes a worker that dies loud at once: its error is logged and fails the waits of
        its namespace, before `shutdown` raises it again."""
        # § pgv9r1j12@evfba
        if task.cancelled() or (error := task.exception()) is None:
            return
        log.error("worker %s stopped: %s", self._id, error, exc_info=error)
        self._died(error)

    def _wake(self) -> None:
        """Wakes the agent at once for a full scan."""
        if not self._wake_pending:
            self._wake_pending = True
            self._events.put_nowait(("wake",))

    def _wake_workflow(self, workflow: str) -> None:
        """Wakes the agent at once on `workflow`, as after a start or a send from this
        process: it lists that workflow's due entries alone."""
        # § c46hgkfse@6979v
        self._events.put_nowait(("wake_workflow", workflow))

    async def shutdown(self) -> None:
        """Starts no new replay and lets the steps in flight end within `shutdown_grace`."""
        if self._task is None:
            raise RuntimeError(f"worker {self._id} was never started")
        self._events.put_nowait(("shutdown",))
        await self._task

    async def drain(self, *, within: timedelta) -> None:
        """Serves every run due now or within `within`, those its own work makes due, and those
        of a crashed worker whose claim expires by then, then stops as `shutdown` does once
        nothing is due by then and nothing is in flight; raises the worker's error if it died.
        Starts the worker if it was not."""
        # § cjtegwkna@9qvvx
        if not isinstance(within, timedelta) or within < timedelta(0):
            raise InvalidArgument(f"within is a timedelta, not negative, not {within!r}")
        task = self._task if self._task is not None else self._start()
        self._events.put_nowait(("drain", self._clock.now().ms + millis(within)))
        # § end cjtegwkna
        # § cjtegwkna.a@4wczd
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await self.shutdown()
            raise
        # § end cjtegwkna.a

    async def serve(self) -> None:
        """Runs until a SIGTERM or a Ctrl-C (SIGINT), or until cancelled, then shuts down."""
        # § dh7y75pvn.a@s5ga7
        task = self._start()
        loop = asyncio.get_running_loop()
        # § one k6m3smdpk@ky9m4
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

    def _skewed(self, skewed: agent.AgentCommand.Skewed) -> None:
        """The agent left a chain alone for a worker whose clock is right."""
        segment_at, now = time_of(skewed.segment_at), time_of(skewed.now)
        # § yp7fv5sa7.c@43b00
        log.error(
            "run %s of chain %s left alone: this worker's clock, %s, is behind its last segment, "
            "%s, by more than claim_skew",
            skewed.current,
            skewed.run,
            rfc3339(now),
            rfc3339(segment_at),
        )
        self._metering.tell(
            Skewed(run=skewed.run, current=skewed.current, segment_at=segment_at, now=now)
        )
        # § end yp7fv5sa7.c

    async def _run(self, core: Core, machine: AgentMachine) -> None:
        clock, events = self._clock, self._events
        replays = _Replays(
            core,
            self._store,
            self._workflows,
            clock,
            events,
            self._settings.step_timeout,
            self._metering,
        )
        ops: set[asyncio.Task] = set()
        grace = 0
        grace_ends: int | None = None

        async def store_op(op_id: int, op: boundary.StoreOp) -> None:
            try:
                events.put_nowait(("feed", op_id, await perform(self._store, op)))
            except ObserverRaised as e:
                events.put_nowait(("died", e.error))

        try:
            # § cfdckf61n.c@66yxa
            retentions = [r for r in self._workflows.retentions.values() if r is not None]
            # Before it serves anything: a run written with a retention no rule expires would
            # never be removed. A `create` or `open` a crash stops halfway leaves its probe keys,
            # `probe/<id>/`, which only their expiry of 1 day removes.
            await self._store.check_expiry([*retentions, self._settings.retention, 1])
            # § end cfdckf61n.c
            while True:
                command = machine.next(clock.now())
                which = command.WhichOneof("command")
                if which == "store":
                    task = clock.spawn(store_op(command.store.id, command.store.op), "store op")
                    ops.add(task)
                    task.add_done_callback(ops.discard)
                elif which == "replay":
                    r = command.replay
                    replays.spawn(r.run, r.input, r.due_at if r.HasField("due_at") else None)
                elif which == "committed":
                    replays.committed(command.committed)
                elif which == "claimed":
                    replays.claimed(command.claimed)
                elif which == "scanned":
                    scanned = command.scanned
                    self._metering.tell(Scan(workflow=scanned.workflow, due=scanned.due))
                elif which == "skewed":
                    self._skewed(command.skewed)
                elif which == "stop_replay":
                    replays.stop(command.stop_replay)
                elif which == "wait_until":
                    at = command.wait_until
                    past_grace = grace_ends is not None and bool(replays.running)
                    if past_grace:
                        left = max(0, grace_ends - clock.elapsed())  # type: ignore[operator]
                        at = layout.Time(ms=min(at.ms, clock.now().ms + left))
                    if events.empty():
                        await clock.wait({events.signal()}, at)
                    if events.empty():
                        if past_grace and clock.elapsed() >= grace_ends:  # type: ignore[operator]
                            # § dh7y75pvn.b@vg22v
                            replays.stop_all()
                            grace_ends = clock.elapsed() + grace
                        continue
                    event = events.get_nowait()
                    kind = event[0]
                    if kind == "feed":
                        machine.feed(event[1], event[2])
                    elif kind == "commit":
                        machine.commit(event[1], event[2])
                    elif kind == "claim":
                        machine.claim(event[1])
                    elif kind == "ended":
                        replays.running.pop(event[1], None)
                        machine.replay_ended(event[1], event[2])
                    elif kind == "wake":
                        self._wake_pending = False
                        machine.wake()
                    elif kind == "wake_workflow":
                        machine.wake_workflow(event[1])
                    elif kind == "died":
                        raise event[1]
                    elif kind == "drain":
                        machine.drain(layout.Time(ms=event[1]))
                    elif kind == "shutdown" and grace_ends is None:
                        # § dh7y75pvn@ajgz3
                        grace = millis(self._settings.shutdown_grace)
                        grace_ends = clock.elapsed() + grace
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
