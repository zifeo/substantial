"""One replay: a run machine driven by the workflow's code (I-23, I-24)."""

from __future__ import annotations

import asyncio
import contextvars
import dataclasses
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, NoReturn

from substantial import observe
from substantial._proto import agent, boundary, json_value, layout, run
from substantial.clock import Clock, Inbox
from substantial.context import Context
from substantial.core import Core
from substantial.durations import millis
from substantial.errors import (
    NEW_VERSION,
    BudgetExceeded,
    Cancelled,
    ContinuedAsNew,
    Fail,
    Failed,
    Failure,
    HistoryTooLarge,
    NonDeterminism,
    ProtocolError,
    RolledBack,
    RunEnded,
    RunFailed,
    RunNotFound,
    StepFailed,
    StoreError,
    UndoFailed,
    WaitTimeout,
    failure_of,
)
from substantial.executor import Executor
from substantial.machines import RunMachine
from substantial.observe import Metering, ObserverRaised
from substantial.scheduler import Shared
from substantial.step import CURRENT, attempt
from substantial.stores import Store, perform
from substantial.workflow import Workflow

log = logging.getLogger("substantial")

Emit = Callable[[tuple[Any, ...]], None]


def dropped() -> agent.ReplayEnd:
    return agent.ReplayEnd(dropped=boundary.Empty())


class Channel:
    """What the agent loop tells a replay: the answer to its commit or its claim, or to stop."""

    def __init__(self) -> None:
        self.stop: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.committed: Inbox[agent.AgentCommand.Committed] = Inbox()
        self.claimed: Inbox[str] = Inbox()

    def stop_replay(self) -> None:
        if not self.stop.done():
            self.stop.set_result(None)


class Replay:
    """What every replay of a worker shares: its core, store, clock, events, step timeout, and
    the prices and observer of its `Substantial`."""

    def __init__(
        self,
        core: Core,
        store: Store,
        clock: Clock,
        emit: Emit,
        step_timeout: timedelta,
        metering: Metering,
    ) -> None:
        self.core = core
        self.store = store
        self.clock = clock
        self.emit = emit
        self.step_timeout = step_timeout
        self.metering = metering

    def tell(self, event: observe.Observed) -> None:
        """Tells the observer; one that raises stops the worker with its error."""
        # § yp7fv5sa7.b@2tszk
        try:
            self.metering.tell(event)
        except Exception as e:
            self.emit(("died", e))


@dataclass
class _Committed:
    """The kind of the last commit of the replay that won."""

    kind: int | None = None


async def replay(
    r: Replay,
    chain: str,
    workflow: Workflow,
    data: run.RunInput,
    due_at: layout.Time | None,
    channel: Channel,
) -> None:
    """Runs one replay of `chain` to its end, which it always reports, dropped when it could not
    go on; `due_at` is the earliest due entry it serves. Its requests to the agent name the chain,
    its logs the run."""
    emit = r.emit
    started, now = r.clock.seconds(), r.clock.now()
    committed = _Committed()
    try:
        end = await _drive(r, chain, workflow, data, channel, committed)
    except Exception as e:
        log.error("replay of %s dropped: %s", data.run, e)
        end = dropped()
    if end.HasField("dropped"):
        ended = "dropped"
    elif end.HasField("blocked"):
        ended = "blocked"
    elif committed.kind == run.COMMIT_KIND_SUSPEND:
        ended = "suspended"
    elif committed.kind == run.COMMIT_KIND_END:
        ended = "ended"
    else:
        ended = "idle"
    delay = None if due_at is None else (now.ms - due_at.ms) / 1000
    seconds = r.clock.seconds() - started
    r.tell(observe.Replay(workflow=workflow.name, delay=delay, seconds=seconds, end=ended))
    emit(("ended", chain, end))


def _input_bytes(start: layout.Start) -> bytes:
    which = start.input.WhichOneof("kind")
    if which is None:
        # § e1dc4dar4.a@5jdxd
        return b"{}"
    if which == "json":
        return start.input.json
    raise ProtocolError(f"input other than inline JSON is not supported yet: {start.input}")


async def _drive(
    r: Replay,
    chain: str,
    workflow: Workflow,
    data: run.RunInput,
    channel: Channel,
    committed: _Committed,
) -> agent.ReplayEnd:
    if not data.HasField("start"):
        raise ProtocolError(f"replay of {data.run} without start")
    clock = r.clock
    input_json = _input_bytes(data.start)
    machine = RunMachine(r.core, data)
    executor = Executor()
    # § aym52h8ch@fbewq
    step_timeout = workflow.step_timeout if workflow.step_timeout is not None else r.step_timeout
    # § end aym52h8ch
    heartbeat = workflow.heartbeat_timeout
    shared = Shared(
        machine,
        clock,
        data,
        input_json,
        executor.loop,
        millis(step_timeout),
        None if heartbeat is None else millis(heartbeat),
    )
    attempts: set[asyncio.Task] = set()
    try:
        c = Context(shared)
        context = contextvars.Context()
        context.run(CURRENT.set, c)
        # § g4kv2kj51@fpwv0
        code: asyncio.Task | None = executor.spawn(workflow.run(c, input_json), context)
        # § end g4kv2kj51
        # An error of the code that is no end: the replay is blocked with it.
        blocked: Failure | None = None
        while True:
            # § k3zma9hjs@zps0d
            blocked = _fail_code(shared, blocked)
            if code is not None and blocked is None:
                await executor.run()
                if code.done():
                    if (
                        shared.fatal is None
                        and shared.diverged is None
                        and not shared.ended_by_core
                        and shared.code_failure is None
                    ):
                        blocked = _end_run(shared, code)
                    code = None
            if shared.diverged is not None or shared.ended_by_core or blocked is not None:
                # Blocked, or ended by the core: the code and its attempts stop where they are.
                if code is not None:
                    code.cancel()
                    code = None
                # § v4n6ja42e.a@31jat
                for task in attempts:
                    task.cancel()
                attempts.clear()
                # § end v4n6ja42e.a
            blocked = _fail_code(shared, blocked)
            if shared.fatal is not None:
                raise ProtocolError(f"a call of the code failed: {shared.fatal}")
            if blocked is not None:
                if clock.checks_only:
                    await _checked(clock, data.run, str(blocked))
                return _block(r, workflow, shared, blocked)
            if channel.stop.done():
                return dropped()
            if not channel.committed.empty():
                raise ProtocolError("committed without a commit")
            # § k3zma9hjs.a@m26jp
            shared.evaluate_waits()
            if shared.code_failure is not None:
                continue
            # § end k3zma9hjs.a
            for task in [t for t in attempts if t.done()]:
                attempts.discard(task)
                shared.step_ended(*task.result())
            # § awhhtbr5n.b@12zfq
            try:
                nxt = shared.next()
            except NonDeterminism as e:
                if not clock.checks_only:
                    raise
                await _checked(clock, data.run, str(e))
            which = nxt.WhichOneof("next")
            if clock.checks_only:
                diverged = shared.diverged
                if diverged is None and which == "blocked":
                    diverged = nxt.blocked.message
                if diverged is not None or which not in ("resolve", "deliver", "child_ended"):
                    await _checked(clock, data.run, diverged)
            # § end awhhtbr5n.b
            if which == "resolve":
                shared.resolve(nxt.resolve)
            elif which == "deliver":
                shared.deliver(nxt.deliver)
            elif which == "child_ended":
                shared.ended.add(nxt.child_ended)
            elif which == "execute":
                attempts.add(_spawn_attempt(r, workflow, shared, nxt.execute))
            elif which == "commit":
                if not await _commit(r, chain, workflow, shared, nxt.commit, channel):
                    return dropped()
                committed.kind = nxt.commit.kind
            elif which == "blocked":
                return _block(r, workflow, shared, Failure.from_proto(nxt.blocked))
            elif which == "claim":
                r.emit(("claim", chain))
                await clock.wait({channel.claimed.signal(), channel.stop}, None)
                if channel.stop.done():
                    return dropped()
                channel.claimed.get_nowait()
                machine.claimed()
            elif which == "wait_steps":
                if not attempts:
                    raise ProtocolError("wait_steps with no step in flight")
                timer = nxt.wait_steps.until if nxt.wait_steps.HasField("until") else None
                await clock.wait({*attempts, channel.stop}, timer)
            elif which == "store":
                read = clock.spawn(_read(r, nxt.store), f"read of {data.run}")
                await clock.wait({read, channel.stop}, None)
                if channel.stop.done():
                    read.cancel()
                    return dropped()
                result = read.result()
                if result is None:
                    return dropped()
                if result.HasField("failed"):
                    raise StoreError(result.failed.message)
                machine.stored(result)
            elif which == "idle":
                return agent.ReplayEnd(idle=nxt.idle)
            else:
                raise ProtocolError(f"empty next: {nxt}")
    finally:
        # § one v4n6ja42e.a@acq7a
        for task in attempts:
            task.cancel()
        shared.closed = True
        # § d9818h4m4.a@mvwpd
        try:
            await executor.close()
        finally:
            machine.close()
        # § end d9818h4m4.a


async def _checked(clock: Clock, run_id: str, diverged: str | None) -> NoReturn:
    """Tells the replay tester how the replay of `run_id` checked, then parks for good, so nothing
    past the history runs or is written: the tester crashes the worker once it read the
    outcome."""
    clock.checked(run_id, diverged)
    while True:
        await clock.wait(set(), None)


async def _read(r: Replay, op: boundary.StoreOp) -> boundary.OpResult | None:
    """An operation of a workflow's send or `c.result`; `None` when the observer raised, which
    stops the worker."""
    try:
        return await perform(r.store, op)
    except ObserverRaised as e:
        r.emit(("died", e.error))
        return None


def _fail_code(shared: Shared, blocked: Failure | None) -> Failure | None:
    """The failure the replay is blocked with: its own, else one the code could not see, a
    handler's or a predicate's."""
    f, shared.code_failure = shared.code_failure, None
    if blocked is not None or shared.fatal is not None or shared.diverged is not None:
        return blocked
    if shared.ended_by_core:
        return None
    return f


def _block(r: Replay, workflow: Workflow, shared: Shared, f: Failure) -> agent.ReplayEnd:
    """Reports the replay blocked with `f`, its code and attempts stopped: nothing more is
    recorded, and the run is tried again after a backoff."""
    # § dxgjekysz.e@w33wm
    if f.type == "NonDeterminism" and not f.message.endswith(NEW_VERSION):
        f = dataclasses.replace(f, message=f"{f.message}; {NEW_VERSION}")
    # § yp7fv5sa7.c@1c3b1
    log.warning("run %s blocked: %s", shared.run, f)
    r.tell(observe.Blocked(workflow=workflow.name, run=shared.run, error=f))
    # § end yp7fv5sa7.c
    return agent.ReplayEnd(blocked=layout.Failure(type=f.type, message=f.message, retryable=False))


async def _commit(
    r: Replay, chain: str, workflow: Workflow, shared: Shared, commit: run.Commit, channel: Channel
) -> bool:
    """Hands a commit to the agent; whether the replay goes on."""
    n = commit.segment.n if commit.HasField("segment") else None
    r.emit(("commit", chain, commit))
    await shared.clock.wait({channel.committed.signal(), channel.stop}, None)
    if channel.stop.done():
        return False
    acked = channel.committed.get_nowait()
    if not acked.won:
        return False
    if n is not None and n != acked.n:
        raise ProtocolError(f"commit of segment {n} acknowledged as {acked.n}")
    _log_end(shared.run, commit)
    for near in commit.near_limits:
        # § yp7fv5sa7.c@ykzxk
        log.warning(
            "run %s near its %s limit: %d of %d",
            shared.run,
            near.limit,
            near.used,
            near.max,
        )
        told = observe.NearLimit(workflow.name, shared.run, near.limit, near.used, near.max)
        r.tell(told)
        # § end yp7fv5sa7.c
    if acked.HasField("cost"):
        shared.arrived(acked.inbox, acked.cost)
    elif acked.inbox:
        raise ProtocolError("messages arrived without the chain's cost")
    return True


def _log_end(run_id: str, commit: run.Commit) -> None:
    """One warning for a run that ended failed or cancelled, from the commit that recorded it."""
    for e in commit.segment.events:
        if e.HasField("failed"):
            error = e.failed.error
            log.warning("run %s failed: %s: %s", run_id, error.type, error.message)
        elif e.HasField("cancelled"):
            log.warning("run %s cancelled: %s", run_id, e.cancelled.reason)


def _attempt_outcome(outcome: run.Outcome, attempt: int, max_attempts: int) -> str:
    """How an attempt ended, as its observer is told."""
    if outcome.HasField("value"):
        return "ok"
    if outcome.error.type == "StepTimeout":
        return "timeout"
    if outcome.error.retryable and attempt < max_attempts:
        return "retry"
    return "failed"


def _spawn_attempt(
    r: Replay, workflow: Workflow, shared: Shared, e: run.Next.Execute
) -> asyncio.Task:
    step = shared.step_body(e.call)
    if step is None:
        raise ProtocolError(f"execute of call {e.call} without a step")
    timeout_ms = e.timeout_ms if e.HasField("timeout_ms") else None

    async def run_attempt() -> tuple[int, int, run.Outcome, list[layout.Amount]]:
        # § yp7fv5sa7.a@8k74j
        started = shared.clock.seconds()
        # Unless it reaches its outcome: cancelled by a shutdown, a lost claim or a crash.
        ended = "abandoned"
        try:
            outcome, charges = await attempt(
                shared.clock, step.name, step.body, step.key, timeout_ms, step.heartbeat_ms
            )
            ended = _attempt_outcome(outcome, e.attempt, step.max_attempts)
            return e.call, e.attempt, outcome, charges
        finally:
            seconds = shared.clock.seconds() - started
            told = observe.Attempt(workflow.name, step.name, e.attempt, seconds, ended)
            r.tell(told)

    return shared.clock.spawn(run_attempt(), f"attempt of {step.key}")


# The errors that end a run failed when the code lets them out: the recorded outcomes of a verb,
# the same at every replay, and the failure the code raises on purpose.
_ENDS = (
    Fail,
    StepFailed,
    BudgetExceeded,
    RolledBack,
    UndoFailed,
    HistoryTooLarge,
    RunFailed,
    WaitTimeout,
    RunNotFound,
    RunEnded,
    Failed,
)


def _ends(e: BaseException) -> bool:
    """Whether an error the code lets out ends its run failed, rather than blocking it."""
    # § dxgjekysz.d@b3zg3
    return isinstance(e, _ENDS)


def _end_run(shared: Shared, code: asyncio.Task) -> Failure | None:
    """Records the end of the code, its value, its failure, its cancellation or its
    continuation; or returns the failure that blocks the run, recording nothing."""
    # § dxgjekysz@1989s
    if code.cancelled():
        # § dxgjekysz.b@4r348
        return Failure("CancelledError", "")
        # § end dxgjekysz.b
    if (e := code.exception()) is None:
        end = run.End(completed=layout.Completed(value=json_value(code.result())))
    elif isinstance(e, Cancelled):
        end = run.End(cancelled=layout.Cancelled(reason=e.reason))
    elif isinstance(e, ContinuedAsNew):
        continued = run.End.Continued(input=json_value(e.input))
        if e.workflow is not None:
            continued.workflow = e.workflow
            if e.version is not None:
                continued.version = e.version
        if e.state is not None:
            continued.state.CopyFrom(json_value(e.state))
        continued.highest = e.highest
        end = run.End(continued=continued)
    elif not _ends(e):
        return failure_of(e)
    else:
        f = failure_of(e)
        failure = layout.Failure(type=f.type, message=f.message, retryable=f.retryable)
        # § one r3sd261h4.b@v7vvf
        if isinstance(e, StepFailed) and e._details is not None:
            failure.details.CopyFrom(e._details)
        elif isinstance(e, StepFailed):
            # A client's RunFailed reads the step's cause from it, as a StepFailed's from the core.
            cause = {
                "type": e.cause.type,
                "message": e.cause.message,
                "retryable": e.cause.retryable,
            }
            failure.details.CopyFrom(json_value(json.dumps({"error": cause}).encode()))
        end = run.End(failed=layout.Failed(error=failure))
    shared.call(run.Call(end=end))
    return None
