"""One replay: a run machine driven by the workflow's code (I-23, I-24)."""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from substantial._proto import agent, boundary, json_value, layout, run
from substantial.clock import Clock, Inbox
from substantial.context import Context
from substantial.core import Core
from substantial.durations import millis
from substantial.errors import (
    Cancelled,
    ContinuedAsNew,
    ProtocolError,
    StepFailed,
    StoreError,
    failure_of,
)
from substantial.executor import Executor
from substantial.machines import RunMachine
from substantial.scheduler import Shared
from substantial.step import CURRENT, attempt
from substantial.stores import Store, perform
from substantial.workflow import Workflow

log = logging.getLogger("substantial")

Emit = Callable[[tuple[Any, ...]], None]


def dropped() -> agent.ReplayEnd:
    return agent.ReplayEnd(dropped=boundary.Empty())


class Channel:
    """What the agent loop tells a replay: the answer to its commit, or to stop."""

    def __init__(self) -> None:
        self.stop: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.committed: Inbox[agent.AgentCommand.Committed] = Inbox()

    def stop_replay(self) -> None:
        if not self.stop.done():
            self.stop.set_result(None)


class Replay:
    """What every replay of a worker shares: its core, store, clock, events and step timeout."""

    def __init__(
        self, core: Core, store: Store, clock: Clock, emit: Emit, step_timeout: timedelta
    ) -> None:
        self.core = core
        self.store = store
        self.clock = clock
        self.emit = emit
        self.step_timeout = step_timeout


async def replay(r: Replay, workflow: Workflow, data: run.RunInput, channel: Channel) -> None:
    """Runs one replay to its end, which it always reports, dropped when it could not go on."""
    emit = r.emit
    try:
        end = await _drive(r, workflow, data, channel)
    except Exception as e:
        log.error("replay of %s dropped: %s", data.run, e)
        end = dropped()
    emit(("ended", data.run, end))


def _input_bytes(start: layout.Start) -> bytes:
    which = start.input.WhichOneof("kind")
    if which is None:
        return b"{}"
    if which == "json":
        return start.input.json
    raise ProtocolError(f"input other than inline JSON is not supported yet: {start.input}")


async def _drive(
    r: Replay, workflow: Workflow, data: run.RunInput, channel: Channel
) -> agent.ReplayEnd:
    if not data.HasField("start"):
        raise ProtocolError(f"replay of {data.run} without start")
    clock, emit = r.clock, r.emit
    input_json = _input_bytes(data.start)
    machine = RunMachine(r.core, data)
    executor = Executor()
    step_timeout = workflow.step_timeout if workflow.step_timeout is not None else r.step_timeout
    shared = Shared(machine, clock, data, input_json, executor.loop, millis(step_timeout))
    attempts: set[asyncio.Task] = set()
    try:
        c = Context(shared)
        context = contextvars.Context()
        context.run(CURRENT.set, c)
        code: asyncio.Task | None = executor.spawn(workflow.run(c, input_json), context)
        while True:
            code = _fail_code(shared, code)
            if code is not None:
                await executor.run()
                if code.done():
                    if shared.fatal is None and shared.diverged is None:
                        _end_run(shared, code)
                    code = None
            if shared.diverged is not None:
                # The core recorded the failure, which `next` commits: the code and its attempts
                # stop where they are.
                if code is not None:
                    code.cancel()
                    code = None
                for task in attempts:
                    task.cancel()
                attempts.clear()
            code = _fail_code(shared, code)
            if shared.fatal is not None:
                raise ProtocolError(f"a call of the code failed: {shared.fatal}")
            if channel.stop.done():
                return dropped()
            if not channel.committed.empty():
                raise ProtocolError("committed without a commit")
            shared.evaluate_waits()
            if shared.code_failure is not None:
                continue
            for task in [t for t in attempts if t.done()]:
                attempts.discard(task)
                shared.step_ended(*task.result())
            nxt = shared.next()
            which = nxt.WhichOneof("next")
            if which == "resolve":
                shared.resolve(nxt.resolve)
            elif which == "deliver":
                shared.deliver(nxt.deliver)
            elif which == "child_ended":
                shared.ended.add(nxt.child_ended)
            elif which == "execute":
                attempts.add(_spawn_attempt(shared, nxt.execute))
            elif which == "commit":
                if not await _commit(shared, data.run, nxt.commit, emit, channel):
                    return dropped()
            elif which == "wait_steps":
                if not attempts:
                    raise ProtocolError("wait_steps with no step in flight")
                timer = nxt.wait_steps.until if nxt.wait_steps.HasField("until") else None
                await clock.wait({*attempts, channel.stop}, timer)
            elif which == "store":
                read = clock.spawn(perform(r.store, nxt.store), f"read of {data.run}")
                await clock.wait({read, channel.stop}, None)
                if channel.stop.done():
                    read.cancel()
                    return dropped()
                result = read.result()
                if result.HasField("failed"):
                    raise StoreError(result.failed.message)
                machine.stored(result)
            elif which == "idle":
                return agent.ReplayEnd(idle=nxt.idle)
            else:
                raise ProtocolError(f"empty next: {nxt}")
    finally:
        for task in attempts:
            task.cancel()
        shared.closed = True
        try:
            await executor.close()
        finally:
            machine.close()


def _fail_code(shared: Shared, code: asyncio.Task | None) -> asyncio.Task | None:
    """Ends the run with a failure the code could not see, a handler's, and drops the code."""
    f, shared.code_failure = shared.code_failure, None
    if f is None or code is None:
        return code
    if shared.fatal is None and shared.diverged is None:
        failure = layout.Failure(type=f.type, message=f.message, retryable=f.retryable)
        shared.call(run.Call(end=run.End(failed=layout.Failed(error=failure))))
    return None


async def _commit(
    shared: Shared, run_id: str, commit: run.Commit, emit: Emit, channel: Channel
) -> bool:
    """Hands a commit to the agent; whether the replay goes on."""
    n = commit.segment.n if commit.HasField("segment") else None
    emit(("commit", run_id, commit))
    await shared.clock.wait({channel.committed.signal(), channel.stop}, None)
    if channel.stop.done():
        return False
    acked = channel.committed.get_nowait()
    if not acked.won:
        return False
    if n is not None and n != acked.n:
        raise ProtocolError(f"commit of segment {n} acknowledged as {acked.n}")
    _log_end(shared.start.chain, commit)
    if acked.inbox:
        shared.arrived(acked.inbox)
    return True


def _log_end(chain: str, commit: run.Commit) -> None:
    """One warning for a run that ended failed or cancelled, from the commit that recorded it."""
    for e in commit.segment.events:
        if e.HasField("failed"):
            log.warning("run %s failed: %s: %s", chain, e.failed.error.type, e.failed.error.message)
        elif e.HasField("cancelled"):
            log.warning("run %s cancelled: %s", chain, e.cancelled.reason)


def _spawn_attempt(shared: Shared, e: run.Next.Execute) -> asyncio.Task:
    found = shared.step_body(e.call)
    if found is None:
        raise ProtocolError(f"execute of call {e.call} without a step")
    body, name, key = found
    timeout_ms = e.timeout_ms if e.HasField("timeout_ms") else None

    async def run_attempt() -> tuple[int, int, run.Outcome]:
        return e.call, e.attempt, await attempt(shared.clock, name, body, key, timeout_ms)

    return shared.clock.spawn(run_attempt(), f"attempt of {key}")


def _end_run(shared: Shared, code: asyncio.Task) -> None:
    """Records the end of the code: its value, its failure, its cancellation or its
    continuation."""
    if code.cancelled():
        end = run.End(failed=layout.Failed(error=layout.Failure(type="CancelledError")))
    elif (e := code.exception()) is None:
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
        end = run.End(continued=continued)
    else:
        f = failure_of(e)
        failure = layout.Failure(type=f.type, message=f.message, retryable=f.retryable)
        if isinstance(e, StepFailed):
            # A client's RunFailed reads the step's cause from it, as a StepFailed's from the core.
            cause = {
                "type": e.cause.type,
                "message": e.cause.message,
                "retryable": e.cause.retryable,
            }
            failure.details.CopyFrom(json_value(json.dumps({"error": cause}).encode()))
        end = run.End(failed=layout.Failed(error=failure))
    shared.call(run.Call(end=end))
