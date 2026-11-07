"""Typed wrappers over the machines of the core: run, agent and plan (I-23)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from google.protobuf.message import DecodeError, Message

from substantial._proto import agent, boundary, layout, plan, run
from substantial.core import Core, Kind
from substantial.errors import (
    CoreError,
    LayoutError,
    NonDeterminism,
    ProtocolError,
    ValueTooLarge,
)


def core_error(e: boundary.CoreError) -> Exception:
    if e.kind == boundary.ERROR_KIND_NON_DETERMINISM:
        return NonDeterminism(e.message)
    if e.kind == boundary.ERROR_KIND_LAYOUT:
        return LayoutError(e.message)
    if e.kind == boundary.ERROR_KIND_VALUE_TOO_LARGE:
        return ValueTooLarge(e.message)
    kind = boundary.ErrorKind.Name(e.kind).removeprefix("ERROR_KIND_")
    return CoreError(kind, e.message)


class _Machine:
    """A machine of the core, dropped with `sub_drop` by `close`."""

    def __init__(self, core: Core, kind: Kind, data: Message) -> None:
        self._core = core
        self._id: int | None = core.new_machine(kind, data.SerializeToString())

    def close(self) -> None:
        if self._id is not None:
            machine, self._id = self._id, None
            self._core.drop_machine(machine)

    def _request(self, request: Message, response: type[Message]) -> tuple[str, Message]:
        if self._id is None:
            raise ProtocolError("a call on a dropped machine")
        out = response()
        try:
            out.ParseFromString(self._core.call(self._id, request.SerializeToString()))
        except DecodeError as e:
            raise ProtocolError(f"undecodable response: {e}") from e
        which = out.WhichOneof("response")
        if which is None:
            raise ProtocolError(f"empty {response.__name__}")
        value = getattr(out, which)
        if which == "error":
            raise core_error(value)
        return which, value

    def _expect(self, request: Message, response: type[Message], expected: str) -> Any:
        which, value = self._request(request, response)
        if which != expected:
            raise ProtocolError(f"expected {expected}, got {which}: {value}")
        return value


class RunMachine(_Machine):
    """The run machine of one replay."""

    def __init__(self, core: Core, data: run.RunInput) -> None:
        super().__init__(core, Kind.RUN, data)

    def call(self, call: run.Call, now: layout.Time) -> run.Called:
        request = run.RunRequest(call=run.RunRequest.MakeCall(call=call, now=now))
        return self._expect(request, run.RunResponse, "called")

    def step_ended(self, call: int, attempt: int, outcome: run.Outcome, now: layout.Time) -> None:
        ended = run.RunRequest.StepEnded(call=call, attempt=attempt, outcome=outcome, now=now)
        self._expect(run.RunRequest(step_ended=ended), run.RunResponse, "done")

    def satisfied(self, call: int) -> None:
        self._expect(run.RunRequest(satisfied=call), run.RunResponse, "done")

    def arrived(self, messages: Sequence[run.InboxMessage]) -> None:
        arrived = run.RunRequest.Arrived(messages=messages)
        self._expect(run.RunRequest(arrived=arrived), run.RunResponse, "done")

    def next(self, now: layout.Time) -> run.Next:
        return self._expect(run.RunRequest(next=now), run.RunResponse, "next")

    def stored(self, result: boundary.OpResult) -> None:
        self._expect(run.RunRequest(stored=result), run.RunResponse, "done")


class AgentMachine(_Machine):
    """The agent machine of one worker."""

    def __init__(self, core: Core, config: agent.AgentConfig) -> None:
        super().__init__(core, Kind.AGENT, config)

    def _done(self, request: agent.AgentRequest) -> None:
        self._expect(request, agent.AgentResponse, "done")

    def next(self, now: layout.Time) -> agent.AgentCommand:
        request = agent.AgentRequest(next=now)
        return self._expect(request, agent.AgentResponse, "command")

    def feed(self, op: int, result: boundary.OpResult) -> None:
        self._done(agent.AgentRequest(feed=agent.AgentRequest.Feed(id=op, result=result)))

    def commit(self, run_id: str, commit: run.Commit) -> None:
        self._done(
            agent.AgentRequest(commit=agent.AgentRequest.RunCommit(run=run_id, commit=commit))
        )

    def replay_ended(self, run_id: str, end: agent.ReplayEnd) -> None:
        ended = agent.AgentRequest.ReplayEnded(run=run_id, end=end)
        self._done(agent.AgentRequest(replay_ended=ended))

    def wake(self) -> None:
        self._done(agent.AgentRequest(wake=boundary.Empty()))

    def shutdown(self) -> None:
        self._done(agent.AgentRequest(shutdown=boundary.Empty()))


class PlanMachine(_Machine):
    """The plan machine of one client action."""

    def __init__(self, core: Core, data: plan.PlanInput) -> None:
        super().__init__(core, Kind.PLAN, data)

    def next(self) -> plan.PlanStep:
        request = plan.PlanRequest(next=boundary.Empty())
        return self._expect(request, plan.PlanResponse, "step")

    def feed(self, result: boundary.OpResult) -> None:
        self._expect(plan.PlanRequest(feed=result), plan.PlanResponse, "done")
