"""The Python scheduler of I-24: what the code and the replay driver share for one replay. The
code's calls go to the run machine in the order the code makes them; the driver runs the code to
quiescence on its own loop (`executor.py`) before it asks the run machine for `next`."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from substantial._proto import layout, run
from substantial.errors import (
    NEW_VERSION,
    Cancelled,
    CoreError,
    Failed,
    Failure,
    InvalidArgument,
    NonDeterminism,
    ProtocolError,
    ValueTooLarge,
    error_from_failure,
)
from substantial.machines import RunMachine

log = logging.getLogger("substantial")

Clock = Callable[[], layout.Time]
Decode = Callable[[layout.Value | None], Any]
StepBody = Callable[[], Awaitable[Any]]
# An undo, given the value of the step it undoes, as the body of its step.
UndoBody = Callable[[Any], StepBody]
Handler = Callable[[run.Message], None]

# Calls carrying the code's own names or values: `INVALID` for them is the code's argument.
_NAMED = {"step", "receive", "bind", "start", "child_result", "send", "publish", "iterate"}


def decode_json(value: layout.Value | None) -> Any:
    if value is None or value.WhichOneof("kind") is None:
        return None
    if value.WhichOneof("kind") != "json":
        raise ProtocolError(f"values other than inline JSON are not supported yet: {value}")
    return json.loads(value.json)


@dataclass(frozen=True)
class StepSpec:
    """What a step call runs: its attempts, and its undo with the step's options."""

    body: StepBody
    undo: tuple[UndoBody, run.StepOptions] | None = None


class Shared:
    """What the code and the replay driver share for one replay."""

    def __init__(
        self,
        machine: RunMachine,
        clock: Clock,
        data: run.RunInput,
        input_json: bytes,
        loop: asyncio.AbstractEventLoop,
        step_timeout_ms: int,
    ):
        self.machine = machine
        # The timeout of a step without its own: its workflow's, else its worker's.
        self.step_timeout_ms = step_timeout_ms
        self.clock = clock
        self.run = data.run
        self.start = data.start
        self.input = input_json
        self.loop = loop
        self.slots: dict[int, tuple[asyncio.Future, Decode]] = {}
        # The steps pending, with their step key.
        self.steps: dict[int, tuple[StepBody, str]] = {}
        # The undos of every step call of the replay, by call.
        self.undos: dict[int, tuple[UndoBody, run.StepOptions]] = {}
        self.handlers: dict[str, list[Handler]] = {}
        self.waits: list[tuple[int, Callable[[], object]]] = []
        # The children whose chain ended, for `Child.done`.
        self.ended: set[str] = set()
        # Whether the code went past the recorded history, so that its logs are written (I-14).
        self.live = len(data.history) == 0
        # A core error met by a call of the code: the replay cannot go on.
        self.fatal: str | None = None
        # A call the history does not answer: the core recorded the run's failure, so the code
        # stops and every later call fails the same way.
        self.diverged: str | None = None
        # A failure outside the code's reach, a handler's or a predicate's: the run fails with it.
        self.code_failure: Failure | None = None
        self.closed = False

    def protocol(self, message: str) -> ProtocolError:
        self.fatal = self.fatal or message
        return ProtocolError(message)

    def call(self, call: run.Call) -> run.Called:
        """Makes a call of the code. `INVALID` for a call carrying the code's own names is the
        code's argument and goes back to it; any other core error is fatal to the replay."""
        if self.closed:
            raise ProtocolError("a call of the code after its replay ended")
        if self.diverged is not None:
            raise NonDeterminism(self.diverged)
        try:
            called = self.machine.call(call, self.clock())
        except NonDeterminism as e:
            self.diverged = f"{e}; {NEW_VERSION}"
            self.waits = []
            log.warning("run %s: %s", self.run, self.diverged)
            raise NonDeterminism(self.diverged) from None
        except CoreError as e:
            if e.kind == "INVALID" and call.WhichOneof("call") in _NAMED:
                raise InvalidArgument(e.message) from None
            self.fatal = self.fatal or str(e)
            raise
        except ValueTooLarge:
            # The core refused the call and recorded nothing: the code may go on.
            raise
        except Exception as e:
            self.fatal = self.fatal or str(e)
            raise
        self.live = called.live
        return called

    def expect(self, call: run.Call, expected: str) -> Any:
        """Makes a call answered at once with `expected`, and returns its answer."""
        called = self.call(call)
        which = called.WhichOneof("called")
        if which != expected:
            raise self.protocol(f"{call.WhichOneof('call')} answered {which}, not {expected}")
        return getattr(called, expected)

    def future(
        self,
        call: run.Call,
        decode: Decode,
        spec: StepSpec | None = None,
        predicate: Callable[[], object] | None = None,
    ) -> asyncio.Future:
        """Makes a call and returns the future of its value: at once, or by a later `resolve`.
        A pending wait is satisfied once `predicate` holds."""
        called = self.call(call)
        which = called.WhichOneof("called")
        if spec is not None:
            if not called.HasField("occurrence"):
                raise self.protocol(f"step call answered without its occurrence: {called}")
            if which == "pending":
                key = f"{self.run}/{call.step.name}/{called.occurrence}"
                self.steps[called.pending] = (spec.body, key)
                if spec.undo is not None:
                    self.undos[called.pending] = spec.undo
        fut = self.loop.create_future()
        if which == "pending":
            self.slots[called.pending] = (fut, decode)
            if predicate is not None:
                self.waits.append((called.pending, predicate))
        elif which == "value":
            _settle(fut, decode, called.value)
        elif which == "done":
            fut.set_result(None)
        else:
            raise self.protocol(f"unexpected answer {which} to {call.WhichOneof('call')}")
        return fut

    def step_body(self, call: int) -> tuple[StepBody, str] | None:
        return self.steps.get(call)

    def fail_code(self, failure: Failure) -> None:
        self.code_failure = self.code_failure or failure

    def bind(self, names: list[str], handler: Handler) -> None:
        """Binds `handler` to `names`: the messages consumed so far first, then each new one."""
        past = self.expect(run.Call(bind=run.Call.Bind(names=names)), "messages")
        try:
            for m in past.messages:
                handler(m)
        except Failed as e:
            self.fail_code(e.failure)
            raise
        for name in names:
            self.handlers.setdefault(name, []).append(handler)

    def deliver(self, message: run.Message) -> None:
        """Hands a message to the handlers of its name, in the order they were bound."""
        for handler in list(self.handlers.get(message.name, [])):
            try:
                handler(message)
            except Failed as e:
                self.fail_code(e.failure)
                return

    def evaluate_waits(self) -> None:
        """Tells the run machine of every wait whose predicate now holds."""
        pending = []
        for call, predicate in self.waits:
            try:
                held = bool(predicate())
            except Exception as e:
                self.fail_code(Failure(type(e).__name__, f"a wait_until predicate: {e}"))
                return
            if held:
                self.machine.satisfied(call)
            else:
                pending.append((call, predicate))
        self.waits = pending

    def resolve(self, r: run.Next.Resolve) -> None:
        slot = self.slots.pop(r.call, None)
        if slot is None:
            raise ProtocolError(f"resolve of unknown call {r.call}")
        self.steps.pop(r.call, None)
        self.waits = [(call, p) for call, p in self.waits if call != r.call]
        fut, decode = slot
        if fut.done():
            return
        which = r.resolution.WhichOneof("resolution")
        if which == "value":
            _settle(fut, decode, r.resolution.value)
        elif which == "error":
            fut.set_exception(error_from_failure(r.resolution.error))
        elif which == "cancelled":
            fut.set_exception(Cancelled(r.resolution.cancelled))
        else:
            raise ProtocolError(f"empty resolution of call {r.call}")

    def step_ended(self, call: int, attempt: int, outcome: run.Outcome) -> None:
        self.machine.step_ended(call, attempt, outcome, self.clock())

    def arrived(self, messages: Sequence[run.InboxMessage]) -> None:
        self.machine.arrived(messages)

    def next(self) -> run.Next:
        nxt = self.machine.next(self.clock())
        self.live = nxt.live
        return nxt


def _settle(fut: asyncio.Future, decode: Decode, value: layout.Value) -> None:
    try:
        fut.set_result(decode(value))
    except Exception as e:
        fut.set_exception(e)
