"""The errors of I-20, and the host's own failures around the core."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from substantial.layout.v1 import layout_pb2


@dataclass(frozen=True)
class Failure:
    """A failure as recorded in a history: a type name, a message, whether a retry may help, and
    the JSON object of details the core records with the failures it makes, else None."""

    # § r3sd261h4@8388n
    type: str
    message: str
    retryable: bool = False
    details: dict[str, Any] | None = field(default=None, hash=False)
    # § end r3sd261h4

    def __str__(self) -> str:
        return f"{self.type}: {self.message}"

    @classmethod
    def from_proto(cls, f: layout_pb2.Failure) -> Failure:
        kind = f.details.WhichOneof("kind")
        if kind is None:
            return cls(f.type, f.message, f.retryable)
        if kind != "json":
            raise ProtocolError(f"{f.type} with details {f.details}")
        return cls(f.type, f.message, f.retryable, _object(f.type, json.loads(f.details.json)))

    @classmethod
    def from_json(cls, error: dict[str, Any]) -> Failure:
        """A failure as the core writes it in JSON: type, message, retryable and details."""
        details = _object(error["type"], error.get("details"))
        return cls(error["type"], error["message"], error["retryable"], details)


def _object(type_: str, details: Any) -> dict[str, Any] | None:
    if details is None or isinstance(details, dict):
        return details
    raise ProtocolError(f"{type_} with details {details!r}, not an object")


# § rabbgjpb2@q81kc
class SubstantialError(Exception):
    """The base of every error Substantial raises, control flow excepted."""


class StepFailed(SubstantialError):
    """A step had no attempt left, or failed with a permanent error; `cause` is the last
    attempt's failure, such as `StepTimeout`, `ValueTooLarge`, `Json` for a value that does not
    encode, or the type of the code's error."""

    def __init__(self, failure: Failure, cause: Failure) -> None:
        super().__init__(failure.message)
        self.failure = failure
        self.cause = cause
        # The core's details of the failure, which the run's end carries as they are: a retry
        # reads the failed step from them.
        self._details: layout_pb2.Value | None = None


class Failed(SubstantialError):
    """A call resolved with an error other than a step's."""

    def __init__(self, failure: Failure) -> None:
        super().__init__(str(failure))
        self.failure = failure


class Fail(SubstantialError):
    """The workflow's own failure, raised on purpose: the run ends failed with this type and
    message."""

    def __init__(self, type: str, message: str) -> None:
        if not isinstance(type, str) or not isinstance(message, str):
            raise InvalidArgument(
                f"Fail takes a type and a message, strings: {type!r}, {message!r}"
            )
        super().__init__(f"{type}: {message}")
        self.failure = Failure(type, message)


class HistoryTooLarge(SubstantialError):
    """The run passed a limit of its history: the core ended it failed with this type."""


class Blocking(Exception):
    """An error of the code that blocks its run with `failure`, recording nothing: a value of the
    code that does not encode, a handler that cannot take a message."""

    def __init__(self, failure: Failure) -> None:
        super().__init__(str(failure))
        self.failure = failure


# § b7wsejcvb.a@4fayn
class Control(BaseException):
    """Control flow of Substantial: catch it by name only, never with a bare `except:`."""


class Cancelled(Control):
    """The run is cancelled: raised at every call it is blocked on."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class RolledBack(Control):
    """A rollback ran its undos: the run ends failed with this reason."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class ContinuedAsNew(Control):
    """The run ends and the next of its chain starts: `continue_as_new`, or `iterate`."""

    def __init__(
        self,
        workflow: str | None,
        version: int | None,
        input: bytes,
        state: bytes | None = None,
        highest: bool = False,
    ) -> None:
        super().__init__("continued as new")
        self.workflow = workflow
        self.version = version
        self.input = input
        self.state = state
        # Into the highest version of its workflow the worker serves, for a loop that upgrades.
        self.highest = highest
        # § end b7wsejcvb.a


class UndoFailed(SubstantialError):
    """An undo had no attempt left; `step` is the undo's step, `<step>.undo`."""

    def __init__(self, step: str, failure: Failure) -> None:
        super().__init__(f"undo {step} failed: {failure}")
        self.step = step
        self.failure = failure


class NotCompressible(SubstantialError):
    """The state of `iterate` does not serialize to JSON at a compression."""


class ValueTooLarge(SubstantialError):
    """A value whose JSON encoding is past `MAX_VALUE` bytes."""


# The hint a divergence from the history carries; the core records the failure without it.
NEW_VERSION = "an incompatible change needs a new version (version=)"


class RunFailed(SubstantialError):
    """The run ended failed or cancelled; `status` is "failed" or "cancelled". `cause` is the
    `StepFailed.cause` of the step that failed it, else None."""

    def __init__(self, run: str, status: str, error: Failure, cause: Failure | None) -> None:
        hint = f"; {NEW_VERSION}" if error.type == "NonDeterminism" else ""
        super().__init__(f"run {run} {status}: {error}{hint}")
        self.run = run
        self.status = status
        self.error = error
        self.cause = cause


class BudgetExceeded(SubstantialError):
    """The run passed the `limit` of its budget on `meter`, having `used` that much: raised at
    every call it was blocked on then and at every later step, start and send. The run ends
    failed with it whatever the code does."""

    def __init__(self, meter: str, limit: int, used: int) -> None:
        super().__init__(f"budget exceeded: {meter} used {used}, over the limit of {limit}")
        self.meter = meter
        self.limit = limit
        self.used = used


class NonDeterminism(SubstantialError):
    pass


class NotInWorkflow(SubstantialError):
    pass


class RunExists(SubstantialError):
    def __init__(self, id: str) -> None:
        super().__init__(f"id {id} is taken by another workflow, version or input")
        self.id = id


class InvalidArgument(SubstantialError):
    pass


class RunEnded(SubstantialError):
    pass


class RunNotFound(SubstantialError):
    """No such chain: it never existed, or it ended and its retention passed."""

    def __init__(self, chain: str) -> None:
        super().__init__(f"no chain {chain}")
        self.chain = chain


class WaitTimeout(SubstantialError):
    pass


class StoreError(SubstantialError):
    pass


class LayoutError(SubstantialError):
    pass


class ConfigError(SubstantialError):
    pass


class CoreError(SubstantialError):
    """A core error other than non-determinism and layout: `INVALID`, `ENDED`, `CORRUPT`."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(f"core error {kind}: {message}")
        self.kind = kind
        self.message = message


class Trap(SubstantialError):
    """The core trapped, a bug of the core: its instance is dropped with every machine in it."""


class ModuleError(SubstantialError):
    """The module could not be loaded: checksum, compilation, missing export."""


class ProtocolError(SubstantialError):
    """The core answered with a response this host does not expect at that point."""


# § end rabbgjpb2


def error_from_failure(f: layout_pb2.Failure) -> SubstantialError:
    """The error a call resolved with a failure raises in the code."""
    if f.type == "RunFailed":
        return _run_failed(f)
    try:
        failure = Failure.from_proto(f)
    except ProtocolError as e:
        return e
    if failure.type == "StepFailed":
        return _step_failed(f, failure)
    if failure.type == "RunNotFound":
        return _run_not_found(f)
    if failure.type == "RunEnded":
        return RunEnded(failure.message)
    if failure.type == "WaitTimeout":
        return WaitTimeout(failure.message)
    if failure.type == "BudgetExceeded":
        return _budget_exceeded(f)
    return Failed(failure)


def _budget_exceeded(f: layout_pb2.Failure) -> SubstantialError:
    """Its details hold the limit and what was used as decimal strings."""
    d = _details(f) or {}
    meter, limit, used = d.get("meter"), d.get("limit"), d.get("used")
    if (
        isinstance(meter, str)
        and isinstance(limit, str)
        and limit.isascii()
        and limit.isdigit()
        and isinstance(used, str)
        and used.isascii()
        and used.isdigit()
    ):
        return BudgetExceeded(meter, int(limit), int(used))
    return ProtocolError(f"BudgetExceeded with details {f.details}")


def _details(f: layout_pb2.Failure) -> dict[str, Any] | None:
    if f.details.WhichOneof("kind") != "json":
        return None
    d = json.loads(f.details.json)
    return d if isinstance(d, dict) else None


def _run_not_found(f: layout_pb2.Failure) -> SubstantialError:
    d = _details(f) or {}
    if d.keys() != {"run"} or not isinstance(d["run"], str):
        return ProtocolError(f"RunNotFound with details {f.details}")
    return RunNotFound(d["run"])


def _wrapped(details: Any) -> Failure | None:
    """The failure a `StepFailed`'s details wrap, `{"error": {...}}`; None if they hold none."""
    error = details.get("error") if isinstance(details, dict) else None
    if not isinstance(error, dict) or not {"type", "message", "retryable"} <= error.keys():
        return None
    return Failure.from_json(error)


def _step_failed(f: layout_pb2.Failure, failure: Failure) -> SubstantialError:
    try:
        cause = _wrapped(failure.details)
    except ProtocolError as e:
        return e
    if cause is None:
        return ProtocolError(f"StepFailed with details {f.details}")
    e = StepFailed(failure, cause)
    e._details = f.details
    return e


def run_failed_cause(type_: str, details: Any) -> Failure | None:
    """The cause of a run failed with `type_`: a `StepFailed`'s wraps it in its details."""
    # § r3sd261h4.a@27yqd
    if type_ != "StepFailed":
        return None
    cause = _wrapped(details)
    if cause is None:
        raise ProtocolError(f"a run failed with StepFailed with details {details!r}")
    return cause


def _run_failed(f: layout_pb2.Failure) -> SubstantialError:
    d = _details(f)
    if d is None:
        return ProtocolError(f"RunFailed with details {f.details}")
    status, error, reason = d.get("status"), d.get("error"), d.get("reason")
    if status == "failed" and error is not None:
        try:
            failure = Failure.from_json(error)
            cause = run_failed_cause(failure.type, failure.details)
        except ProtocolError as e:
            return e
        return RunFailed(d["run"], status, failure, cause)
    if status == "cancelled" and reason is not None:
        return RunFailed(d["run"], status, Failure("Cancelled", reason), None)
    return ProtocolError(f"RunFailed of {d.get('run')} as {status}")


def failure_of(e: BaseException) -> Failure:
    """The failure an error of the code ends or blocks its run with."""
    if isinstance(e, StepFailed | Failed | Fail | Blocking):
        return e.failure
    if isinstance(e, RolledBack):
        return Failure("RolledBack", e.reason)
    # § one r3sd261h4.c@76wte
    return Failure(type(e).__name__, str(e))
