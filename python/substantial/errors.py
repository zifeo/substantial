"""The errors of I-20, and the host's own failures around the core."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from substantial.layout.v1 import layout_pb2


@dataclass(frozen=True)
class Failure:
    """A failure as recorded in a history: a type name, a message, whether a retry may help."""

    type: str
    message: str
    retryable: bool = False

    def __str__(self) -> str:
        return f"{self.type}: {self.message}"

    @classmethod
    def from_proto(cls, f: layout_pb2.Failure) -> Failure:
        return cls(f.type, f.message, f.retryable)


class SubstantialError(Exception):
    """The base of every error Substantial raises, control flow excepted."""


class StepFailed(SubstantialError):
    """A step had no attempt left, or failed with a permanent error; `cause` is the last
    attempt's failure, such as `StepTimeout`, `ValueTooLarge` or the type of the code's error."""

    def __init__(self, failure: Failure, cause: Failure) -> None:
        super().__init__(failure.message)
        self.failure = failure
        self.cause = cause


class Failed(SubstantialError):
    """A call resolved with an error other than a step's."""

    def __init__(self, failure: Failure) -> None:
        super().__init__(str(failure))
        self.failure = failure


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
    ) -> None:
        super().__init__("continued as new")
        self.workflow = workflow
        self.version = version
        self.input = input
        self.state = state


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


class NonDeterminism(SubstantialError):
    pass


class NotInWorkflow(SubstantialError):
    pass


def _rfc3339(t: datetime) -> str:
    return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class RunExists(SubstantialError):
    """`collected` when the id is taken by a run collected, `until` then."""

    def __init__(self, id: str, *, until: datetime | None) -> None:
        by = (
            "another workflow, version or input"
            if until is None
            else f"a run collected, until {_rfc3339(until)}"
        )
        super().__init__(f"id {id} is taken by {by}")
        self.collected = until is not None
        self.until = until


class InvalidArgument(SubstantialError):
    pass


class RunEnded(SubstantialError):
    pass


class RunNotFound(SubstantialError):
    """`collected` when the chain's first run was collected, its id taken `until` then."""

    def __init__(self, chain: str, *, until: datetime | None) -> None:
        collected = (
            ""
            if until is None
            else f": it was collected, and its id is taken until {_rfc3339(until)}"
        )
        super().__init__(f"no chain {chain}{collected}")
        self.collected = until is not None
        self.until = until


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


def error_from_failure(f: layout_pb2.Failure) -> SubstantialError:
    """The error a call resolved with a failure raises in the code."""
    if f.type == "RunFailed":
        return _run_failed(f)
    failure = Failure.from_proto(f)
    if failure.type == "StepFailed":
        return _step_failed(f, failure)
    if failure.type == "RunNotFound":
        return _run_not_found(f)
    if failure.type == "RunEnded":
        return RunEnded(failure.message)
    return Failed(failure)


def _details(f: layout_pb2.Failure) -> dict[str, Any] | None:
    if f.details.WhichOneof("kind") != "json":
        return None
    d = json.loads(f.details.json)
    return d if isinstance(d, dict) else None


def _run_not_found(f: layout_pb2.Failure) -> SubstantialError:
    d = _details(f) or {}
    run, until = d.get("run"), d.get("until")
    if not isinstance(run, str) or not isinstance(until, str | None):
        return ProtocolError(f"RunNotFound with details {f.details}")
    try:
        return RunNotFound(run, until=None if until is None else datetime.fromisoformat(until))
    except ValueError:
        return ProtocolError(f"RunNotFound with details {f.details}")


def _wrapped(details: Any) -> Failure | None:
    """The failure a `StepFailed`'s details wrap, `{"error": {...}}`; None if they hold none."""
    error = details.get("error") if isinstance(details, dict) else None
    if not isinstance(error, dict) or not {"type", "message", "retryable"} <= error.keys():
        return None
    return Failure(error["type"], error["message"], error["retryable"])


def _step_failed(f: layout_pb2.Failure, failure: Failure) -> SubstantialError:
    cause = _wrapped(_details(f))
    if cause is None:
        return ProtocolError(f"StepFailed with details {f.details}")
    return StepFailed(failure, cause)


def run_failed_cause(type_: str, details: Any) -> Failure | None:
    """The cause of a run failed with `type_`: a `StepFailed`'s wraps it in its details."""
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
        failure = Failure(error["type"], error["message"], error["retryable"])
        try:
            cause = run_failed_cause(failure.type, error.get("details"))
        except ProtocolError as e:
            return e
        return RunFailed(d["run"], status, failure, cause)
    if status == "cancelled" and reason is not None:
        return RunFailed(d["run"], status, Failure("Cancelled", reason), None)
    return ProtocolError(f"RunFailed of {d.get('run')} as {status}")


def failure_of(e: BaseException) -> Failure:
    """The failure an error of the code ends its run with."""
    if isinstance(e, StepFailed | Failed):
        return e.failure
    if isinstance(e, RolledBack):
        return Failure("RolledBack", e.reason)
    return Failure(type(e).__name__, str(e))
