"""The errors of I-20, and the host's own failures around the core."""

from __future__ import annotations

import json
from dataclasses import dataclass
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
    """The run ended failed or cancelled; `status` is "failed" or "cancelled"."""

    def __init__(self, run: str, status: str, error: Failure) -> None:
        hint = f"; {NEW_VERSION}" if error.type == "NonDeterminism" else ""
        super().__init__(f"run {run} {status}: {error}{hint}")
        self.run = run
        self.status = status
        self.error = error


class NonDeterminism(SubstantialError):
    pass


class NotInWorkflow(SubstantialError):
    pass


class RunExists(SubstantialError):
    pass


class InvalidArgument(SubstantialError):
    pass


class RunEnded(SubstantialError):
    pass


class RunNotFound(SubstantialError):
    pass


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
        return RunNotFound(failure.message)
    if failure.type == "RunEnded":
        return RunEnded(failure.message)
    return Failed(failure)


def _details(f: layout_pb2.Failure) -> dict[str, Any] | None:
    if f.details.WhichOneof("kind") != "json":
        return None
    d = json.loads(f.details.json)
    return d if isinstance(d, dict) else None


def _step_failed(f: layout_pb2.Failure, failure: Failure) -> SubstantialError:
    error = (_details(f) or {}).get("error")
    if not isinstance(error, dict) or not {"type", "message", "retryable"} <= error.keys():
        return ProtocolError(f"StepFailed with details {f.details}")
    return StepFailed(failure, Failure(error["type"], error["message"], error["retryable"]))


def _run_failed(f: layout_pb2.Failure) -> SubstantialError:
    d = _details(f)
    if d is None:
        return ProtocolError(f"RunFailed with details {f.details}")
    status, error, reason = d.get("status"), d.get("error"), d.get("reason")
    if status == "failed" and error is not None:
        return RunFailed(
            d["run"], status, Failure(error["type"], error["message"], error["retryable"])
        )
    if status == "cancelled" and reason is not None:
        return RunFailed(d["run"], status, Failure("Cancelled", reason))
    return ProtocolError(f"RunFailed of {d.get('run')} as {status}")


def failure_of(e: BaseException) -> Failure:
    """The failure an error of the code ends its run with."""
    if isinstance(e, StepFailed | Failed):
        return e.failure
    if isinstance(e, RolledBack):
        return Failure("RolledBack", e.reason)
    return Failure(type(e).__name__, str(e))
