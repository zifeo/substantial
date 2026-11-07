"""Workflows (I-5), the retry policy and permanent errors of steps (I-6)."""

from __future__ import annotations

import inspect
import json
import math
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Concatenate, Protocol, overload

from substantial._proto import checked, run
from substantial.durations import millis
from substantial.errors import ConfigError, Failed, Failure, InvalidArgument
from substantial.names import check_key

if TYPE_CHECKING:
    from substantial.context import Context

_U32 = 1 << 32


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class Retry:
    """Exponential backoff for a step: every field is required (I-6)."""

    max_attempts: int
    initial: timedelta
    max: timedelta
    multiplier: float

    def __post_init__(self) -> None:
        if not _is_int(self.max_attempts) or not 0 < self.max_attempts < _U32:
            raise InvalidArgument(f"max_attempts must be a positive int: {self.max_attempts!r}")
        if not isinstance(self.initial, timedelta) or not isinstance(self.max, timedelta):
            raise InvalidArgument("initial and max are timedeltas")
        if self.initial < timedelta(0) or self.max < self.initial:
            raise InvalidArgument(f"invalid backoff from {self.initial} to {self.max}")
        if isinstance(self.multiplier, bool) or not isinstance(self.multiplier, int | float):
            raise InvalidArgument(f"multiplier is a number, not {self.multiplier!r}")
        if not math.isfinite(self.multiplier) or self.multiplier < 1:
            raise InvalidArgument(f"multiplier must be at least 1: {self.multiplier!r}")

    def policy(self) -> run.RetryPolicy:
        return run.RetryPolicy(
            max_attempts=self.max_attempts,
            initial_ms=millis(self.initial),
            max_ms=millis(self.max),
            multiplier=float(self.multiplier),
        )


class Permanent(Exception):
    """An error that fails a step at once, attempts left or not."""


class Workflow[**P, R]:
    """A workflow: a name, an optional version, and an async function of a context and the
    fields of a JSON object input."""

    def __init__(
        self,
        fn: Callable[Concatenate[Context, P], Awaitable[R]],
        *,
        name: str | None = None,
        version: int | None = None,
        step_timeout: timedelta | None = None,
    ):
        if not inspect.iscoroutinefunction(fn):
            raise InvalidArgument(f"a workflow is an async function, not {fn!r}")
        if version is not None and (not _is_int(version) or not 0 <= version < _U32):
            raise InvalidArgument(f"a workflow version is an int in [0, 2**32), not {version!r}")
        if step_timeout is not None and millis(step_timeout) == 0:
            raise InvalidArgument(f"a workflow step_timeout is positive, not {step_timeout}")
        self.fn: Callable[..., Awaitable[R]] = fn
        self.name: str = check_key("workflow name", fn.__name__ if name is None else name)
        self.version = version
        # The timeout of its steps without their own, over the worker's `step_timeout`.
        self.step_timeout = step_timeout
        self.__doc__ = fn.__doc__
        self.__wrapped__ = fn

    def __repr__(self) -> str:
        version = "" if self.version is None else f" version {self.version}"
        return f"<workflow {self.name}{version}>"

    async def run(self, c: Context, input_json: bytes) -> bytes:
        """Runs the code on a JSON object input, and encodes its value."""
        fields = json.loads(input_json)
        if not isinstance(fields, dict):
            raise InvalidArgument(f"an input is a JSON object, not {fields!r}")
        value = await self.fn(c, **fields)
        try:
            data = json.dumps(value, allow_nan=False).encode()
        except (TypeError, ValueError) as e:
            raise Failed(Failure("Json", f"the value of {self.name}: {e}")) from None
        return checked(data)


class _WorkflowDecorator(Protocol):
    def __call__[**P, R](
        self, fn: Callable[Concatenate[Context, P], Awaitable[R]], /
    ) -> Workflow[P, R]: ...


@overload
def workflow[**P, R](fn: Callable[Concatenate[Context, P], Awaitable[R]], /) -> Workflow[P, R]: ...
@overload
def workflow(
    *, name: str | None = None, version: int | None = None, step_timeout: timedelta | None = None
) -> _WorkflowDecorator: ...
def workflow(
    fn: Callable[..., Awaitable[Any]] | None = None,
    /,
    *,
    name: str | None = None,
    version: int | None = None,
    step_timeout: timedelta | None = None,
) -> Workflow[..., Any] | _WorkflowDecorator:
    """`@workflow` or `@workflow(name=, version=, step_timeout=)`."""
    if fn is not None:
        return Workflow(fn)
    return lambda f: Workflow(f, name=name, version=version, step_timeout=step_timeout)


class Workflows:
    """The workflows a worker serves, by name and version."""

    def __init__(self, workflows: Iterable[Workflow[..., Any]]) -> None:
        self.by_key: dict[tuple[str, int | None], Workflow[..., Any]] = {}
        if isinstance(workflows, str | Workflow) or not isinstance(workflows, Iterable):
            raise ConfigError(f"a worker serves a list of workflows, not {workflows!r}")
        for w in workflows:
            if not isinstance(w, Workflow):
                raise ConfigError(f"a worker serves workflows, not {w!r}")
            check_key("workflow name", w.name)
            key = (w.name, w.version)
            if key in self.by_key:
                raise ConfigError(f"workflow {w.name!r} version {w.version!r} given twice")
            self.by_key[key] = w
        if not self.by_key:
            raise ConfigError("a worker needs at least one workflow")

    def get(self, name: str, version: int | None) -> Workflow[..., Any] | None:
        return self.by_key.get((name, version))

    def served(self) -> list[tuple[str, list[int], bool]]:
        """Each name served: its versions, and whether its runs without a version are."""
        names = sorted({name for name, _ in self.by_key})
        return [
            (
                name,
                sorted(v for n, v in self.by_key if n == name and v is not None),
                (name, None) in self.by_key,
            )
            for name in names
        ]
