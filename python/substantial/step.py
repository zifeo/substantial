"""Steps (I-6): the declared step, the step call, its attempts, and the step key."""

from __future__ import annotations

import asyncio
import functools
import inspect
import json
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, overload

from substantial._proto import json_value, layout, run
from substantial.durations import millis
from substantial.errors import InvalidArgument, NotInWorkflow, ValueTooLarge
from substantial.names import check_name
from substantial.scheduler import Shared, StepBody, StepSpec, UndoBody, decode_json
from substantial.workflow import Permanent, Retry

if TYPE_CHECKING:
    from substantial.context import Context

CURRENT: ContextVar[Context] = ContextVar("substantial.context")
# The saga the steps called here belong to, set by `async with c.saga()`.
SAGA: ContextVar[int | None] = ContextVar("substantial.saga", default=None)
_STEP_KEY: ContextVar[str] = ContextVar("substantial.step_key")


def current() -> Context:
    try:
        return CURRENT.get()
    except LookupError:
        raise NotInWorkflow("a declared step is called outside a workflow") from None


def step_key() -> str:
    """`<run>/<step>/<occurrence>`, the same for every attempt and replay of the step running."""
    try:
        return _STEP_KEY.get()
    except LookupError:
        raise NotInWorkflow("step_key is called inside a step") from None


def _is_async(fn: Callable[..., Any]) -> bool:
    return inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(
        getattr(fn, "__call__", None)  # noqa: B004
    )


def check_options(retry: Retry | None, timeout: timedelta | None, undo: object = None) -> None:
    if retry is not None and not isinstance(retry, Retry):
        raise InvalidArgument(f"retry is a Retry, not {retry!r}")
    if timeout is not None:
        millis(timeout)
    if undo is not None and not callable(undo):
        raise InvalidArgument(f"undo is a function, not {undo!r}")


def body_of(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> StepBody:
    if _is_async(fn):
        return functools.partial(fn, *args, **kwargs)
    # Synchronous functions run in a thread, so a blocking call never stops the worker.
    return functools.partial(asyncio.to_thread, fn, *args, **kwargs)


def call_step(
    shared: Shared,
    name: str,
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    retry: Retry | None,
    timeout: timedelta | None,
    undo: Callable[..., Any] | None = None,
) -> asyncio.Future:
    """Calls the step `name`: its occurrence is taken now, and its value comes from its attempts
    or from the history."""
    check_options(retry, timeout, undo)
    options = run.StepOptions(
        timeout_ms=shared.step_timeout_ms if timeout is None else millis(timeout)
    )
    if retry is not None:
        options.retry.CopyFrom(retry.policy())
    undo_spec: tuple[UndoBody, run.StepOptions] | None = None
    if undo is not None:
        u = undo
        undo_options = run.StepOptions()
        undo_options.CopyFrom(options)
        undo_spec = (lambda value: body_of(u, (value, *args), kwargs), undo_options)
        options.undo = True
    saga = SAGA.get()
    if saga is not None:
        options.saga = saga
    call = run.Call(step=run.Call.Step(name=name, options=options))
    return shared.future(call, decode_json, StepSpec(body_of(fn, args, kwargs), undo_spec))


class Step[**P, R]:
    """A declared step: calling it inside a workflow returns the future of its value."""

    def __init__(
        self,
        fn: Callable[P, Awaitable[R] | R],
        *,
        name: str | None = None,
        retry: Retry | None = None,
        timeout: timedelta | None = None,
        undo: Callable[..., Any] | None = None,
    ) -> None:
        if not callable(fn):
            raise InvalidArgument(f"a step is a function, not {fn!r}")
        check_options(retry, timeout, undo)
        self.fn = fn
        self.name: str = check_name("step name", fn.__name__ if name is None else name)
        self.retry = retry
        self.timeout = timeout
        self.undo = undo
        functools.update_wrapper(self, fn)

    def __repr__(self) -> str:
        return f"<step {self.name}>"

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> asyncio.Future[R]:
        c = current()
        return call_step(
            c._shared, self.name, self.fn, args, kwargs, self.retry, self.timeout, self.undo
        )


# Overloads rather than one `Awaitable[R] | R`, which mypy solves as `Never`.
class _StepDecorator(Protocol):
    @overload
    def __call__[**P, R](self, fn: Callable[P, Awaitable[R]], /) -> Step[P, R]: ...
    @overload
    def __call__[**P, R](self, fn: Callable[P, R], /) -> Step[P, R]: ...


@overload
def step[**P, R](fn: Callable[P, Awaitable[R]], /) -> Step[P, R]: ...
@overload
def step[**P, R](fn: Callable[P, R], /) -> Step[P, R]: ...
@overload
def step(
    *,
    name: str | None = None,
    retry: Retry | None = None,
    timeout: timedelta | None = None,
    undo: Callable[..., Any] | None = None,
) -> _StepDecorator: ...
def step(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    name: str | None = None,
    retry: Retry | None = None,
    timeout: timedelta | None = None,
    undo: Callable[..., Any] | None = None,
) -> Step[..., Any] | _StepDecorator:
    """`@step` or `@step(name=, retry=, timeout=, undo=)`; `undo` takes the step's value then its
    arguments."""
    if fn is not None:
        return Step(fn)
    return lambda f: Step(f, name=name, retry=retry, timeout=timeout, undo=undo)


def _failure(type_: str, message: str, retryable: bool) -> run.Outcome:
    return run.Outcome(error=layout.Failure(type=type_, message=message, retryable=retryable))


async def attempt(body: StepBody, key: str, timeout_ms: int | None) -> run.Outcome:
    """Runs one attempt of a step with its step key; a `Permanent` error is not retryable."""
    _STEP_KEY.set(key)
    timeout = asyncio.timeout(None if timeout_ms is None else timeout_ms / 1000)
    try:
        async with timeout:
            value = await body()
            # A sync function such as `lambda: charge(card)` may return a coroutine to await.
            if inspect.isawaitable(value):
                value = await value
    except Permanent as e:
        return _failure(type(e).__name__, str(e), False)
    except Exception as e:
        if isinstance(e, TimeoutError) and timeout.expired():
            return _failure("StepTimeout", f"attempt past {timeout_ms} ms", True)
        return _failure(type(e).__name__, str(e), True)
    try:
        data = json.dumps(value, allow_nan=False).encode()
    except (TypeError, ValueError) as e:
        return _failure("Json", str(e), False)
    try:
        return run.Outcome(value=json_value(data))
    except ValueTooLarge as e:
        return _failure(type(e).__name__, str(e), False)
