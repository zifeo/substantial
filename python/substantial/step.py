"""Steps (I-6): the declared step, the step call, its attempts, and the step key."""

from __future__ import annotations

import asyncio
import functools
import inspect
import weakref
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, overload

from substantial._proto import dumps, json_value, layout, run
from substantial.clock import Clock, Heartbeat
from substantial.cost import MAX
from substantial.durations import millis
from substantial.errors import InvalidArgument, NotInWorkflow, ValueTooLarge
from substantial.names import check_step_name
from substantial.scheduler import Shared, StepBody, StepSpec, Undo, decode_json
from substantial.workflow import Permanent, Retry

if TYPE_CHECKING:
    from substantial.context import Context

CURRENT: ContextVar[Context] = ContextVar("substantial.context")
# The saga the steps called here belong to, set by `async with c.saga()`.
SAGA: ContextVar[int | None] = ContextVar("substantial.saga", default=None)


class LiveAttempt:
    """The step attempt running: its step key, what it charged so far, in call order, and its
    last heartbeat."""

    def __init__(self, key: str, clock: Clock, heartbeat_ms: int | None) -> None:
        self.key = key
        self.charges: list[layout.Amount] = []
        self.clock = clock
        self.heartbeat = Heartbeat(heartbeat_ms, clock.elapsed())


# A thread a sync step runs in copies the context, and with it the attempt.
_ATTEMPT: ContextVar[LiveAttempt] = ContextVar("substantial.attempt")
# The tasks whose step body runs in a thread, which cannot be interrupted: the testing environment
# waits for them rather than cancelling them.
THREADED: weakref.WeakSet[asyncio.Task[Any]] = weakref.WeakSet()


def current() -> Context:
    try:
        return CURRENT.get()
    except LookupError:
        raise NotInWorkflow("a declared step is called outside a workflow") from None


def step_key() -> str:
    """`<run>/<step>/<occurrence>`, the same for every attempt and replay of the step running."""
    try:
        return _ATTEMPT.get().key
    except LookupError:
        raise NotInWorkflow("step_key is called inside a step") from None


def heartbeat() -> None:
    """Tells the worker that the step attempt running is alive: a step with a
    `heartbeat_timeout` fails its attempt with a retryable `StepTimeout` once that long passed
    without one."""
    # § vvp37rp40.a@v8xsf
    try:
        running = _ATTEMPT.get()
    except LookupError:
        raise NotInWorkflow("heartbeat is called inside a step") from None
    running.heartbeat.beat = running.clock.elapsed()


def charge(currency: str, nano: int) -> None:
    """Adds `nano` nano-units of `currency` to the charges of the step attempt running, recorded
    with its outcome and counted in the run's cost; a currency outside ISO 4217 fails the step at
    once with `InvalidArgument`, once the attempt ended."""
    # § cvnf47vy8@z6wnv
    try:
        running = _ATTEMPT.get()
    except LookupError:
        raise NotInWorkflow("charge is called inside a step") from None
    if not isinstance(currency, str):
        raise InvalidArgument(f"a currency is a string, not {currency!r}")
    # § one cvnf47vy8.a@ebbwf
    if isinstance(nano, bool) or not isinstance(nano, int) or not 0 <= nano <= MAX:
        raise InvalidArgument(f"a charge is an int from 0 to {MAX}, not {nano!r}")
    running.charges.append(layout.Amount(currency=currency, nano=nano))


def _is_async(fn: Callable[..., Any]) -> bool:
    return inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(
        getattr(fn, "__call__", None)  # noqa: B004
    )


def check_options(
    retry: Retry | None,
    timeout: timedelta | None,
    undo: object = None,
    heartbeat_timeout: timedelta | None = None,
) -> None:
    if retry is not None and not isinstance(retry, Retry):
        raise InvalidArgument(f"retry is a Retry, not {retry!r}")
    if timeout is not None:
        millis(timeout)
    if heartbeat_timeout is not None and millis(heartbeat_timeout) == 0:
        raise InvalidArgument(f"a step's heartbeat_timeout is positive, not {heartbeat_timeout}")
    if undo is not None and not callable(undo):
        raise InvalidArgument(f"undo is a function, not {undo!r}")


def body_of(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> StepBody:
    if _is_async(fn):
        return functools.partial(fn, *args, **kwargs)
    # Synchronous functions run in a thread, so a blocking call never stops the worker.
    return functools.partial(_in_thread, fn, *args, **kwargs)


async def _in_thread(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    # § v4n6ja42e.b@0w0bc
    task = asyncio.current_task()
    if task is not None:
        THREADED.add(task)
    return await asyncio.to_thread(fn, *args, **kwargs)


def call_step(
    shared: Shared,
    name: str,
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    retry: Retry | None,
    timeout: timedelta | None,
    undo: Callable[..., Any] | None = None,
    heartbeat_timeout: timedelta | None = None,
) -> asyncio.Future:
    """Calls the step `name`: its occurrence is taken now, and its value comes from its attempts
    or from the history."""
    check_options(retry, timeout, undo, heartbeat_timeout)
    # § aym52h8ch@yvspe
    options = run.StepOptions(
        timeout_ms=shared.step_timeout_ms if timeout is None else millis(timeout)
    )
    # § end aym52h8ch
    # § vvp37rp40@w3r9z
    heartbeat_ms = (
        shared.heartbeat_timeout_ms if heartbeat_timeout is None else millis(heartbeat_timeout)
    )
    # § end vvp37rp40
    if retry is not None:
        options.retry.CopyFrom(retry.policy())
    undo_spec: Undo | None = None
    if undo is not None:
        u = undo
        undo_options = run.StepOptions()
        undo_options.CopyFrom(options)
        # § a5c1mfs8d@nbm20
        undo_spec = (lambda value: body_of(u, (value, *args), kwargs), undo_options, heartbeat_ms)
        # § end a5c1mfs8d
        options.undo = True
    saga = SAGA.get()
    if saga is not None:
        options.saga = saga
    call = run.Call(step=run.Call.Step(name=name, options=options))
    # § one wv67rmrjm@c8x6e, vjy5p3exq@8mjvv
    spec = StepSpec(body_of(fn, args, kwargs), undo_spec, heartbeat_ms)
    return shared.future(call, decode_json, spec)


class Step[**P, R]:
    """A declared step: calling it inside a workflow returns the future of its value."""

    def __init__(
        self,
        fn: Callable[P, Awaitable[R] | R],
        *,
        name: str | None = None,
        retry: Retry | None = None,
        timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        undo: Callable[..., Any] | None = None,
    ) -> None:
        if not callable(fn):
            raise InvalidArgument(f"a step is a function, not {fn!r}")
        check_options(retry, timeout, undo, heartbeat_timeout)
        self.fn = fn
        self.name: str = check_step_name(fn.__name__ if name is None else name)
        self.retry = retry
        self.timeout = timeout
        self.heartbeat_timeout = heartbeat_timeout
        self.undo = undo
        functools.update_wrapper(self, fn)

    def __repr__(self) -> str:
        return f"<step {self.name}>"

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> asyncio.Future[R]:
        c = current()
        return call_step(
            c._shared,
            self.name,
            self.fn,
            args,
            kwargs,
            self.retry,
            self.timeout,
            self.undo,
            self.heartbeat_timeout,
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
    heartbeat_timeout: timedelta | None = None,
    undo: Callable[..., Any] | None = None,
) -> _StepDecorator: ...
def step(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    name: str | None = None,
    retry: Retry | None = None,
    timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    undo: Callable[..., Any] | None = None,
) -> Step[..., Any] | _StepDecorator:
    """`@step` or `@step(name=, retry=, timeout=, heartbeat_timeout=, undo=)`; `undo` takes the
    step's value then its arguments.

    An attempt the worker abandons, past its timeout or its worker's shutdown grace, is cancelled
    at its `await` when `async`; a plain `def`, run in a thread, cannot be stopped and runs to its
    end, its outcome dropped."""
    if fn is not None:
        return Step(fn)
    return lambda f: Step(
        f,
        name=name,
        retry=retry,
        timeout=timeout,
        heartbeat_timeout=heartbeat_timeout,
        undo=undo,
    )


def _failure(type_: str, message: str, retryable: bool) -> run.Outcome:
    return run.Outcome(error=layout.Failure(type=type_, message=message, retryable=retryable))


async def attempt(
    clock: Clock,
    name: str,
    body: StepBody,
    key: str,
    timeout_ms: int | None,
    heartbeat_ms: int | None,
) -> tuple[run.Outcome, list[layout.Amount]]:
    """Runs one attempt of a step with its step key, to its outcome and its charges; a
    `Permanent` error is not retryable."""
    running = LiveAttempt(key, clock, heartbeat_ms)
    _ATTEMPT.set(running)
    ended = await clock.attempt(name, key, lambda: _outcome(body), timeout_ms, running.heartbeat)
    if ended == "timeout":
        # § one aym52h8ch@8hw3s, v4n6ja42e@8b671
        outcome = _failure("StepTimeout", f"attempt past {timeout_ms} ms", True)
    elif ended == "silent":
        # § one vvp37rp40@4c6d7
        outcome = _failure("StepTimeout", f"no heartbeat within {heartbeat_ms} ms", True)
    else:
        outcome = ended
    # A copy: an attempt left running in its thread charges on, past its end.
    # § one cvnf47vy8@x7r7t
    return outcome, list(running.charges)


async def _outcome(body: StepBody) -> run.Outcome:
    # § epxcjcz4s@1rrce
    try:
        value = await body()
        # A sync function such as `lambda: charge(card)` may return a coroutine to await.
        if inspect.isawaitable(value):
            value = await value
    except Permanent as e:
        # § epxcjcz4s.a@nxz47
        return _failure(type(e).__name__, str(e), False)
    except Exception as e:
        return _failure(type(e).__name__, str(e), True)
    try:
        data = dumps(value)
    except (TypeError, ValueError) as e:
        return _failure("Json", str(e), False)
    try:
        return run.Outcome(value=json_value(data))
    except ValueTooLarge as e:
        # § h8wg39cct.b@q88x6
        return _failure(type(e).__name__, str(e), False)
