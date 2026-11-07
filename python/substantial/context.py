"""What workflow code calls (I-6 to I-14); one context per replay."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, overload

from substantial import child, iterate, messages, saga
from substantial._proto import boundary, json_value, layout, run
from substantial.child import Child
from substantial.durations import millis
from substantial.errors import Cancelled, ContinuedAsNew, InvalidArgument, ProtocolError
from substantial.log import Log
from substantial.names import check_name, check_str
from substantial.scheduler import Shared, decode_json
from substantial.step import call_step
from substantial.workflow import Retry, Workflow


def _decode_now(value: layout.Value | None) -> datetime:
    text = decode_json(value)
    try:
        return datetime.fromisoformat(text)
    except (TypeError, ValueError) as e:
        raise ProtocolError(f"now {text!r}: {e}") from e


def _decode_random(value: layout.Value | None) -> float:
    x = decode_json(value)
    if not isinstance(x, float):
        raise ProtocolError(f"random {x!r}")
    return x


def _decode_uuid(value: layout.Value | None) -> uuid.UUID:
    text = decode_json(value)
    try:
        return uuid.UUID(text)
    except (TypeError, ValueError) as e:
        raise ProtocolError(f"uuid {text!r}: {e}") from e


def _decode_bool(value: layout.Value | None) -> bool:
    x = decode_json(value)
    if not isinstance(x, bool):
        raise ProtocolError(f"a wait answered {x!r}")
    return x


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _decode_none(_: layout.Value | None) -> None:
    return None


class Context:
    """The context a workflow's code calls, its first argument."""

    def __init__(self, shared: Shared) -> None:
        self._shared = shared
        self.log = Log(shared)
        """The run's logger, muted while a replay walks the recorded history."""

    @property
    def chain(self) -> str:
        """The chain id of this run, the id a client uses with `sub.run`."""
        return self._shared.start.chain

    # Overloads rather than one `Awaitable[R] | R`, which mypy solves as `Never`.
    @overload
    def step[*Ts, R](
        self,
        name: str,
        fn: Callable[[*Ts], Awaitable[R]],
        /,
        *args: *Ts,
        retry: Retry | None = None,
        timeout: timedelta | None = None,
        undo: Callable[..., Any] | None = None,
    ) -> asyncio.Future[R]: ...
    @overload
    def step[*Ts, R](
        self,
        name: str,
        fn: Callable[[*Ts], R],
        /,
        *args: *Ts,
        retry: Retry | None = None,
        timeout: timedelta | None = None,
        undo: Callable[..., Any] | None = None,
    ) -> asyncio.Future[R]: ...
    def step(
        self,
        name: str,
        fn: Callable[..., Any],
        /,
        *args: Any,
        retry: Retry | None = None,
        timeout: timedelta | None = None,
        undo: Callable[..., Any] | None = None,
    ) -> asyncio.Future[Any]:
        """Runs `fn(*args)` as the step `name`; an awaitable it returns is awaited."""
        check_name("step name", name)
        if not callable(fn):
            raise InvalidArgument(f"a step runs a function, not {fn!r}")
        return call_step(self._shared, name, fn, args, {}, retry, timeout, undo)

    def now(self) -> asyncio.Future[datetime]:
        """The time, UTC, recorded."""
        return self._shared.future(run.Call(now=boundary.Empty()), _decode_now)

    def random(self) -> asyncio.Future[float]:
        """A float in [0, 1), recorded."""
        return self._shared.future(run.Call(random=boundary.Empty()), _decode_random)

    def uuid(self) -> asyncio.Future[uuid.UUID]:
        """A UUID, recorded."""
        return self._shared.future(run.Call(uuid=boundary.Empty()), _decode_uuid)

    def sleep(self, duration: timedelta) -> asyncio.Future[None]:
        """Sleeps for `duration`, fixed when it first runs; a replay never sleeps again."""
        return self._shared.future(run.Call(sleep_ms=millis(duration)), _decode_none)

    def sleep_until(self, at: datetime) -> asyncio.Future[None]:
        """Sleeps until `at`, an aware datetime; a time that has passed fires at once."""
        if not isinstance(at, datetime) or at.utcoffset() is None:
            raise InvalidArgument(f"sleep_until takes an aware datetime, not {at!r}")
        until = layout.Time(ms=(at - _EPOCH) // timedelta(milliseconds=1))
        return self._shared.future(run.Call(sleep_until=until), _decode_none)

    def receive(self, name: str) -> asyncio.Future[Any]:
        """The next message of `name` that no `receive` took yet."""
        return self._shared.future(run.Call(receive=name), decode_json)

    @overload
    def handle(self, name: str, fn: Callable[[Any], object]) -> None: ...
    @overload
    def handle(self, handlers: Mapping[str, Callable[[Any], object]], /) -> None: ...
    def handle(
        self,
        name: str | Mapping[str, Callable[[Any], object]],
        fn: Callable[[Any], object] | None = None,
    ) -> None:
        """`handle(name, fn)` binds `fn` to the messages of `name`: every one consumed so far, in
        order, then each new one. `handle({name: fn, ...})` binds several at once, the past
        messages of all their names in the order they arrived."""
        if isinstance(name, Mapping):
            if fn is not None:
                raise InvalidArgument("handle takes a mapping of handlers alone, without fn")
            messages.handle(self._shared, name)
            return
        if fn is None:
            raise InvalidArgument(f"handle({name!r}) takes a handler")
        messages.handle(self._shared, {name: fn})

    def wait_until(
        self, predicate: Callable[[], object], *, timeout: timedelta | None = None
    ) -> asyncio.Future[bool]:
        """`True` once `predicate` holds, evaluated after each message, child end and timer;
        `False` when `timeout` passes first."""
        if not callable(predicate):
            raise InvalidArgument(f"wait_until takes a predicate, not {predicate!r}")
        wait = run.Call.Wait()
        if timeout is not None:
            wait.timeout_ms = millis(timeout)
        return self._shared.future(run.Call(wait=wait), _decode_bool, predicate=predicate)

    def send(
        self, run: str, name: str, value: Any = None, *, id: str | None = None
    ) -> asyncio.Future[None]:
        """Sends a message to the last run of the chain `run`, delivered once per `id`, which the
        core derives from this call when none is given."""
        return child.send(self._shared, run, name, value, id)

    def publish(self, key: str, value: Any) -> None:
        """Makes `value` the last one published under `key` by the chain (`run.read(key)`)."""
        published = layout.Published(key=key, value=json_value(child.encode(value, "the value")))
        self._shared.expect(run.Call(publish=published), "done")

    def saga(self) -> saga.Saga:
        """`async with c.saga():` on an error inside, the undos of the steps called inside, most
        recent first, then the error again."""
        return saga.Saga(self._shared)

    async def rollback(self, reason: str) -> None:
        """Runs the undos of every step so far, most recent first, then ends the run failed with
        `RolledBack(reason)`; it does not return."""
        await saga.rollback(self._shared, check_str("a reason", reason))

    def start[R](
        self,
        workflow: Workflow[..., R],
        input: dict[str, Any] | None = None,
        *,
        id: str | None = None,
    ) -> asyncio.Future[Child[R]]:
        """Starts a child run of `workflow`, under `id`, or an id the core derives from this
        run's."""
        return child.start(self._shared, workflow, input, id)

    def iterate[S](self, init: S) -> iterate.Iteration:
        """`async for s in c.iterate(State())`: compresses at each boundary past the threshold."""
        return iterate.Iteration(self._shared, init)

    async def continue_as_new(
        self, input: dict[str, Any] | None, *, workflow: Workflow[..., Any] | None = None
    ) -> None:
        """Ends the run and starts the next of its chain with `input`, of `workflow` if given;
        it does not return."""
        data = child.input_object(input)
        if workflow is None:
            raise ContinuedAsNew(None, None, data)
        if not isinstance(workflow, Workflow):
            raise InvalidArgument(f"continue_as_new takes a workflow, not {workflow!r}")
        raise ContinuedAsNew(workflow.name, workflow.version, data)

    async def cancel(self, reason: str) -> None:
        """Ends the run cancelled; it does not return."""
        raise Cancelled(check_str("a reason", reason))
