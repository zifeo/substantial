"""Children (I-11) and messages sent from a workflow (I-9)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any

import rfc8785

from substantial._proto import check_json, checked, dumps, json_value, layout, run
from substantial.cost import budget_proto
from substantial.durations import millis
from substantial.errors import InvalidArgument, RunFailed
from substantial.names import check_key, check_sent_name, check_str
from substantial.scheduler import Shared, decode_json
from substantial.workflow import Workflow


def encode(value: Any, what: str) -> bytes:
    try:
        data = dumps(value)
    except (TypeError, ValueError) as e:
        # § z6zyx22ss.c@e22dv
        raise InvalidArgument(f"{what} is not JSON: {e}") from None
    # § one h8wg39cct.c@ke9f0
    return checked(data)


def input_object(input: dict[str, Any] | None) -> bytes:
    """An input as canonical JSON (RFC 8785), as the core compares a start's input byte for
    byte."""
    # § e1dc4dar4.a@3952h
    if input is None:
        return b"{}"
    if not isinstance(input, dict):
        raise InvalidArgument(f"an input is a JSON object, not {input!r}")
    # § end e1dc4dar4.a
    try:
        # § aaev3rs1q@wn64x
        # § one z6zyx22ss.a@tm9wf
        # silcrow:follow
        check_json(input)
        data = rfc8785.dumps(input)
    except (ValueError, rfc8785.CanonicalizationError) as e:
        # § one aaev3rs1q.a@kgccx, aaev3rs1q.c@0kpnv
        raise InvalidArgument(f"the input is not JSON: {e}") from None
    # § one h8wg39cct.c@fp03p
    return checked(data)


def effective_budget(
    budget: Mapping[str, int] | None, workflow: Workflow[..., Any]
) -> layout.Budget | None:
    """The start's own budget, else its workflow's, else none."""
    # § evdz71nzb.a@yhr1r
    if budget is not None:
        return budget_proto(budget)
    if workflow.budget is not None:
        return budget_proto(workflow.budget)
    return None


def done[T](shared: Shared, value: T) -> asyncio.Future[T]:
    fut: asyncio.Future[T] = shared.loop.create_future()
    fut.set_result(value)
    return fut


def send(
    shared: Shared, run_id: str, name: str, value: Any, id: str | None
) -> asyncio.Future[None]:
    check_key("run id", run_id)
    check_sent_name(name)
    data = run.Call.Send(run=run_id, name=name, value=json_value(encode(value, "the message")))
    if id is not None:
        data.id = check_key("message id", id)
    return shared.future(run.Call(send=data), decode_json)


def failure_read(shared: Shared, chain: str) -> Callable[[asyncio.Future[Any]], None]:
    """Tells the clock when a result of `chain` gave the code its failure."""

    def read(result: asyncio.Future[Any]) -> None:
        if result.cancelled():
            return
        e = result.exception()
        if isinstance(e, RunFailed) and e.status == "failed":
            shared.clock.failure_read(chain)

    return read


def chain_result(shared: Shared, chain: str, timeout: timedelta | None) -> asyncio.Future[Any]:
    data = run.Call.ChainResult(chain=check_key("run id", chain))
    if timeout is not None:
        data.timeout_ms = millis(timeout)
    result = shared.future(run.Call(chain_result=data), decode_json)
    result.add_done_callback(failure_read(shared, chain))
    return result


class Child[R]:
    """A run started by this one: its `result`, whether it is `done`, and messages to it."""

    def __init__(self, shared: Shared, chain: str) -> None:
        self._shared = shared
        self.chain = chain
        """The child's chain id, the id a client uses with `sub.run`."""

    def __repr__(self) -> str:
        return f"<child {self.chain}>"

    @property
    def done(self) -> bool:
        """Whether the child's chain ended, for a `wait_until` over several things."""
        return self.chain in self._shared.ended

    def result(self) -> asyncio.Future[R]:
        """The value the child's chain ended with; `RunFailed` if it failed or was cancelled."""
        result = self._shared.future(run.Call(child_result=self.chain), decode_json)
        result.add_done_callback(failure_read(self._shared, self.chain))
        return result

    def send(self, name: str, value: Any = None, *, id: str | None = None) -> asyncio.Future[None]:
        """Sends the child a message, delivered once per `id`, which the core derives from this
        call when none is given."""
        return send(self._shared, self.chain, name, value, id)

    def cancel(self, reason: str) -> asyncio.Future[None]:
        """Cancels the child as a client's `run.cancel` does; resolves once the outcome is
        recorded, before the child ends: `result()` then raises `RunFailed`, cancelled."""
        data = run.Call.Cancel(run=self.chain, reason=check_str("a reason", reason))
        return self._shared.future(run.Call(cancel=data), decode_json)


def start[R](
    shared: Shared,
    workflow: Workflow[..., R],
    input: dict[str, Any] | None,
    id: str | None,
    budget: Mapping[str, int] | None,
    abandon: bool,
) -> asyncio.Future[Child[R]]:
    if not isinstance(workflow, Workflow):
        # § bxb0ygj7m@cn660
        raise InvalidArgument(f"start takes a workflow, not {workflow!r}")
    # § aaev3rs1q.b@fjn4v
    data = run.Call.Start(
        workflow=workflow.name,
        input=json_value(input_object(input)),
        budget=effective_budget(budget, workflow),
    )
    # § end aaev3rs1q.b
    if workflow.version is not None:
        data.version = workflow.version
    if id is not None:
        data.id = check_key("run id", id)
    if not isinstance(abandon, bool):
        raise InvalidArgument(f"abandon is a bool, not {abandon!r}")
    data.abandon = abandon
    child: Child[R] = Child(shared, shared.expect(run.Call(start=data), "run"))
    return done(shared, child)
