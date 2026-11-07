"""Children (I-11) and messages sent from a workflow (I-9)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from substantial._proto import checked, json_value, run
from substantial.errors import InvalidArgument
from substantial.names import check_key, check_name
from substantial.scheduler import Shared, decode_json
from substantial.workflow import Workflow


def encode(value: Any, what: str) -> bytes:
    try:
        data = json.dumps(value, allow_nan=False).encode()
    except (TypeError, ValueError) as e:
        raise InvalidArgument(f"{what} is not JSON: {e}") from None
    return checked(data)


def input_object(input: dict[str, Any] | None) -> bytes:
    if input is None:
        return b"{}"
    if not isinstance(input, dict):
        raise InvalidArgument(f"an input is a JSON object, not {input!r}")
    return encode(input, "the input")


def done[T](shared: Shared, value: T) -> asyncio.Future[T]:
    fut: asyncio.Future[T] = shared.loop.create_future()
    fut.set_result(value)
    return fut


def send(
    shared: Shared, run_id: str, name: str, value: Any, id: str | None
) -> asyncio.Future[None]:
    check_key("run id", run_id)
    check_name("message name", name)
    data = run.Call.Send(run=run_id, name=name, value=json_value(encode(value, "the message")))
    if id is not None:
        data.id = check_key("message id", id)
    return shared.future(run.Call(send=data), decode_json)


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
        return self._shared.future(run.Call(child_result=self.chain), decode_json)

    def send(self, name: str, value: Any = None, *, id: str | None = None) -> asyncio.Future[None]:
        """Sends the child a message, delivered once per `id`, which the core derives from this
        call when none is given."""
        return send(self._shared, self.chain, name, value, id)


def start[R](
    shared: Shared, workflow: Workflow[..., R], input: dict[str, Any] | None, id: str | None
) -> asyncio.Future[Child[R]]:
    if not isinstance(workflow, Workflow):
        raise InvalidArgument(f"start takes a workflow, not {workflow!r}")
    data = run.Call.Start(workflow=workflow.name, input=json_value(input_object(input)))
    if workflow.version is not None:
        data.version = workflow.version
    if id is not None:
        data.id = check_key("run id", id)
    child: Child[R] = Child(shared, shared.expect(run.Call(start=data), "run"))
    return done(shared, child)
