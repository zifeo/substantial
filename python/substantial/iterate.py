"""Loops and compression (I-12): the state goes to the next run."""

from __future__ import annotations

import dataclasses
import json
from typing import Any

from substantial._proto import boundary, checked, json_value, run
from substantial.child import encode
from substantial.errors import ContinuedAsNew, NotCompressible, ProtocolError
from substantial.scheduler import Shared, decode_json


def should_continue_as_new(shared: Shared) -> bool:
    value = decode_json(shared.expect(run.Call(should_continue_as_new=boundary.Empty()), "value"))
    if not isinstance(value, bool):
        raise shared.protocol(f"should_continue_as_new answered {value!r}")
    return value


def _fields(state: Any) -> Any:
    if dataclasses.is_dataclass(state) and not isinstance(state, type):
        return dataclasses.asdict(state)
    return state


def _encode(value: Any, what: str) -> bytes:
    try:
        data = json.dumps(value, allow_nan=False).encode()
    except (TypeError, ValueError) as e:
        raise NotCompressible(f"{what} does not serialize to JSON: {e}") from None
    return checked(data)


def _decode(cls: type, fields: Any) -> Any:
    if dataclasses.is_dataclass(cls):
        if not isinstance(fields, dict):
            raise ProtocolError(f"iterate answered {fields!r} for {cls.__name__}")
        return cls(**fields)
    return fields


class Iteration:
    """`async for s in c.iterate(State())`: the same state object at each iteration."""

    def __init__(self, shared: Shared, init: Any) -> None:
        self._shared = shared
        self._init = init
        self._state: Any = None
        self._started = False

    def __aiter__(self) -> Iteration:
        return self

    async def __anext__(self) -> Any:
        shared = self._shared
        if not self._started:
            self._started = True
            initial = encode(_fields(self._init), "the state of iterate")
            value = shared.expect(run.Call(iterate=json_value(initial)), "value")
            self._state = _decode(type(self._init), decode_json(value))
            return self._state
        if should_continue_as_new(shared):
            state = _encode(_fields(self._state), "the state of iterate")
            raise ContinuedAsNew(None, None, shared.input, state)
        return self._state
