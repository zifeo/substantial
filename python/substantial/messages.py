"""Handlers bound to a run's messages (I-9)."""

from __future__ import annotations

import inspect
import types
import typing
from collections.abc import Callable, Mapping
from typing import Any

from substantial._proto import run
from substantial.errors import Failed, Failure, InvalidArgument
from substantial.scheduler import Shared, decode_json

_JSON_TYPES = (int, float, str, bool, list, dict, type(None))


def _accepted(annotation: Any, where: str) -> tuple[type, ...] | None:
    """The JSON types a handler's parameter annotation accepts; `None` for any value."""
    if annotation is inspect.Parameter.empty or annotation is Any:
        return None
    if annotation is None:
        return (type(None),)
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        accepted: list[type] = []
        for arg in typing.get_args(annotation):
            types_ = _accepted(arg, where)
            if types_ is None:
                return None
            accepted.extend(types_)
        return tuple(accepted)
    # A generic such as list[str] is checked by its origin only.
    base = origin if origin is not None else annotation
    if base in _JSON_TYPES:
        return (base,)
    raise InvalidArgument(
        f"{where} takes {annotation!r}: handlers decode only the JSON types "
        "(int, float, str, bool, list, dict, None) and their unions so far"
    )


def _parameter(fn: Callable[..., Any]) -> tuple[type, ...] | None:
    """The types the single parameter of a handler accepts."""
    try:
        signature = inspect.signature(fn, eval_str=True)
    except ValueError:
        # A builtin without a signature, as `list.append`: nothing to check.
        return None
    except NameError as e:
        raise InvalidArgument(f"the annotations of {fn!r} do not resolve: {e}") from None
    params = [
        p for p in signature.parameters.values() if p.kind not in (p.VAR_KEYWORD, p.KEYWORD_ONLY)
    ]
    if len(params) != 1:
        raise InvalidArgument(f"a handler takes one argument, the message's value: {fn!r}")
    if inspect.iscoroutinefunction(fn):
        raise InvalidArgument(f"a handler is a plain function, not async: {fn!r}")
    return _accepted(params[0].annotation, repr(fn))


def _is(value: Any, accepted: tuple[type, ...]) -> bool:
    if isinstance(value, bool):
        return bool in accepted
    if isinstance(value, int) and float in accepted:
        return True
    return isinstance(value, accepted)


def _decode_failure(m: run.Message, why: object) -> Failed:
    return Failed(Failure("MessageDecode", f"message {m.name!r} {m.id}: {why}"))


def _handler(fn: Callable[[Any], object]) -> Callable[[run.Message], None]:
    """A handler of messages calling `fn` with each value; a failure of either fails the run."""
    accepted = _parameter(fn)

    def handle(m: run.Message) -> None:
        try:
            value = decode_json(m.value if m.HasField("value") else None)
        except ValueError as e:
            raise _decode_failure(m, e) from None
        if accepted is not None and not _is(value, accepted):
            names = ", ".join(t.__name__ for t in accepted)
            raise _decode_failure(m, f"{value!r} is not of {names}")
        try:
            fn(value)
        except Exception as e:
            raise _decode_failure(m, f"{type(e).__name__}: {e}") from None

    return handle


def handle(shared: Shared, handlers: Mapping[str, Callable[[Any], object]]) -> None:
    """Binds every handler of `handlers` at once, so that the past messages of their names come in
    the order they arrived."""
    for name in handlers:
        if not isinstance(name, str):
            raise InvalidArgument(f"a message name is a string, not {name!r}")
    by_name = {name: _handler(fn) for name, fn in handlers.items()}
    shared.bind(list(by_name), lambda m: by_name[m.name](m))
