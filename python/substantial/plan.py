"""Client actions as plan machines (I-23): the host performs each store step until the outcome."""

from __future__ import annotations

import secrets
import threading
from typing import Any

from substantial._proto import plan
from substantial.core import Core
from substantial.errors import CoreError, InvalidArgument, ProtocolError
from substantial.machines import PlanMachine
from substantial.stores import Store, perform


async def run_plan(core: Core, store: Store, data: plan.PlanInput) -> tuple[str, object]:
    """Runs a plan to its outcome, the first step that is not a store operation.

    An `INVALID` answer is the caller's argument, its names and ids, not a bug of the host.
    """
    machine = PlanMachine(core, data)
    try:
        while True:
            step = machine.next()
            which = step.WhichOneof("step")
            if which == "store":
                machine.feed(await perform(store, step.store))
                continue
            if which is None:
                raise ProtocolError("empty plan step")
            return which, getattr(step, which)
    except CoreError as e:
        if e.kind == "INVALID":
            raise InvalidArgument(e.message) from None
        raise
    finally:
        machine.close()


def unexpected(name: str, which: str, outcome: object) -> ProtocolError:
    return ProtocolError(f"{name} plan answered {which}: {outcome}")


def outcome_of(name: str, which: str, outcome: Any, expected: str) -> tuple[str, Any]:
    """The case of a plan's outcome, which must be of kind `expected`."""
    if which != expected:
        raise unexpected(name, which, outcome)
    oneof = outcome.DESCRIPTOR.oneofs[0].name
    case = outcome.WhichOneof(oneof)
    if case is None:
        raise unexpected(name, which, outcome)
    return case, getattr(outcome, case)


_CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz"
_LOCK = threading.Lock()
_last = 0


def ulid(ms: int) -> str:
    """A ULID: 48 bits of milliseconds then 80 random bits, in lowercase Crockford base32, as run
    and message ids are lowercase (I-17).

    Monotonic in the process: one made in the millisecond of the last, or before it, is the last
    plus one, so a client's messages are consumed in the order sent."""
    global _last
    if ms < 0:
        raise ValueError("a time after 1970")
    with _LOCK:
        value = ((ms & ((1 << 48) - 1)) << 80) | secrets.randbits(80)
        if value >> 80 <= _last >> 80:
            value = _last + 1
        if value >> 128:
            raise ValueError("ULIDs exhausted")
        _last = value
    return "".join(_CROCKFORD[(value >> (i * 5)) & 31] for i in reversed(range(26)))
