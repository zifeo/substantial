"""Client actions as plan machines (I-23): the host performs each store step until the outcome."""

from __future__ import annotations

import secrets
import threading
from collections.abc import AsyncGenerator, Callable
from typing import Any

from substantial._proto import plan
from substantial.core import Core
from substantial.errors import CoreError, InvalidArgument, ProtocolError
from substantial.machines import PlanMachine
from substantial.observe import ObserverRaised
from substantial.stores import Store, perform


async def plan_steps(
    core: Core, store: Store, data: plan.PlanInput
) -> AsyncGenerator[tuple[str, object]]:
    """The outcomes of a plan, each step that is not a store operation; the plan goes on when
    the next one is asked, and is dropped when the iteration ends.

    An `INVALID` answer, or input refused, is the caller's argument, its names, ids and budget,
    not a bug of the host. An observer that raises fails the plan with its error.
    """
    machine: PlanMachine | None = None
    try:
        machine = PlanMachine(core, data)
        while True:
            step = machine.next()
            which = step.WhichOneof("step")
            if which == "store":
                machine.feed(await perform(store, step.store))
                continue
            if which is None:
                raise ProtocolError("empty plan step")
            yield which, getattr(step, which)
    except CoreError as e:
        if e.kind == "INVALID":
            raise InvalidArgument(e.message) from None
        raise
    except ObserverRaised as e:
        raise e.error from None
    finally:
        if machine is not None:
            machine.close()


async def run_plan(core: Core, store: Store, data: plan.PlanInput) -> tuple[str, object]:
    """Runs a plan to its outcome, the first step that is not a store operation."""
    steps = plan_steps(core, store, data)
    try:
        return await anext(steps)
    finally:
        await steps.aclose()


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


# § one cq034wpjp@hh6rr
_CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz"


class Ulids:
    """ULIDs: 48 bits of milliseconds then 80 random bits from `bits`, in lowercase Crockford
    base32, as run and message ids are lowercase (I-17).

    Monotonic: one made in the millisecond of the last, or before it, is the last plus one, so a
    client's messages are consumed in the order sent."""

    def __init__(self, bits: Callable[[int], int]) -> None:
        self._bits = bits
        self._lock = threading.Lock()
        self._last = 0

    def __call__(self, ms: int) -> str:
        if ms < 0:
            raise ValueError("a time after 1970")
        with self._lock:
            value = ((ms & ((1 << 48) - 1)) << 80) | self._bits(ms)
            # § wtwx1e769.b@php44
            if value >> 80 <= self._last >> 80:
                value = self._last + 1
            if value >> 128:
                raise ValueError("ULIDs exhausted")
            self._last = value
            # § end wtwx1e769.b
        return "".join(_CROCKFORD[(value >> (i * 5)) & 31] for i in reversed(range(26)))


# The ULIDs of the process, monotonic across its clients.
# § one gpzydmca3.b@27519
ulid = Ulids(lambda _: secrets.randbits(80))
