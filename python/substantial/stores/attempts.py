"""The requests an adapter sends for one store operation, its retries and read-backs included,
which the core counts for runs and the observer is told."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar


class Sent:
    """The requests of the operation being measured."""

    def __init__(self) -> None:
        self.n = 0


_SENT: ContextVar[Sent | None] = ContextVar("substantial.sent", default=None)


def sent() -> None:
    """Counts one request of the operation being measured; called by the adapters' shared retry
    layer for each attempt it makes."""
    measuring = _SENT.get()
    if measuring is not None:
        measuring.n += 1


@contextmanager
def measured() -> Iterator[Sent]:
    """Measures the store operation performed inside, failed or not. An adapter outside the shared
    retry layer, the memory store or a test's, sends each operation once. An operation measured
    inside another, through a store wrapping a store, counts in both."""
    # § zemxe46k3.a@wspab
    measuring = Sent()
    token = _SENT.set(measuring)
    try:
        yield measuring
    finally:
        _SENT.reset(token)
        measuring.n = max(measuring.n, 1)
        outer = _SENT.get()
        if outer is not None:
            outer.n += measuring.n
