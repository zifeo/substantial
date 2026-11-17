"""Durations (I-8) and the host's clock."""

from __future__ import annotations

import time
from datetime import timedelta

from substantial._proto import layout
from substantial.errors import InvalidArgument


def _number(n: float) -> float:
    if isinstance(n, bool) or not isinstance(n, int | float):
        raise InvalidArgument(f"a duration takes a number, not {n!r}")
    return n


def seconds(n: float) -> timedelta:
    return timedelta(seconds=_number(n))


def minutes(n: float) -> timedelta:
    return timedelta(minutes=_number(n))


def hours(n: float) -> timedelta:
    return timedelta(hours=_number(n))


def days(n: float) -> timedelta:
    return timedelta(days=_number(n))


def millis(d: timedelta) -> int:
    """A duration in whole milliseconds; a negative one is refused."""
    if not isinstance(d, timedelta):
        raise InvalidArgument(f"a duration is a timedelta, not {type(d).__name__}")
    if d < timedelta(0):
        raise InvalidArgument(f"a duration is not negative: {d}")
    return d // timedelta(milliseconds=1)


def now() -> layout.Time:
    return layout.Time(ms=time.time_ns() // 1_000_000)


def until(at: layout.Time, current: layout.Time) -> float:
    """How many seconds `at` lies ahead of `current`, zero when it has passed."""
    return max(0, at.ms - current.ms) / 1000
