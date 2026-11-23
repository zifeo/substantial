"""Schedules: `s.Cron`, the fire times of a cron expression in a time zone, and `c.schedule()`,
the loop that fires them, configured by the `schedule` message `sub.schedule` sends.

The cron arithmetic is the library's own, the same in every host: a local time a clock change
skips fires at the end of the gap, and a local time it repeats fires at its first occurrence.
"""

from __future__ import annotations

import functools
import hashlib
import json
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, available_timezones

import rfc8785

from substantial.errors import ConfigError, InvalidArgument, ProtocolError
from substantial.names import RESERVED_STEP as NEXT
from substantial.step import call_step

if TYPE_CHECKING:
    from substantial.context import Context

# The message that carries a schedule's config.
MESSAGE = "schedule"
# How many fired instants the published `schedule` value keeps.
# § one ey8zhz0kq.d@g9gcf
RECENT = 10

_MINUTE = 60_000
_DAY = 86_400_000
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_NAIVE_EPOCH = datetime(1970, 1, 1)
# A cron expression none of whose fires comes within this many years never fires.
_YEARS = 8

_MONTHS = {
    m: i + 1
    for i, m in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
    )
}
_DAYS = {d: i for i, d in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"])}
_FIELDS = (
    ("minute", 0, 59, {}),
    ("hour", 0, 23, {}),
    ("day of month", 1, 31, {}),
    ("month", 1, 12, _MONTHS),
    ("day of week", 0, 7, _DAYS),
)
_NUMBER = re.compile(r"[0-9]+")


def _value(expr: str, field: str, text: str, names: dict[str, int]) -> int:
    if _NUMBER.fullmatch(text):
        return int(text)
    if text.lower() in names:
        return names[text.lower()]
    raise InvalidArgument(f"cron {expr!r}: {text!r} is not a {field}")


def _field(expr: str, text: str, field: str, lo: int, hi: int, names: dict[str, int]) -> set[int]:
    values: set[int] = set()
    for item in text.split(","):
        span, slash, step_text = item.partition("/")
        step = 1
        if slash:
            if not _NUMBER.fullmatch(step_text) or int(step_text) == 0:
                raise InvalidArgument(f"cron {expr!r}: the step {step_text!r} is not positive")
            step = int(step_text)
        if span == "*":
            a, b = lo, hi
        elif "-" in span:
            first, _, last = span.partition("-")
            a, b = _value(expr, field, first, names), _value(expr, field, last, names)
        else:
            a = _value(expr, field, span, names)
            b = hi if slash else a
        if not lo <= a <= b <= hi:
            raise InvalidArgument(f"cron {expr!r}: {item!r} is not a {field} range of {lo}-{hi}")
        values.update(range(a, b + 1, step))
    return values


@functools.cache
def _zones() -> frozenset[str]:
    return frozenset(available_timezones())


def _zone(tz: str) -> ZoneInfo:
    # A case-insensitive file system (macOS) opens `europe/zurich`, which Linux and the other
    # hosts refuse.
    if tz not in _zones():
        raise InvalidArgument(f"{tz!r} is not a time zone of the IANA database")
    try:
        return ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError):
        raise InvalidArgument(f"{tz!r} is not a time zone of the IANA database") from None


@dataclass(frozen=True)
class Cron:
    """A cron expression of five fields (minute, hour, day of month, month, day of week) and the
    IANA time zone it reads its times in: `s.Cron("0 3 * * *", "Europe/Zurich")`."""

    expr: str
    tz: str

    def __post_init__(self) -> None:
        if not isinstance(self.expr, str) or not isinstance(self.tz, str):
            raise InvalidArgument(f"a cron takes an expression and a time zone, strings: {self!r}")
        _compile(self.expr, self.tz)


@dataclass(frozen=True)
class _Compiled:
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]
    # Vixie cron: when either day field starts with `*`, a day matches both, otherwise either.
    either: bool
    zone: ZoneInfo

    def day(self, w: datetime) -> bool:
        # § d08sxcdv8.a@55a8b
        dom = w.day in self.days
        dow = (w.isoweekday() % 7) in self.weekdays
        return (dom or dow) if self.either else (dom and dow)


def _compile(expr: str, tz: str) -> _Compiled:
    fields = expr.split()
    if len(fields) != 5:
        raise InvalidArgument(
            f"cron {expr!r} has {len(fields)} fields, not 5: minute hour day-of-month month "
            "day-of-week"
        )
    sets = [_field(expr, t, *spec) for t, spec in zip(fields, _FIELDS, strict=True)]
    weekdays = frozenset(d % 7 for d in sets[4])
    # § d08sxcdv8.a@9zfdw
    either = not fields[2].startswith("*") and not fields[4].startswith("*")
    # § end d08sxcdv8.a
    minutes, hours, days, months = (frozenset(v) for v in sets[:4])
    c = _Compiled(minutes, hours, days, months, weekdays, either, _zone(tz))
    _next_wall(c, datetime(2000, 1, 1), expr)
    return c


def _next_wall(c: _Compiled, w: datetime, expr: str) -> datetime:
    """The first local time at or after `w`, naive and on the minute, that matches."""
    limit = w.year + _YEARS
    while w.year <= limit:
        if w.month not in c.months:
            w = datetime(w.year + w.month // 12, w.month % 12 + 1, 1)
        elif not c.day(w):
            w = datetime(w.year, w.month, w.day) + timedelta(days=1)
        elif w.hour not in c.hours:
            w = datetime(w.year, w.month, w.day, w.hour) + timedelta(hours=1)
        elif w.minute not in c.minutes:
            w = w.replace(second=0, microsecond=0) + timedelta(minutes=1)
        else:
            return w
    raise InvalidArgument(f"cron {expr!r} never fires")


def _offset(zone: ZoneInfo, t: int) -> int:
    offset = (_EPOCH + timedelta(milliseconds=t)).astimezone(zone).utcoffset()
    assert offset is not None
    return offset // timedelta(milliseconds=1)


def _wall_ms(w: datetime) -> int:
    return (w - _NAIVE_EPOCH) // timedelta(milliseconds=1)


def _resolve(zone: ZoneInfo, w: int) -> int:
    """The instant of the local time `w` (ms, read as UTC): the earlier of two in a repeated
    hour, the end of the gap for a time a clock change skips."""
    # § d08sxcdv8@mxt82
    before, after = _offset(zone, w - _DAY), _offset(zone, w + _DAY)
    valid = [w - o for o in {before, after} if _offset(zone, w - o) == o]
    if valid:
        return min(valid)
    lo, hi = w - max(before, after), w - min(before, after)
    first = _offset(zone, lo)
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if _offset(zone, mid) == first:
            lo = mid
        else:
            hi = mid
    return hi


def next_fire(cron: Cron, after: int) -> int:
    """The first fire time of `cron` strictly after `after`, both in ms since the epoch."""
    c = _compile(cron.expr, cron.tz)
    return _next_fire(c, cron.expr, after)


def _next_fire(c: _Compiled, expr: str, after: int) -> int:
    # No local time before the earliest reading of `after` can resolve after it.
    # § d08sxcdv8@xerv6
    least = min(_offset(c.zone, after + d) for d in (-_DAY, 0, _DAY))
    w = _NAIVE_EPOCH + timedelta(milliseconds=(after + least) // _MINUTE * _MINUTE)
    while True:
        w = _next_wall(c, w, expr)
        t = _resolve(c.zone, _wall_ms(w))
        if t > after:
            return t
        w += timedelta(minutes=1)


def iso(ms: int) -> str:
    """The form of instants in a schedule's values: `2026-11-15T03:00:00.000Z`."""
    t = _EPOCH + timedelta(milliseconds=ms)
    return f"{t:%Y-%m-%dT%H:%M:%S}.{ms % 1000:03d}Z"


def parse(text: str) -> int:
    try:
        t = datetime.fromisoformat(text)
    except (TypeError, ValueError) as e:
        raise ProtocolError(f"a schedule instant {text!r}: {e}") from e
    return (t - _EPOCH) // timedelta(milliseconds=1)


def plan(expr: str, tz: str, after: str, now: str, backfill: int) -> dict[str, Any]:
    """The fires due by `now` after `after`, at most the `backfill` most recent, those before
    them skipped, and the next fire after `now`."""
    # § vnykycs34.d@tw1qx
    c = _compile(expr, tz)
    t, end = parse(after), parse(now)
    due: list[int] = []
    count, oldest, newest = 0, None, None
    while (t := _next_fire(c, expr, t)) <= end:
        due.append(t)
        if len(due) > backfill:
            newest = due.pop(0)
            oldest = newest if oldest is None else oldest
            count += 1
    skipped = {
        "count": count,
        "oldest": None if oldest is None else iso(oldest),
        "newest": None if newest is None else iso(newest),
    }
    return {"due": [iso(d) for d in due], "skipped": skipped, "next": iso(t)}


def config(revision: int, cron: Cron | None, backfill: int) -> dict[str, Any]:
    """The value of a `schedule` message."""
    return {
        "revision": revision,
        "cron": None if cron is None else cron.expr,
        "tz": None if cron is None else cron.tz,
        "backfill": backfill,
    }


def message_id(wanted: dict[str, Any]) -> str:
    """The id of a config's `schedule` message: its revision and a digest of its canonical JSON,
    so that a resend of it is delivered once and another config of the revision reaches the run."""
    digest = hashlib.sha256(rfc8785.dumps(wanted)).hexdigest()[:16]
    return f"schedule-{wanted['revision']}-{digest}"


def check_config(revision: object, cron: object, backfill: object) -> None:
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise InvalidArgument(f"a schedule's revision is a positive int, not {revision!r}")
    if cron is not None and not isinstance(cron, Cron):
        raise InvalidArgument(f"a schedule takes an s.Cron or None, not {cron!r}")
    if isinstance(backfill, bool) or not isinstance(backfill, int) or backfill < 1:
        raise InvalidArgument(f"backfill is a positive int, not {backfill!r}")


def _initial() -> dict[str, Any]:
    return {
        "revision": 0,
        "cron": None,
        "tz": None,
        "backfill": None,
        "after": None,
        "next": None,
        "pending": [],
        "last": None,
        "recent": [],
        "skipped": {"count": 0, "oldest": None, "newest": None},
    }


def _published(st: dict[str, Any]) -> dict[str, Any]:
    # § ey8zhz0kq.d@c232n
    keys = ("revision", "cron", "tz", "backfill", "last", "recent", "skipped")
    return {
        **{k: st[k] for k in keys},
        "next": st["pending"][0] if st["pending"] else st["next"],
    }


def _received(value: Any) -> tuple[int, Cron | None, int]:
    # § ey8zhz0kq.g@yh4ek
    if not isinstance(value, dict) or set(value) != {"revision", "cron", "tz", "backfill"}:
        raise InvalidArgument(f"a schedule message is a config of sub.schedule, not {value!r}")
    cron = None if value["cron"] is None else Cron(value["cron"], value["tz"])
    if cron is None and value["tz"] is not None:
        raise InvalidArgument(f"a schedule message without cron has no tz: {value!r}")
    check_config(value["revision"], cron, value["backfill"])
    return value["revision"], cron, value["backfill"]


def _flat(revision: int, cron: str | None, tz: str | None, backfill: int | None) -> str:
    return json.dumps([revision, cron, tz, backfill], separators=(",", ":"))


async def _apply(c: Context, st: dict[str, Any], configs: list[Any]) -> None:
    """Applies the config of the highest revision received, if higher than the state's."""
    received = [_received(v) for v in configs]
    configs.clear()
    # § ey8zhz0kq.e@n0ntt
    revision, cron, backfill = max(received, key=lambda r: r[0])
    # § end ey8zhz0kq.e
    # § ey8zhz0kq.h@j3yhf
    known: dict[int, str] = {}
    if st["revision"] > 0:
        known[st["revision"]] = _flat(st["revision"], st["cron"], st["tz"], st["backfill"])
    for r, cron_r, backfill_r in received:
        text = _flat(r, *((None, None) if cron_r is None else (cron_r.expr, cron_r.tz)), backfill_r)
        if known.get(r, text) != text:
            raise ConfigError(f"schedule revision {r} came with two configs: {known[r]} and {text}")
        known[r] = text
    # § end ey8zhz0kq.h
    # § one ey8zhz0kq@5y6t4
    if revision <= st["revision"]:
        return
    expr, tz = (None, None) if cron is None else (cron.expr, cron.tz)
    st["revision"], st["backfill"] = revision, backfill
    if (expr, tz) != (st["cron"], st["tz"]):
        # § vnykycs34.c@bg94z
        # Another cron starts from now: it fires nothing it would have fired before.
        st["cron"], st["tz"], st["pending"], st["next"] = expr, tz, [], None
        st["after"] = None if cron is None else iso(_ms(await c.now()))
        if cron is not None:
            # The plan that follows publishes it, with its next fire.
            return
    c.publish(MESSAGE, _published(st))


def _ms(t: datetime) -> int:
    return (t - _EPOCH) // timedelta(milliseconds=1)


async def schedule(c: Context, upgrade: bool) -> AsyncIterator[datetime]:
    """The loop of `c.schedule()`: its state is the loop's, its config the last `schedule`
    message of a higher revision."""
    configs: list[Any] = []
    c.handle(MESSAGE, configs.append)
    async for st in c.iterate(_initial(), upgrade=upgrade):
        while not st["pending"]:
            if configs:
                await _apply(c, st, configs)
                continue
            if st["cron"] is None:
                # § vnykycs34.c@57m4t
                await c.wait_until(lambda: bool(configs))
                continue
            now = _ms(await c.now())
            args = (st["cron"], st["tz"], st["after"], iso(now), st["backfill"])
            before = st["next"]
            # § vnykycs34.a@ytpk6
            fires = await call_step(c._shared, NEXT, plan, args, {}, None, None, None)
            # § end vnykycs34.a
            skipped = fires["skipped"]
            if fires["due"]:
                st["pending"], st["after"] = fires["due"], fires["due"][-1]
            if skipped["count"]:
                was = st["skipped"]
                st["skipped"] = {
                    "count": was["count"] + skipped["count"],
                    "oldest": was["oldest"] or skipped["oldest"],
                    "newest": skipped["newest"],
                }
            st["next"] = fires["next"]
            if not st["pending"]:
                if st["next"] != before:
                    # § ey8zhz0kq.d@p15j3
                    c.publish(MESSAGE, _published(st))
                wait = timedelta(milliseconds=parse(st["next"]) - now)
                await c.wait_until(lambda: bool(configs), timeout=wait)
        # § vnykycs34@p61hd
        at = st["pending"].pop(0)
        st["last"] = at
        # § ey8zhz0kq.d@4rpw0
        st["recent"] = [*st["recent"], at][-RECENT:]
        c.publish(MESSAGE, _published(st))
        # § end ey8zhz0kq.d
        yield _EPOCH + timedelta(milliseconds=parse(at))
        # § end vnykycs34
        # Before the loop's boundary: only its state crosses a compression.
        if configs:
            await _apply(c, st, configs)
