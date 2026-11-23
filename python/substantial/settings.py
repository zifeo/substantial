"""Worker settings (I-18): a preset names every setting, the only defaults there are. A worker
overrides only the public ones; the others keep their preset value."""

from __future__ import annotations

import dataclasses
import math
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from substantial.errors import ConfigError
from substantial.stores.base import EXPIRY_MAX_DAYS

KIB = 1024
MIB = 1024 * KIB
PRESETS = ("script", "service")
# § yxy5pzfkp@4w1p8
PUBLIC = (
    "concurrency",
    "max_idle",
    "shutdown_grace",
    "claim_ttl",
    "continue_as_new_after",
    "step_timeout",
    "retention",
)
# § end yxy5pzfkp
# The longest retention, 100 years of days, the longest expiry a store write carries.
# § one pnctq6tx8@0npxx
RETENTION_MAX_DAYS = EXPIRY_MAX_DAYS


def check_retention(days: object, what: str) -> None:
    """Refuses a retention, how many whole days an ended run is kept (I-16), under 1 or over
    36,500."""
    # § pnctq6tx8@2j3nt
    if not _is_int(days) or not 1 <= days <= RETENTION_MAX_DAYS:
        raise ConfigError(f"{what} must be whole days from 1 to {RETENTION_MAX_DAYS}, not {days!r}")


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class ContinueAsNewAfter:
    """The compression threshold: whichever of the two the history passes first."""

    segments: int
    bytes: int

    def __post_init__(self) -> None:
        segments, size = self.segments, self.bytes
        if not (_is_int(segments) and _is_int(size)) or segments < 0 or size < 0:
            raise ConfigError(f"invalid continue_as_new_after {self!r}")


@dataclass(frozen=True)
class Settings:
    concurrency: int
    commit_every: timedelta
    shutdown_grace: timedelta
    claim_ttl: timedelta
    claim_skew: timedelta
    max_idle: timedelta
    wait: timedelta
    due_page: int
    due_pages: int
    continue_as_new_after: ContinueAsNewAfter
    dedup_window: timedelta
    step_timeout: timedelta
    # Whole days.
    retention: int
    blocked_initial: timedelta
    blocked_max: timedelta
    blocked_multiplier: float
    max_history_bytes: int
    max_history_events: int
    max_replay_inbox: int
    max_seen: int
    history_warn_percent: int

    @staticmethod
    def preset(preset: str, *, waiting_store: bool) -> Settings:
        """Every setting of `preset`; `waiting_store` is whether the store's listings can wait."""
        if preset not in PRESETS:
            raise ConfigError(f"unknown preset {preset!r}, expected one of {PRESETS}")
        service = preset == "service"
        if not service:
            max_idle = timedelta(seconds=1)
        else:
            max_idle = timedelta(minutes=5) if waiting_store else timedelta(seconds=30)
        return Settings(
            concurrency=16 if service else 1,
            # § one ry26kkc7v.a@v7cjr
            commit_every=timedelta(0),
            shutdown_grace=timedelta(seconds=25 if service else 5),
            claim_ttl=timedelta(seconds=30),
            # § one ry26kkc7v.b@px706
            claim_skew=timedelta(seconds=5),
            max_idle=max_idle,
            wait=timedelta(seconds=20),
            # § one ry26kkc7v.e@5h1jf
            due_page=1000,
            due_pages=10,
            continue_as_new_after=ContinueAsNewAfter(segments=1000, bytes=8 * MIB),
            # § one ry26kkc7v.c@0zaax
            dedup_window=timedelta(days=7),
            step_timeout=timedelta(minutes=5),
            retention=30,
            # § ksmvx2mfk@bsjhy
            blocked_initial=timedelta(seconds=10 if service else 1),
            blocked_max=timedelta(minutes=10 if service else 1),
            blocked_multiplier=2.0,
            # § end ksmvx2mfk
            # § one ry26kkc7v.f@zxs2g
            max_history_bytes=128 * MIB,
            # § one ry26kkc7v.g@fhr48
            max_history_events=100_000,
            # § one ry26kkc7v.h@sz1ag
            max_replay_inbox=10_000,
            # § one ry26kkc7v.i@mvxr4
            max_seen=100_000,
            # § one ry26kkc7v.j@fh0n8
            history_warn_percent=80,
        )

    def override(self, **settings: Any) -> Settings:
        """These settings with some public ones replaced; any other name or an invalid value is
        refused.

        `continue_as_new_after` takes a `ContinueAsNewAfter`, or `0` to compress at every
        boundary; `retention` whole days, an `int`.
        """
        unknown = sorted(set(settings) - set(PUBLIC))
        # § one yxy5pzfkp.a@944qs, yxy5pzfkp.b@02f8w
        if unknown:
            raise ConfigError(f"unknown settings {unknown}")
        if settings.get("continue_as_new_after") == 0 and _is_int(
            settings["continue_as_new_after"]
        ):
            settings["continue_as_new_after"] = ContinueAsNewAfter(segments=0, bytes=0)
        replaced = dataclasses.replace(self, **settings)
        replaced.validate()
        return replaced

    def validate(self) -> None:
        check_retention(self.retention, "retention")
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            expected = {"int": int, "timedelta": timedelta}.get(str(f.type))
            if expected is int and not _is_int(value):
                raise ConfigError(f"{f.name} must be an int, not {value!r}")
            if expected is timedelta and not isinstance(value, timedelta):
                raise ConfigError(f"{f.name} must be a timedelta, not {value!r}")
            if expected is not None and value < expected(0):
                raise ConfigError(f"{f.name} must not be negative: {value!r}")
        for name in (
            "concurrency",
            "due_page",
            "due_pages",
            "max_history_bytes",
            "max_history_events",
            "max_replay_inbox",
            "max_seen",
        ):
            if getattr(self, name) == 0:
                raise ConfigError(f"{name} must be positive")
        if self.due_page > 1000:
            raise ConfigError(f"due_page must be at most 1000, an S3 page: {self.due_page!r}")
        for name in (
            "claim_ttl",
            "max_idle",
            "step_timeout",
            "blocked_initial",
        ):
            if getattr(self, name) == timedelta(0):
                raise ConfigError(f"{name} must be positive")
        if self.blocked_max < self.blocked_initial:
            raise ConfigError(f"blocked_max must be at least blocked_initial: {self.blocked_max!r}")
        m = self.blocked_multiplier
        if isinstance(m, bool) or not isinstance(m, int | float) or not math.isfinite(m) or m < 1:
            raise ConfigError(f"blocked_multiplier must be a number of at least 1, not {m!r}")
        p = self.history_warn_percent
        if not _is_int(p) or not 1 <= p <= 100:
            raise ConfigError(f"history_warn_percent must be an int from 1 to 100, not {p!r}")
        if not timedelta(seconds=1) <= self.wait <= timedelta(seconds=20) or self.wait % timedelta(
            seconds=1
        ):
            raise ConfigError(f"wait must be whole seconds from 1 to 20: {self.wait!r}")
        c = self.continue_as_new_after
        if not isinstance(c, ContinueAsNewAfter):
            raise ConfigError(f"continue_as_new_after must be a ContinueAsNewAfter or 0, not {c!r}")
