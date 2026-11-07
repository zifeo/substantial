"""Worker settings (I-18): a preset names every setting, the only defaults there are. A worker
overrides only the public ones; the others keep their preset value."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from substantial.errors import ConfigError

KIB = 1024
MIB = 1024 * KIB
PRESETS = ("script", "service")
PUBLIC = (
    "concurrency",
    "max_idle",
    "shutdown_grace",
    "claim_ttl",
    "continue_as_new_after",
    "step_timeout",
)


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
    snapshot_every: int
    continue_as_new_after: ContinueAsNewAfter
    dedup_window: timedelta
    step_timeout: timedelta

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
            commit_every=timedelta(0),
            shutdown_grace=timedelta(seconds=25 if service else 5),
            claim_ttl=timedelta(seconds=30),
            claim_skew=timedelta(seconds=5),
            max_idle=max_idle,
            wait=timedelta(seconds=20),
            due_page=100,
            due_pages=10,
            snapshot_every=64,
            continue_as_new_after=ContinueAsNewAfter(segments=1000, bytes=8 * MIB),
            dedup_window=timedelta(days=7),
            step_timeout=timedelta(minutes=5),
        )

    def override(self, **settings: Any) -> Settings:
        """These settings with some public ones replaced; any other name or an invalid value is
        refused.

        `continue_as_new_after` takes a `ContinueAsNewAfter`, or `0` to compress at every
        boundary.
        """
        unknown = sorted(set(settings) - set(PUBLIC))
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
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            expected = {"int": int, "timedelta": timedelta}.get(str(f.type))
            if expected is int and not _is_int(value):
                raise ConfigError(f"{f.name} must be an int, not {value!r}")
            if expected is timedelta and not isinstance(value, timedelta):
                raise ConfigError(f"{f.name} must be a timedelta, not {value!r}")
            if expected is not None and value < expected(0):
                raise ConfigError(f"{f.name} must not be negative: {value!r}")
        for name in ("concurrency", "due_page", "due_pages", "snapshot_every"):
            if getattr(self, name) == 0:
                raise ConfigError(f"{name} must be positive")
        if self.due_page > 1000:
            raise ConfigError(f"due_page must be at most 1000, an S3 page: {self.due_page!r}")
        for name in ("claim_ttl", "max_idle", "step_timeout"):
            if getattr(self, name) == timedelta(0):
                raise ConfigError(f"{name} must be positive")
        if not timedelta(seconds=1) <= self.wait <= timedelta(seconds=20) or self.wait % timedelta(
            seconds=1
        ):
            raise ConfigError(f"wait must be whole seconds from 1 to 20: {self.wait!r}")
        c = self.continue_as_new_after
        if not isinstance(c, ContinueAsNewAfter):
            raise ConfigError(f"continue_as_new_after must be a ContinueAsNewAfter or 0, not {c!r}")
