"""Presets and their overrides (I-18), validated strictly."""

import dataclasses
from datetime import timedelta

import pytest

from substantial import (
    FOREVER,
    ConfigError,
    ContinueAsNewAfter,
    Substantial,
    days,
    minutes,
    seconds,
    workflow,
)
from substantial.settings import Settings


def test_presets_differ_where_the_table_says() -> None:
    script = Settings.preset("script", waiting_store=False)
    service = Settings.preset("service", waiting_store=False)
    assert (script.concurrency, service.concurrency) == (1, 16)
    assert script.max_idle == seconds(1)
    assert service.max_idle == seconds(30)
    assert Settings.preset("service", waiting_store=True).max_idle == minutes(5)
    assert (script.shutdown_grace, service.shutdown_grace) == (seconds(5), seconds(25))
    assert script.continue_as_new_after == ContinueAsNewAfter(1000, 8 * 1024 * 1024)
    assert script.step_timeout == service.step_timeout == minutes(5)
    assert script.retention == service.retention == days(30)


def test_override_replaces_and_refuses() -> None:
    s = Settings.preset("script", waiting_store=False).override(
        concurrency=4,
        max_idle=seconds(2),
        shutdown_grace=seconds(3),
        claim_ttl=seconds(40),
        continue_as_new_after=0,
        step_timeout=seconds(30),
        retention=FOREVER,
    )
    assert s.step_timeout == seconds(30)
    assert s.retention is FOREVER
    assert s.override(retention=days(90)).retention == days(90)
    assert (s.concurrency, s.max_idle, s.shutdown_grace) == (4, seconds(2), seconds(3))
    assert s.claim_ttl == seconds(40)
    assert s.continue_as_new_after == ContinueAsNewAfter(0, 0)
    refused = [
        {"concurency": 4},
        {"inline_max": 1024},
        {"concurrency": 0},
        {"concurrency": True},
        {"concurrency": 1.5},
        {"claim_ttl": timedelta(0)},
        {"shutdown_grace": timedelta(seconds=-1)},
        {"step_timeout": timedelta(0)},
        {"step_timeout": 300},
        {"continue_as_new_after": 1},
        {"continue_as_new_after": (1, 2)},
        {"retention": minutes(59)},
        {"retention": None},
        {"retention": "forever"},
    ]
    for override in refused:
        with pytest.raises(ConfigError):
            s.override(**override)
    with pytest.raises(ConfigError):
        Settings.preset("daemon", waiting_store=False)


async def test_a_worker_refuses_the_settings_of_the_preset_only() -> None:
    @workflow
    async def example(c) -> None:
        return None

    sub = await Substantial.create("memory://settings-hidden", partitions=1)
    for name in [
        "commit_every",
        "claim_skew",
        "wait",
        "due_page",
        "due_pages",
        "snapshot_every",
        "dedup_window",
        "tombstone",
        "retention_min",
        "recheck",
        "snapshot_grace",
        "store_retry",
    ]:
        with pytest.raises(ConfigError, match=rf"^unknown settings \['{name}'\]$"):
            sub.worker([example], preset="service", **{name: seconds(1)})


def test_the_hidden_settings_are_still_checked() -> None:
    s = Settings.preset("script", waiting_store=False)
    for hidden in [
        {"due_page": 0},
        {"due_page": 1001},
        {"wait": timedelta(0)},
        {"wait": seconds(21)},
        {"wait": timedelta(milliseconds=1500)},
        {"tombstone": timedelta(0)},
        {"retention_min": days(31)},
    ]:
        with pytest.raises(ConfigError):
            dataclasses.replace(s, **hidden).validate()


def test_continue_as_new_after_is_checked_when_built() -> None:
    for args in [(-1, 0), (1, "8MiB")]:
        with pytest.raises(ConfigError):
            ContinueAsNewAfter(*args)  # type: ignore[arg-type]
