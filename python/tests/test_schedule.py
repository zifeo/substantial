"""Schedules: the cron arithmetic against the vectors every host checks (`dev/cross/cron.json`),
and `c.schedule()` with `sub.schedule` on the test environment. The TypeScript and Rust hosts port
these tests under the same names."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import substantial as s
from substantial.schedule import config, iso, message_id, next_fire, parse, plan

VECTORS = json.loads((Path(__file__).parents[2] / "dev/cross/cron.json").read_text())
# A Monday.
T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)
HOURLY = s.Cron("0 * * * *", "UTC")


@pytest.mark.parametrize("v", VECTORS["next"], ids=lambda v: f"{v['cron']} {v['tz']}")
def test_fire_times_match_the_shared_vectors(v: dict[str, Any]) -> None:
    cron, t, fires = s.Cron(v["cron"], v["tz"]), parse(v["after"]), []
    for _ in v["fires"]:
        t = next_fire(cron, t)
        fires.append(iso(t))
    assert fires == v["fires"]


@pytest.mark.parametrize("v", VECTORS["invalid"], ids=lambda v: f"{v['cron']!r} {v['tz']}")
def test_invalid_crons_and_zones_are_refused(v: dict[str, Any]) -> None:
    with pytest.raises(s.InvalidArgument):
        s.Cron(v["cron"], v["tz"])


@pytest.mark.parametrize("v", VECTORS["plan"], ids=lambda v: f"{v['cron']} {v['now']}")
def test_plans_of_missed_fires_match_the_shared_vectors(v: dict[str, Any]) -> None:
    assert plan(v["cron"], v["tz"], v["after"], v["now"], v["backfill"]) == v["result"]


fired: list[str] = []


def record(at: str) -> None:
    fired.append(at)


@s.workflow
async def ticker(c: s.Context) -> None:
    async for at in c.schedule():
        await c.step("tick", record, at.isoformat())


@pytest.fixture
async def env() -> AsyncIterator[s.testing.Env]:
    fired.clear()
    async with s.testing.Env.open(now=T0) as env:
        yield env
        await env.check()


def hours(*hs: int) -> list[str]:
    return [T0.replace(hour=h).isoformat() for h in hs]


async def test_a_schedule_fires_at_each_cron_time(env: s.testing.Env) -> None:
    async with env.worker([ticker], preset="service"):
        run = await env.schedule(ticker, "tick", HOURLY, revision=1)
        await env.advance(s.hours(3))
        assert fired == hours(10, 11, 12)
        assert await run.read("schedule") == {
            "revision": 1,
            "cron": "0 * * * *",
            "tz": "UTC",
            "backfill": 5,
            "last": "2030-01-07T12:00:00.000Z",
            "recent": [
                "2030-01-07T10:00:00.000Z",
                "2030-01-07T11:00:00.000Z",
                "2030-01-07T12:00:00.000Z",
            ],
            "skipped": {"count": 0, "oldest": None, "newest": None},
            "next": "2030-01-07T13:00:00.000Z",
        }


async def test_missed_fires_are_backfilled_oldest_first_up_to_backfill(env: s.testing.Env) -> None:
    async with env.worker([ticker], preset="service"):
        run = await env.schedule(ticker, "tick", HOURLY, revision=1)
        await env.settle()
    await env.advance(s.hours(8))
    assert fired == []
    async with env.worker([ticker], preset="service"):
        await env.settle()
    assert fired == hours(13, 14, 15, 16, 17)
    published = await run.read("schedule")
    assert published["skipped"] == {
        "count": 3,
        "oldest": "2030-01-07T10:00:00.000Z",
        "newest": "2030-01-07T12:00:00.000Z",
    }
    assert published["next"] == "2030-01-07T18:00:00.000Z"


async def test_backfill_is_part_of_the_config(env: s.testing.Env) -> None:
    async with env.worker([ticker], preset="service"):
        await env.schedule(ticker, "tick", HOURLY, revision=1, backfill=2)
        await env.settle()
    await env.advance(s.hours(4))
    async with env.worker([ticker], preset="service"):
        await env.settle()
    assert fired == hours(12, 13)


async def test_a_config_applies_only_if_its_revision_is_higher(env: s.testing.Env) -> None:
    async with env.worker([ticker], preset="service"):
        run = await env.schedule(ticker, "tick", HOURLY, revision=2)
        await env.settle()
        stale = {"revision": 1, "cron": "*/10 * * * *", "tz": "UTC", "backfill": 5}
        await run.send("schedule", stale)
        await env.advance(s.hours(1))
        assert fired == hours(10)
        assert (await run.read("schedule"))["revision"] == 2


async def test_sub_schedule_does_nothing_twice_and_refuses_a_stale_or_another_config(
    env: s.testing.Env,
) -> None:
    async with env.worker([ticker], preset="service"):
        await env.schedule(ticker, "tick", HOURLY, revision=1)
        await env.settle()
        await env.schedule(ticker, "tick", HOURLY, revision=1)
        with pytest.raises(s.ConfigError, match="revision 1 is applied with another config"):
            await env.schedule(ticker, "tick", s.Cron("0 * * * *", "Europe/Zurich"), revision=1)
        await env.schedule(ticker, "tick", s.Cron("30 * * * *", "UTC"), revision=2)
        await env.settle()
        with pytest.raises(s.ConfigError, match="revision 2 is applied already, higher than 1"):
            await env.schedule(ticker, "tick", HOURLY, revision=1)
        await env.advance(s.hours(1))
        assert fired == [T0.replace(minute=30).isoformat()]


async def test_a_malformed_schedule_message_blocks_the_run(env: s.testing.Env) -> None:
    run = await env.schedule(ticker, "tick", HOURLY, revision=1)
    await run.send("schedule", {"revision": 2, "cron": "0 * * * *"})
    async with env.worker([ticker], preset="service"):
        await env.settle()
    with pytest.raises(
        s.testing.Stuck, match="blocked by InvalidArgument: a schedule message is a config"
    ):
        await run.result()


def test_message_ids_are_the_same_in_every_host() -> None:
    hourly = s.Cron("0 * * * *", "UTC")
    assert message_id(config(1, hourly, 5)) == "schedule-1-0a984120fb588a41"
    assert message_id(config(2, None, 1)) == "schedule-2-1669c891556cdd13"


async def test_two_configs_of_one_revision_block_the_run(env: s.testing.Env) -> None:
    await env.schedule(ticker, "tick", HOURLY, revision=1)
    run = await env.schedule(ticker, "tick", s.Cron("30 * * * *", "UTC"), revision=1)
    async with env.worker([ticker], preset="service"):
        await env.settle()
    with pytest.raises(
        s.testing.Stuck, match="blocked by ConfigError: schedule revision 1 came with two configs"
    ):
        await run.result()


async def test_none_disables_a_schedule_and_a_new_cron_fires_from_when_it_applies(
    env: s.testing.Env,
) -> None:
    async with env.worker([ticker], preset="service"):
        run = await env.schedule(ticker, "tick", HOURLY, revision=1)
        await env.advance(s.hours(1))
        await env.schedule(ticker, "tick", None, revision=2)
        await env.advance(s.hours(3))
        assert fired == hours(10)
        assert (await run.read("schedule"))["cron"] is None
        assert await run.status() == "suspended"
        await env.schedule(ticker, "tick", HOURLY, revision=3)
        await env.advance(s.hours(1))
        assert fired == hours(10, 14)


async def test_a_schedule_crosses_compressions(env: s.testing.Env) -> None:
    async with env.worker([ticker], preset="service", continue_as_new_after=0):
        run = await env.schedule(ticker, "tick", HOURLY, revision=1)
        await env.advance(s.hours(5))
        await env.schedule(ticker, "tick", HOURLY, revision=2, backfill=1)
        await env.advance(s.hours(2))
        assert fired == hours(10, 11, 12, 13, 14, 15, 16)
        published = await run.read("schedule")
        assert (published["revision"], published["backfill"], len(published["recent"])) == (2, 1, 7)


async def test_the_schedule_step_name_is_reserved(env: s.testing.Env) -> None:
    with pytest.raises(s.InvalidArgument, match="reserved"):

        @s.step(name="schedule.next")
        def taken() -> None: ...

    @s.workflow
    async def inline(c: s.Context) -> None:
        await c.step("schedule.next", lambda: None)

    async with env.worker([inline], preset="service"):
        run = await env.start(inline)
        with pytest.raises(s.testing.Stuck, match=r"blocked by InvalidArgument: .*reserved"):
            await run.result()


async def test_sub_schedule_checks_its_arguments(env: s.testing.Env) -> None:
    for kwargs in ({"revision": 0}, {"revision": 1, "backfill": 0}, {"revision": True}):
        with pytest.raises(s.InvalidArgument):
            await env.schedule(ticker, "tick", HOURLY, **kwargs)  # type: ignore[arg-type]
    with pytest.raises(s.InvalidArgument, match=r"s\.Cron"):
        await env.schedule(ticker, "tick", "0 * * * *", revision=1)  # type: ignore[arg-type]


async def test_a_config_publishes_its_next_fire_at_once(env: s.testing.Env) -> None:
    async with env.worker([ticker], preset="service"):
        run = await env.schedule(ticker, "tick", HOURLY, revision=1)
        await env.settle()
        published = await run.read("schedule")
        assert (published["next"], published["last"]) == ("2030-01-07T10:00:00.000Z", None)
        await env.schedule(ticker, "tick", None, revision=2)
        await env.settle()
        assert (await run.read("schedule"))["next"] is None
