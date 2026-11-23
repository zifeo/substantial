"""`Retry(deadline=)`: no attempt of a step starts past its deadline, counted from its first
attempt's start. The TypeScript and Rust hosts port these tests under the same names."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

import substantial as s

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)


@s.step(retry=s.Retry(10, s.seconds(1), s.seconds(1), 1, deadline=timedelta(milliseconds=2500)))
async def flaky() -> None:
    raise ConnectionError("down")


@s.workflow
async def calling(c: s.Context) -> str:
    try:
        await flaky()
    except s.StepFailed as e:
        return e.failure.message
    return "ok"


async def test_no_attempt_starts_past_the_deadline() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([calling], preset="script"),
    ):
        run = await env.start(calling)
        await env.advance(s.seconds(10))
        # § s0a5x1qed@r1898
        assert "after 3 attempts" in await run.result()
        retries = [e.data for e in await run.history() if e.kind == "step_retry"]
        assert len(retries) == 2 and all("deadline" in r for r in retries), retries
        # § end s0a5x1qed


def test_a_deadline_is_a_positive_duration() -> None:
    for deadline in (timedelta(0), -s.seconds(1), 5):
        # § s0a5x1qed.a@rcfxs
        with pytest.raises(s.InvalidArgument):
            s.Retry(3, s.seconds(1), s.seconds(1), 1, deadline=deadline)  # type: ignore[arg-type]
        # § end s0a5x1qed.a
