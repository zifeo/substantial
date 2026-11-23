"""A run's history limits: passing one ends the run failed with `HistoryTooLarge`, and a commit
past their warning share says so once. The TypeScript and Rust hosts port these tests under the
same names."""

from __future__ import annotations

import logging
import secrets

import pytest
from conftest import LONG, script

import substantial as s
from substantial.stores.memory import MemoryStore


@s.workflow
async def clocking(c: s.Context) -> str:
    try:
        for _ in range(100):
            await c.now()
    except s.HistoryTooLarge:
        return "caught"
    return "done"


async def test_a_run_past_a_limit_of_its_history_ends_failed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    told: list[s.Observed] = []
    caplog.set_level(logging.WARNING, logger="substantial")
    store = MemoryStore(f"limits-{secrets.token_hex(4)}")
    sub = await s.Substantial._create(store, observe=told.append)
    async with script(sub, [clocking], max_history_events=10, history_warn_percent=50):
        run = await sub.start(clocking, id="clock")
        # § vkpvtd54g@xn639
        with pytest.raises(s.RunFailed) as failed:
            await run.result(timeout=LONG)
        error = failed.value.error
        assert error.type == "HistoryTooLarge", error
        # § end vkpvtd54g
        # § vkpvtd54g.b@98nxs
        assert error.details == {"limit": "history_events", "used": "11", "max": "10"}
        assert not error.retryable
        # § end vkpvtd54g.b
    # Told once, by the commit that took the run past half of it, its end's here.
    near = [e for e in told if isinstance(e, s.NearLimit)]
    assert near == [s.NearLimit("clocking", "clock", "history_events", 12, 10)], near
    assert "run clock near its history_events limit: 12 of 10" in caplog.text


@s.workflow
async def taking(c: s.Context) -> int:
    got: list[int] = []
    c.handle("n", got.append)
    await c.wait_until(lambda: len(got) >= 5)
    return len(got)


async def test_a_replay_past_its_inbox_limit_ends_failed() -> None:
    sub = await s.Substantial._create(MemoryStore(f"inbox-{secrets.token_hex(4)}"))
    run = await sub.start(taking)
    for n in range(5):
        await run.send("n", n)
    async with script(sub, [taking], max_replay_inbox=3):
        with pytest.raises(s.RunFailed) as failed:
            await run.result(timeout=LONG)
    assert failed.value.error.details is not None
    assert failed.value.error.details["limit"] == "replay_inbox", failed.value.error
