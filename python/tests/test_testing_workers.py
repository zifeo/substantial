"""The test environment of docs/testing.md: workers and steps, crashes, pauses, holds, deploys."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import substantial as s

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)
# The PSP's charges and the ledger's ops, as the fakes of a shop would record them.
charges: list[str] = []
applied: list[str] = []
in_flight = [0, 0]


@pytest.fixture(autouse=True)
def fakes() -> None:
    charges.clear()
    applied.clear()
    in_flight[:] = [0, 0]


@pytest.fixture
async def env() -> AsyncIterator[s.testing.Env]:
    async with s.testing.Env.open(now=T0) as env:
        yield env
        await env.check()


async def charge(order: str) -> str:
    charges.append(order)
    return f"pay-{order}"


@s.workflow
async def checkout(c: s.Context, order: str) -> dict[str, str]:
    payment = await c.step("charge", charge, order)
    return {"payment": payment, "at": (await c.now()).isoformat()}


@s.workflow
async def retrying(c: s.Context, order: str) -> str:
    once = s.Retry(max_attempts=3, initial=s.seconds(1), max=s.seconds(1), multiplier=1)
    return await c.step("charge", charge, order, retry=once)


async def apply(op: str) -> None:
    in_flight[0] += 1
    in_flight[1] = max(in_flight)
    await asyncio.sleep(0)
    applied.append(op)
    in_flight[0] -= 1


@s.workflow
async def account(c: s.Context, customer: str) -> None:
    """An actor: applies each op it is sent, in order, and publishes the balance after it."""
    ops: list[dict[str, Any]] = []
    c.handle("op", ops.append)
    balance = 0
    while True:
        done = balance
        await c.wait_until(lambda done=done: len(ops) > done)
        await c.step("apply", apply, ops[done]["id"])
        balance += 1
        c.publish(f"op.{ops[done]['id']}", balance)


# The worker the next attempt of `abandoning` crashes from inside its body.
crashing: list[s.Worker] = []


@s.workflow
async def abandoning(c: s.Context, order: str) -> str:
    async def body(order: str) -> str:
        if crashing:
            crashing.pop().crash()
            # Interrupted here: the crash abandoned this attempt.
            await asyncio.sleep(30)
        return await charge(order)

    return await c.step("charge", body, order)


# The worker the next attempt of `pausing` pauses from inside its body.
pausing_: list[s.Worker] = []


@s.workflow
async def pausing(c: s.Context, order: str) -> str:
    async def body(order: str) -> str:
        pausing_.pop().pause()
        await asyncio.sleep(0)
        return await charge(order)

    return await c.step("charge", body, order)


@s.workflow
async def remind(c: s.Context, account: str) -> str:
    await c.sleep(s.days(30))
    return f"reminded {account}"


@s.workflow(name="remind", version=2)
async def remind_v2(c: s.Context, account: str) -> str:
    await c.sleep(s.days(30))
    return f"reminded {account}, v2"


async def test_a_crash_after_the_charge_before_it_is_recorded(env: s.testing.Env) -> None:
    async with (
        env.worker([checkout], preset="service"),
        env.worker([checkout], preset="service"),
    ):
        hold = env.hold("charge")
        run = await env.start(checkout, {"order": "a-42"})
        await env.settle()
        assert hold.held == 1
        hold.crash()
        assert hold.held == 0
        await env.advance(s.seconds(34))
        assert charges == ["a-42"]
        await env.advance(s.seconds(2))
        assert (await run.result())["payment"] == "pay-a-42"
        assert charges == ["a-42", "a-42"]


async def test_claims_are_renewed_and_taken_over_after_the_last_renewal(
    env: s.testing.Env,
) -> None:
    async with (
        env.worker([checkout], preset="service"),
        env.worker([checkout], preset="service"),
    ):
        hold = env.hold("charge")
        run = await env.start(checkout, {"order": "a-42"})
        # Renewed every 15 s, the claim outlives claim_ttl while its worker holds the step.
        await env.advance(s.seconds(61))
        assert charges == ["a-42"]
        hold.crash()
        # The last renewal was at 60 s: the run is taken over 35 s later.
        await env.advance(s.seconds(34))
        assert charges == ["a-42"]
        await env.advance(s.seconds(1))
        assert (await run.result())["at"] == "2030-01-07T09:01:35.001000+00:00"


async def test_a_paused_workers_step_body_goes_on_to_its_end(env: s.testing.Env) -> None:
    async with env.worker([pausing], preset="service") as worker:
        pausing_.append(worker)
        run = await env.start(pausing, {"order": "a-42"})
        await env.settle()
        assert charges == ["a-42"]
        assert await run.status() == "running"
        worker.resume()
        assert await run.result() == "pay-a-42"


async def test_a_crashed_workers_step_is_interrupted(env: s.testing.Env) -> None:
    async with env.worker([abandoning], preset="service") as first:
        crashing.append(first)
        run = await env.start(abandoning, {"order": "a-42"})
        started = time.perf_counter()
        await env.settle()
        assert time.perf_counter() - started < 5, "settle does not wait for an interrupted body"
        assert not first.alive and first.error is None
        assert charges == []
        async with env.worker([abandoning], preset="service"):
            await env.advance(s.minutes(1))
            assert await run.result() == "pay-a-42"
            assert charges == ["a-42"]


async def test_two_pods_on_one_entity_never_overlap(env: s.testing.Env) -> None:
    async with (
        env.worker([account], preset="service"),
        env.worker([account], preset="service"),
    ):
        run = await env.start(account, {"customer": "cus-1"}, id="customer-cus-1")
        for i in range(20):
            await run.send("op", {"id": f"op-{i:03}"}, id=f"op-{i:03}")
        await env.advance(s.minutes(1))
        assert await run.read("op.op-019") == 20
        assert applied == [f"op-{i:03}" for i in range(20)]
        assert in_flight[1] == 1


async def test_a_paused_pod_does_not_apply_an_op_twice(env: s.testing.Env) -> None:
    async with env.worker([account], preset="service") as paused:
        hold = env.hold("apply")
        run = await env.start(account, {"customer": "cus-1"}, id="customer-cus-1")
        await run.send("op", {"id": "op-1"}, id="op-1")
        await env.settle()
        assert hold.held == 1
        async with env.worker([account], preset="service"):
            paused.pause()
            await env.advance(s.seconds(65))
            assert await run.read("op.op-1") == 1
            paused.resume()
            hold.release()
            await env.settle()
            assert await run.read("op.op-1") == 1
            assert applied == ["op-1", "op-1"]


async def test_a_pause_shorter_than_the_claim_changes_nothing(env: s.testing.Env) -> None:
    async with env.worker([account], preset="service") as paused:
        hold = env.hold("apply")
        run = await env.start(account, {"customer": "cus-1"}, id="customer-cus-1")
        await run.send("op", {"id": "op-1"}, id="op-1")
        await env.settle()
        async with env.worker([account], preset="service"):
            paused.pause()
            await env.advance(s.seconds(20))
            paused.resume()
            hold.release()
            await env.advance(s.minutes(1))
            assert await run.read("op.op-1") == 1
            assert applied == ["op-1"]


async def test_a_shutdown_leaves_held_steps_and_its_runs_to_another_worker(
    env: s.testing.Env,
) -> None:
    async with env.worker([checkout], preset="service") as leaving:
        hold = env.hold("charge")
        run = await env.start(checkout, {"order": "a-42"})
        await env.settle()
        async with env.worker([checkout], preset="service"):
            await leaving.shutdown()
            assert hold.held == 0 and not leaving.alive
            # Its claims are deleted: the other worker takes the run at its next scan, at 30 s.
            await env.advance(s.seconds(30))
            assert await run.result() == {"payment": "pay-a-42", "at": "2030-01-07T09:00:30+00:00"}
            assert charges == ["a-42", "a-42"]


async def test_a_held_attempt_released_before_its_timeout_counts_once() -> None:
    for early, attempts in ((timedelta(milliseconds=1), 1), (timedelta(0), 2)):
        charges.clear()
        async with (
            s.testing.Env.open(now=T0) as env,
            env.worker([retrying], preset="service"),
        ):
            hold = env.hold("charge")
            run = await env.start(retrying, {"order": "a-42"})
            await env.advance(s.minutes(5) - early)
            # At 5:00 the attempt has timed out: the release comes too late.
            assert hold.held == (1 if early else 0)
            hold.release()
            await env.advance(s.seconds(1))
            assert await run.result() == "pay-a-42"
            assert len(charges) == attempts


async def test_a_hold_takes_the_next_attempts_and_undos(env: s.testing.Env) -> None:
    @s.workflow
    async def refunded(c: s.Context, order: str) -> str:
        async def refund(payment: str, order: str) -> None:
            charges.append(f"refund {payment}")

        async def ship(order: str) -> None:
            raise s.Permanent("no carrier")

        try:
            async with c.saga():
                await c.step("charge", charge, order, undo=refund)
                await c.step("ship", ship, order)
        except s.StepFailed:
            return "refunded"
        return "shipped"

    async with env.worker([refunded], preset="service"):
        charged, undone = env.hold("charge", times=2), env.hold("charge.undo")
        first = await env.start(refunded, {"order": "a"})
        second = await env.start(refunded, {"order": "b"})
        await env.settle()
        assert charged.held == 2 and undone.held == 0
        charged.release()
        await env.settle()
        assert undone.held == 1
        undone.release()
        assert await first.result() == await second.result() == "refunded"
        assert sorted(charges) == ["a", "b", "refund pay-a", "refund pay-b"]


async def test_a_deploy_mid_wait_keeps_old_runs_on_old_code(env: s.testing.Env) -> None:
    async with env.worker([remind], preset="service"):
        old = await env.start(remind, {"account": "acc-1"})
        await env.advance(s.days(10))
    async with env.worker([remind, remind_v2], preset="service"):
        new = await env.start(remind_v2, {"account": "acc-2"})
        await env.advance(s.days(30))
        assert await old.result() == "reminded acc-1"
        assert await new.result() == "reminded acc-2, v2"


async def test_a_cancel_landing_with_the_timer(env: s.testing.Env) -> None:
    async with env.worker([remind], preset="service"), env.client() as client:
        own = await env.start(remind, {"account": "acc-1"})
        other = await env.start(remind, {"account": "acc-2"})
        await env.advance(s.days(30) - timedelta(milliseconds=1))
        await own.cancel("account deleted")
        await client.run(remind, other.chain).cancel("account deleted")
        await env.advance(s.minutes(1))
        assert await own.status() == "cancelled"
        # The cancel from another pod reaches the store first; the replay at the timer sees it.
        assert await other.status() == "cancelled"


async def test_a_message_to_a_run_that_just_ended(env: s.testing.Env) -> None:
    async with env.worker([remind], preset="service"):
        run = await env.start(remind, {"account": "acc-1"})
        await env.advance(s.days(30))
        with pytest.raises(s.RunEnded):
            await run.send("upgraded", id="upgrade-acc-1")
        assert await run.result() == "reminded acc-1"


async def test_a_redelivered_webhook_applies_once(env: s.testing.Env) -> None:
    async with env.worker([account], preset="service"):
        run = await env.start(account, {"customer": "cus-1"}, id="customer-cus-1")
        again = await env.start(account, {"customer": "cus-1"}, id="customer-cus-1")
        assert run.chain == again.chain
        for _ in range(2):
            await again.send("op", {"id": "op-1"}, id="evt-1")
        await env.advance(s.minutes(1))
        assert applied == ["op-1"]


async def test_a_worker_reports_whether_it_is_alive(env: s.testing.Env) -> None:
    worker = env.worker([checkout], preset="service")
    assert not worker.alive and worker.error is None
    async with worker:
        assert worker.alive
    assert not worker.alive and worker.error is None


paused: list[s.Worker] = []


@s.workflow
async def napper(c: s.Context) -> str:
    await c.sleep(s.seconds(1))
    return "woke"


@s.workflow
async def resumer(c: s.Context) -> str:
    async def resume() -> str:
        paused[0].resume()
        return "resumed"

    return await c.step("resume", resume)


async def test_a_resume_during_a_settle_does_not_hang_it(env: s.testing.Env) -> None:
    async with (
        env.worker([napper], preset="service") as napping,
        env.worker([resumer], preset="service"),
    ):
        paused[:] = [napping]
        nap = await env.start(napper, id="nap")
        await env.settle()
        napping.pause()
        await env.advance(s.seconds(2))
        await env.start(resumer, id="res")
        # The step body resumes the paused worker while the settle runs.
        await asyncio.wait_for(env.settle(), 5)
        assert await nap.result() == "woke"
