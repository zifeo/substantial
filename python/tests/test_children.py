"""Children (I-11), loops and continue-as-new (I-12), end to end."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from conftest import LONG, script

from substantial import (
    Context,
    InvalidArgument,
    Permanent,
    RunFailed,
    RunStatus,
    Substantial,
    step_key,
    workflow,
)
from substantial.child import Child
from substantial.stores import Store


def refuse() -> None:
    raise Permanent("zero")


@workflow
async def double(c: Context, n: int) -> int:
    if n == 0:
        await c.step("refuse", refuse)
    return n * 2


@workflow
async def listener(c: Context) -> str:
    return await c.receive("hurry")


@workflow
async def parent(c: Context) -> list[object]:
    ok = await c.start(double, {"n": 21}, id="kid-ok")
    failing = await c.start(double, {"n": 0})
    listening = await c.start(listener)
    await listening.send("hurry", "now")
    heard = await listening.result()
    await c.wait_until(lambda: ok.done and failing.done)
    value = await ok.result()
    try:
        await failing.result()
    except RunFailed as e:
        failed = e.error.type
    else:
        raise AssertionError("the failing child did not fail")
    return [value, failed, ok.chain, heard]


async def test_a_parent_awaits_its_children(store: Store) -> None:
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [parent, double, listener]):
        run = await sub.start(parent)
        assert await run.result(timeout=LONG) == [42, "StepFailed", "kid-ok", "now"]
        assert await sub.run(double, "kid-ok").result(timeout=LONG) == 42


@workflow
async def whoami(c: Context) -> str:
    return c.chain


@workflow
async def asker(c: Context) -> list[str]:
    kid = await c.start(whoami)
    return [c.chain, kid.chain, await kid.result()]


async def test_a_run_knows_its_chain_as_its_parent_and_clients_do(store: Store) -> None:
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [asker, whoami]):
        run = await sub.start(asker)
        mine, kid, answered = await run.result(timeout=LONG)
        assert mine == run.chain
        assert kid == answered != mine, "a derived id, the child's own chain"
        assert await sub.run(whoami, kid).result(timeout=LONG) == kid


@workflow
async def adopter(c: Context) -> str:
    kid = await c.start(double, {"n": 21}, id="taken")
    try:
        await kid.result()
    except RunFailed as e:
        assert e.status == RunStatus.FAILED
        return e.error.type
    raise AssertionError("a child of a taken id did not fail")


async def test_a_child_id_taken_by_another_run_fails_the_child(store: Store) -> None:
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [adopter, double]):
        taken = await sub.start(double, {"n": 1}, id="taken")
        assert await taken.result(timeout=LONG) == 2
        run = await sub.start(adopter)
        assert await run.result(timeout=LONG) == "RunExists"


async def test_the_result_of_a_run_not_a_child_is_invalid(store: Store) -> None:
    @workflow
    async def stranger(c: Context) -> str:
        try:
            await Child(c._shared, "someone").result()
        except InvalidArgument:
            return "refused"
        return "accepted"

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [stranger]):
        run = await sub.start(stranger)
        assert await run.result(timeout=LONG) == "refused"


@dataclass
class Count:
    i: int = 0
    total: int = 0


async def test_a_loop_compresses_at_every_boundary(store: Store) -> None:
    keys: list[str] = []
    chains: list[str] = []

    @workflow
    async def counter(c: Context) -> list[int]:
        beside = 0
        chains.append(c.chain)
        async for s in c.iterate(Count()):
            keys.append(await c.step("tick", step_key))
            if s.i == 3:
                break
            s.i += 1
            s.total += 10
            beside += 10
        return [s.i, s.total, beside]

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [counter], continue_as_new_after=0):
        run = await sub.start(counter, id="counter-1")
        result = await run.result(timeout=LONG)
    assert result == [3, 30, 0], "the state carries on, a local beside the loop starts again"
    assert set(chains) == {"counter-1"}, f"every run of the chain knows it: {chains}"
    assert len(keys) == 4, f"one tick per iteration: {keys}"
    assert keys[0] == "counter-1/tick/0"
    assert len({k.split("/")[0] for k in keys}) == 4, f"a run per iteration: {keys}"
    assert all(k.endswith("/tick/0") for k in keys), keys


@dataclass
class Opaque:
    at: object = None


async def test_a_state_that_is_not_json_fails_a_compression(store: Store) -> None:
    @workflow
    async def opaque(c: Context) -> None:
        started = await c.now()
        async for s in c.iterate(Opaque()):
            s.at = started

    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [opaque], continue_as_new_after=0):
        run = await sub.start(opaque)
        with pytest.raises(RunFailed) as failed:
            await run.result(timeout=LONG)
    assert failed.value.error.type == "NotCompressible", failed.value
    assert "datetime" in failed.value.error.message


@workflow
async def countdown(c: Context, n: int) -> str:
    if n > 0:
        await c.continue_as_new({"n": n - 1})
    return "liftoff"


@workflow(name="landing", version=2)
async def landing(c: Context, n: int) -> str:
    return f"landed v2 after {n}"


@workflow
async def flight(c: Context, n: int) -> str:
    await c.continue_as_new({"n": n}, workflow=landing)
    raise AssertionError("continue_as_new returned")


async def test_continue_as_new_by_hand(store: Store) -> None:
    sub = await Substantial._create(store, partitions=1)
    async with script(sub, [countdown, flight, landing]):
        run = await sub.start(countdown, {"n": 3})
        assert await run.result(timeout=LONG) == "liftoff"
        assert await run.status() == RunStatus.COMPLETED
        other = await sub.start(flight, {"n": 5})
        assert await other.result(timeout=LONG) == "landed v2 after 5"
