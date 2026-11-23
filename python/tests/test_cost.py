"""What runs cost: prices, charges, `c.cost()` and budgets, in a test environment."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from typing import Any

import pytest

import substantial as s
from substantial.cost import cost_of

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)
S3 = "aws-s3-standard-us-east-1-2026-11-09"
MAX = 2**63 - 1


async def pay(c: s.Context, nano: int) -> None:
    """A step that charges `nano` nano-dollars."""

    async def paying() -> None:
        s.charge("USD", nano)

    await c.step("pay", paying)


def payer(name: str, budget: dict[str, int] | None = None) -> s.Workflow[..., dict[str, Any]]:
    """Pays 6 nano-dollars `n` times, then returns its cost."""

    async def paying(c: s.Context, n: int) -> dict[str, Any]:
        for _ in range(n):
            await pay(c, 6)
        return dataclasses.asdict(await c.cost())

    return s.Workflow(paying, name=name, budget=budget)


def is_budget_exceeded(e: s.RunFailed) -> bool:
    return e.status == "failed" and e.error.type == "BudgetExceeded"


def test_the_named_prices_and_what_prices_refuse() -> None:
    named = s.Prices(S3)
    assert named == s.Prices(
        currency="USD", create=5_000_000, cas=5_000_000, get=400_000, list=5_000_000, delete=0
    )
    overridden = s.Prices(S3, get=500_000)
    assert (overridden.currency, overridden.get, overridden.list) == ("USD", 500_000, 5_000_000)
    assert s.Prices(S3, create=MAX).create == MAX
    assert named._of("create", 2) == s.Amount("USD", 10_000)
    assert s.Prices(S3, cas=3000)._of("cas", 2) == s.Amount("USD", 6)
    assert named._of("get", 1) == s.Amount("USD", 400)
    assert named._of("delete", 3) == s.Amount("USD", 0)
    assert s.Prices(S3, get=1001)._of("get", 1).nano == 2, "a share rounds up"
    refused: list[dict[str, Any]] = [
        {"name": "zero"},
        {"currency": "USD", "create": 1, "get": 1, "list": 1},
        {"currency": "USD", "create": 1, "get": 1, "list": 1, "delete": 1},
        {"name": S3, "cas": -1},
        {"name": S3, "currency": "usd"},
        {"name": S3, "currency": "US1"},
        {"name": S3, "currency": "USDT"},
        {"name": S3, "create": -1},
        {"name": S3, "create": MAX + 1},
        {"name": S3, "create": True},
        {"name": S3, "get": 1.0},
        {"name": S3, "list": "1"},
    ]
    for fields in refused:
        name = fields.pop("name", None)
        with pytest.raises(s.ConfigError):
            s.Prices(name, **fields)


async def test_prices_are_given_to_create_and_checked_by_the_worker() -> None:
    with pytest.raises(s.ConfigError):
        await s.Substantial.create("memory://prices-wrong", prices="USD")  # type: ignore[arg-type]
    async with s.Substantial.create("memory://prices-fine", prices=s.Prices(S3)):
        pass
    # Three uppercase letters, not a currency: the worker refuses it when it starts.
    async with s.testing.Env.open(now=T0, prices=s.Prices(S3, currency="QQQ")) as env:
        worker = env.worker([payer("p")], preset="service")
        with pytest.raises(s.ConfigError):
            async with worker:
                pass
        assert not worker.alive


async def test_charges_reach_the_cost_by_step_and_in_total() -> None:
    @s.workflow
    async def charged(c: s.Context) -> dict[str, Any]:
        async def paying() -> None:
            s.charge("USD", 5)
            s.charge("EUR", 3)
            s.charge("USD", 5)

        def fee() -> None:
            s.charge("USD", 1)

        await c.step("pay", paying)
        await c.step("fee", fee)
        return dataclasses.asdict(await c.cost())

    async with (
        s.testing.Env.open(now=T0, prices=s.Prices(S3)) as env,
        env.worker([charged], preset="service"),
    ):
        cost = await (await env.start(charged)).result()
        assert cost["steps"] == {"fee": {"USD": 1}, "pay": {"EUR": 3, "USD": 10}}
        store = cost["store"]["amounts"]["USD"]
        assert store > 0, cost
        assert cost["amounts"] == {"USD": store + 11, "EUR": 3}
        assert cost["requests"] > 0 and cost["requests"] == cost["store"]["requests"], cost
        assert cost["requests"] == sum(cost[k] for k in ("create", "cas", "get", "list", "delete"))
        assert cost["children"]["requests"] == 0
        await env.check()


async def test_charge_refuses_a_wrong_amount_and_runs_only_in_a_step() -> None:
    @s.workflow
    async def refusing(c: s.Context) -> list[Any]:
        def refused() -> list[bool]:
            out = []
            for nano in (MAX + 1, -1, True, 1.0, "1"):
                try:
                    s.charge("USD", nano)  # type: ignore[arg-type]
                    out.append(False)
                except s.InvalidArgument:
                    out.append(True)
            return out

        def blocking() -> str:
            s.charge("USD", 2)
            return s.step_key()

        try:
            s.charge("USD", 1)
            outside = "charged"
        except s.NotInWorkflow:
            outside = "NotInWorkflow"
        return [
            await c.step("pay", refused),
            await c.step("blocking", blocking),
            outside,
            (await c.cost()).amounts,
        ]

    async with s.testing.Env.open(now=T0) as env, env.worker([refusing], preset="service"):
        refused, key, outside, amounts = await (await env.start(refusing, id="p")).result()
        assert refused == [True] * 5
        assert key == "p/blocking/0"
        assert outside == "NotInWorkflow", "the workflow's code is no step"
        assert amounts == {"USD": 2}
    with pytest.raises(s.NotInWorkflow):
        s.charge("USD", 1)


async def test_an_unknown_currency_fails_the_step_with_invalid_argument() -> None:
    @s.workflow
    async def lowercase(c: s.Context) -> str:
        async def paying() -> None:
            s.charge("usd", 1)

        try:
            await c.step("pay", paying)
        except s.StepFailed as e:
            return e.cause.type
        return "paid"

    async with s.testing.Env.open(now=T0) as env, env.worker([lowercase], preset="service"):
        assert await (await env.start(lowercase)).result() == "InvalidArgument"


async def test_cost_is_recorded_and_replays_the_same() -> None:
    seen: list[int] = []

    @s.workflow
    async def twice(c: s.Context) -> list[int]:
        before = await c.cost()
        seen.append(before.requests)
        await c.sleep(s.minutes(1))
        after = await c.cost()
        return [before.requests, after.requests]

    async with s.testing.Env.open(now=T0) as env, env.worker([twice], preset="service"):
        run = await env.start(twice)
        await env.advance(s.minutes(1))
        before, after = await run.result()
        assert seen == [before, before], "the replay got the recorded value"
        assert after > before
        assert all(e.kind != "budget_exceeded" for e in await run.history())


def test_costs_read_the_core_json() -> None:
    meters = {"create": 1, "cas": 1, "get": 2, "list": 3, "delete": 0, "requests": 7}
    cost = cost_of(
        {
            **meters,
            "amounts": {"USD": "9007199254740993"},
            "store": {**meters, "amounts": {}},
            "steps": {"pay": {"USD": "5"}},
            "children": {**{k: 0 for k in meters}, "amounts": {}},
        }
    )
    assert cost.amounts == {"USD": 9_007_199_254_740_993}
    assert cost.steps == {"pay": {"USD": 5}}
    assert cost.store == s.Meters(1, 1, 2, 3, 0, 7, {})
    assert cost.children.requests == 0


@pytest.mark.parametrize(("meter", "limit"), [("requests", 20), ("cas", 3)])
async def test_a_budget_on_requests_passed_raises_and_fails_the_run(meter: str, limit: int) -> None:
    @s.workflow
    async def spender(c: s.Context) -> int:
        def ident(i: int) -> int:
            return i

        try:
            for i in range(100):
                await c.step("s", ident, i)
                await c.sleep(s.minutes(1))
        except s.BudgetExceeded as e:
            c.publish("over", [e.meter, e.limit, e.used])
            raise
        return 0

    async with s.testing.Env.open(now=T0) as env, env.worker([spender], preset="service"):
        run = await env.start(spender, budget={meter: limit})
        await env.advance(s.minutes(30))
        with pytest.raises(s.RunFailed) as failed:
            await run.result()
        assert is_budget_exceeded(failed.value), failed.value
        assert await run.status() == s.RunStatus.FAILED
        over, at, used = await run.read("over")
        assert (over, at) == (meter, limit)
        assert used > limit
        await env.check()


async def test_a_budget_in_a_currency_is_passed_by_charges() -> None:
    wf = payer("payer")
    async with s.testing.Env.open(now=T0) as env, env.worker([wf], preset="service"):
        run = await env.start(wf, {"n": 3}, budget={"USD": 10})
        with pytest.raises(s.RunFailed) as failed:
            await run.result()
        assert is_budget_exceeded(failed.value), failed.value
        await env.check()


async def test_a_declared_budget_a_start_overrides_and_the_same_id_with_another() -> None:
    with pytest.raises(s.InvalidArgument):
        payer("declared", {"Requests": 1})
    wf = payer("declared", {"USD": 10})
    async with s.testing.Env.open(now=T0) as env, env.worker([wf], preset="service"):
        with pytest.raises(s.RunFailed) as failed:
            await (await env.start(wf, {"n": 3}, id="d")).result()
        assert is_budget_exceeded(failed.value), failed.value
        given = await env.start(wf, {"n": 3}, id="g", budget={"USD": 100})
        assert (await given.result())["amounts"] == {"USD": 18}
        # The budget is part of the start's identity.
        await env.start(wf, {"n": 3}, id="g", budget={"USD": 100})
        with pytest.raises(s.RunExists):
            await env.start(wf, {"n": 3}, id="g")
        for budget in ({"Requests": 1}, {"get": MAX + 1}, {"get": True}, {"QQQ": 1}, ["USD"]):
            with pytest.raises(s.InvalidArgument):
                await env.start(wf, {"n": 3}, budget=budget)  # type: ignore[arg-type]
        await env.check()


async def test_a_retry_with_a_larger_budget_completes_and_keeps_the_cost_spent() -> None:
    wf = payer("retried")
    async with s.testing.Env.open(now=T0) as env, env.worker([wf], preset="service"):
        run = await env.start(wf, {"n": 3}, budget={"USD": 10})
        with pytest.raises(s.RunFailed) as failed:
            await run.result()
        assert is_budget_exceeded(failed.value), failed.value
        await run.retry(budget={"USD": 100})
        with pytest.raises(s.InvalidArgument):
            await run.retry(budget={"USD": 1000})
        # The two payments of the failed run, and the retry's.
        assert (await run.result())["amounts"] == {"USD": 18}
        await env.check()


async def test_a_childs_cost_reaches_its_parent() -> None:
    kid = payer("kid")

    @s.workflow
    async def parent(c: s.Context) -> list[Any]:
        child = await c.start(kid, {"n": 2})
        theirs = await child.result()
        return [theirs, dataclasses.asdict(await c.cost())]

    async with s.testing.Env.open(now=T0) as env, env.worker([parent, kid], preset="service"):
        theirs, mine = await (await env.start(parent)).result()
        assert mine["children"]["amounts"] == {"USD": 12}
        assert mine["children"]["requests"] >= theirs["requests"], mine
        assert mine["amounts"] == {"USD": 12}
        assert mine["steps"] == {}


async def test_a_child_start_takes_its_own_budget_else_its_workflows() -> None:
    kid = payer("kid", {"USD": 100})

    @s.workflow
    async def parent(c: s.Context) -> list[str]:
        out = []
        for budget in ({"USD": 5}, None):
            # The second payment's attempt does not start past the limit.
            child = await c.start(kid, {"n": 2}, budget=budget)
            try:
                await child.result()
                out.append("completed")
            except s.RunFailed as e:
                out.append(e.error.type)
        return out

    async with s.testing.Env.open(now=T0) as env, env.worker([parent, kid], preset="service"):
        assert await (await env.start(parent)).result() == ["BudgetExceeded", "completed"]
        await env.check()


async def test_a_child_start_past_the_budget_raises_at_once() -> None:
    kid = payer("kid")

    @s.workflow
    async def parent(c: s.Context) -> str:
        await pay(c, 20)
        try:
            await c.start(kid, {"n": 0})
            started = "started"
        except s.BudgetExceeded as e:
            started = e.meter
        c.publish("start", started)
        return started

    async with s.testing.Env.open(now=T0) as env, env.worker([parent, kid], preset="service"):
        run = await env.start(parent, budget={"USD": 10})
        with pytest.raises(s.RunFailed) as failed:
            await run.result()
        assert is_budget_exceeded(failed.value), failed.value
        assert await run.read("start") == "USD"
        await env.check()


async def test_a_run_failed_by_its_budget_or_a_step_gives_the_failure_details() -> None:
    @s.workflow
    async def over(c: s.Context) -> None:
        await pay(c, 1_500_000)

        async def after() -> None:
            pass

        await c.step("after", after)

    class Declined(s.Permanent):
        pass

    @s.workflow
    async def declined(c: s.Context) -> None:
        def decline() -> None:
            raise Declined("not today")

        await c.step("pay", decline)

    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([over, declined], preset="service"),
    ):
        run = await env.start(over, budget={"USD": 1_000_000})
        with pytest.raises(s.RunFailed) as failed:
            await run.result()
        assert failed.value.error.type == "BudgetExceeded"
        assert failed.value.error.details == {"meter": "USD", "limit": "1000000", "used": "1500000"}
        run = await env.start(declined)
        with pytest.raises(s.RunFailed) as failed:
            await run.result()
        assert failed.value.error.type == "StepFailed"
        assert failed.value.error.details is not None
        assert failed.value.error.details["step"] == "pay"
        assert failed.value.cause == s.Failure("Declined", "not today", False)
