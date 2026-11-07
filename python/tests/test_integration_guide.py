"""docs/integration.md: the service of `python/examples/webhooks.py`, run on the test environment
of docs/testing.md."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest
from test_examples import load

import substantial as s

web = load("webhooks")
T0 = datetime(2026, 11, 6, 9, 0, tzinfo=UTC)


@pytest.fixture
def crm(monkeypatch: pytest.MonkeyPatch) -> Any:
    crm = web.Crm()
    monkeypatch.setattr(web, "crm", crm)
    return crm


@pytest.fixture
def ledger(monkeypatch: pytest.MonkeyPatch) -> Any:
    ledger = web.Ledger()
    monkeypatch.setattr(web, "ledger", ledger)
    return ledger


@contextlib.asynccontextmanager
async def serving(app: Any) -> AsyncIterator[None]:
    """The app's lifespan, as an ASGI server runs it around its requests."""
    inbox: asyncio.Queue[dict] = asyncio.Queue()
    outbox: asyncio.Queue[dict] = asyncio.Queue()
    scope = {"type": "lifespan", "asgi": {"version": "3.0"}, "state": {}}
    task = asyncio.create_task(app(scope, inbox.get, outbox.put))
    await inbox.put({"type": "lifespan.startup"})
    assert (await outbox.get())["type"] == "lifespan.startup.complete"
    yield
    await inbox.put({"type": "lifespan.shutdown"})
    assert (await outbox.get())["type"] == "lifespan.shutdown.complete"
    await task


async def call(app: Any, method: str, path: str, body: Any = None) -> tuple[int, Any]:
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
        "server": ("test", 80),
        "client": ("test", 1),
    }
    data = b"" if body is None else json.dumps(body).encode()
    sent: list[dict] = []

    async def receive() -> dict:
        return {"type": "http.request", "body": data, "more_body": False}

    async def send(message: dict) -> None:
        sent.append(message)

    await app(scope, receive, send)
    raw = b"".join(m.get("body", b"") for m in sent[1:])
    return sent[0]["status"], json.loads(raw) if raw else None


async def test_the_app_runs_its_worker_in_its_lifespan(crm: Any, ledger: Any) -> None:
    async with s.testing.Env.open(now=T0) as env:
        app = web.create_app(env.client, concurrency=4)
        async with serving(app):
            assert await call(app, "GET", "/readyz") == (200, None)
            event = {"id": "evt_1AbC", "type": "invoice.paid", "data": {"invoice": "in_1"}}
            for attempt in (1, 2):
                body = {**event, "delivery_attempt": attempt}
                assert await call(app, "POST", "/webhooks/provider", body) == (200, None)
            other = {**event, "data": {"invoice": "in_2"}}
            assert (await call(app, "POST", "/webhooks/provider", other))[0] == 409

            op = {"id": "op-1", "kind": "credit", "amount": 30}
            assert await call(app, "POST", "/accounts/acc-1/ops", op) == (202, None)
            assert await call(app, "GET", "/accounts/acc-1/ops/op-1") == (202, {"pending": True})
            await env.settle()
            assert await call(app, "GET", "/accounts/acc-1/ops/op-1") == (
                200,
                {"ok": True, "balance": 30},
            )
            order = {"order": "o-1", "account": "acc-1", "amount": 20}
            assert await call(app, "POST", "/orders", order) == (200, {"paid": True})
            order = {"order": "o-2", "account": "acc-1", "amount": 20}
            assert await call(app, "POST", "/orders", order) == (200, {"paid": False})

            env.outage(s.minutes(1))
            assert (await call(app, "POST", "/accounts/acc-1/ops", op))[0] == 503
            await env.advance(s.minutes(1))
            assert await call(app, "POST", "/accounts/acc-1/ops", op) == (202, None)

            app.state.worker.crash()
            assert await call(app, "GET", "/readyz") == (503, None)
            assert await call(app, "GET", "/livez") == (200, None)
        assert crm.events == {"evt-evt_1AbC": {"kind": "invoice.paid", "invoice": "in_1"}}
        assert ledger.posted == {"op-1": ("acc-1", 30), "order-o-1": ("acc-1", -20)}
        await env.check()


async def test_an_event_redelivered_after_its_run_was_collected_is_answered_as_applied(
    crm: Any, ledger: Any
) -> None:
    async with s.testing.Env.open(now=T0) as env:
        app = web.create_app(env.client, concurrency=4)
        async with serving(app):
            event = {"id": "evt_1AbC", "type": "invoice.paid", "data": {"invoice": "in_1"}}
            assert await call(app, "POST", "/webhooks/provider", event) == (200, None)
            await env.advance(s.days(31))
            with pytest.raises(s.RunNotFound):
                await env.run(web.provider_event, web.run_id("evt", "evt_1AbC")).status()
            assert await call(app, "POST", "/webhooks/provider", event) == (200, None)
        assert crm.events == {"evt-evt_1AbC": {"kind": "invoice.paid", "invoice": "in_1"}}
        await env.check()


async def test_a_closed_account_answers_410_and_never_reopens(ledger: Any) -> None:
    async with s.testing.Env.open(now=T0) as env:
        app = web.create_app(env.client, concurrency=4)
        async with serving(app):
            op = {"id": "op-1", "kind": "credit", "amount": 30}
            assert await call(app, "POST", "/accounts/acc-1/ops", op) == (202, None)
            close = {"id": "close-1", "kind": "close"}
            assert await call(app, "POST", "/accounts/acc-1/ops", close) == (202, None)
            await env.settle()
            closed = (410, {"error": "account acc-1 is closed"})
            late = {"id": "op-2", "kind": "credit", "amount": 5}
            # Within the default retention, past it, and past the 7 days an id stays taken then.
            for i, wait in enumerate((s.days(1), s.days(30), s.days(7))):
                await env.advance(wait)
                assert await call(app, "POST", "/accounts/acc-1/ops", late) == closed
                order = {"order": f"o-{i}", "account": "acc-1", "amount": 20}
                assert await call(app, "POST", "/orders", order) == (200, {"paid": False})
            assert await call(app, "GET", "/accounts/acc-1/ops/close-1") == (
                200,
                {"ok": True, "balance": 30, "closed": True},
            )
            assert await call(app, "GET", "/accounts/acc-1/ops/order-o-2") == (
                200,
                {"ok": False, "closed": True},
            )
        assert ledger.posted == {"op-1": ("acc-1", 30)}
        await env.check()


async def test_an_order_resent_after_its_run_was_collected_is_answered_as_handled(
    ledger: Any,
) -> None:
    async with s.testing.Env.open(now=T0) as env:
        app = web.create_app(env.client, concurrency=4)
        async with serving(app):
            top_up = {"id": "top-up", "kind": "credit", "amount": 30}
            assert await call(app, "POST", "/accounts/acc-1/ops", top_up) == (202, None)
            order = {"order": "o-1", "account": "acc-1", "amount": 20}
            assert await call(app, "POST", "/orders", order) == (200, {"paid": True})
            await env.advance(s.days(31))
            with pytest.raises(s.RunNotFound):
                await env.run(web.pay_order, "order-o-1").status()
            assert await call(app, "POST", "/orders", order) == (200, None)
            other = {**order, "amount": 25}
            assert await call(app, "POST", "/orders", other) == (200, None)
        assert ledger.posted == {"top-up": ("acc-1", 30), "order-o-1": ("acc-1", -20)}
        await env.check()


@dataclass
class Pod:
    """An environment with one worker serving the example's workflows."""

    env: s.testing.Env
    worker: s.Worker


@contextlib.asynccontextmanager
async def pod(**settings: Any) -> AsyncIterator[Pod]:
    async with (
        s.testing.Env.open(now=settings.pop("now", T0)) as env,
        env.worker(web.WORKFLOWS, preset="service", **settings) as worker,
    ):
        yield Pod(env, worker)
        await env.check()


async def test_each_change_pushes_the_sync_back(crm: Any) -> None:
    async with pod() as p:
        run = await p.env.start(web.contact_sync, {"contact": "C1"}, id=web.run_id("contact", "C1"))
        for version in (1, 2, 3):
            await run.send("changed", version)
            await p.env.advance(s.minutes(1))
        await p.env.advance(s.seconds(59))
        assert crm.pushed == []
        await p.env.advance(s.seconds(1))
        assert crm.pushed == [("C1", 3)]


async def test_an_endless_stream_is_synced_within_the_max_wait(crm: Any) -> None:
    async with pod() as p:
        run = await p.env.start(web.contact_sync, {"contact": "C1"}, id="contact-c1")
        pushed_at = []
        for version in range(40):
            await run.send("changed", version)
            await p.env.advance(s.minutes(1))
            if len(crm.pushed) > len(pushed_at):
                pushed_at.append(p.env.now - T0)
        assert crm.pushed == [("C1", 14), ("C1", 29)]
        assert pushed_at == [s.minutes(15), s.minutes(30)]


@pytest.mark.parametrize("apart", [False, True], ids=["one-instant", "settled"])
async def test_two_changes_in_one_instant_or_apart(crm: Any, apart: bool) -> None:
    async with pod() as p:
        run = await p.env.start(web.contact_sync, {"contact": "C1"}, id="contact-c1")
        await run.send("changed", 2)
        if apart:
            await p.env.settle()
        await run.send("changed", 1)
        await p.env.advance(s.minutes(2))
        assert crm.pushed == [("C1", 2)]


async def test_a_change_during_the_push_crosses_a_compression(crm: Any) -> None:
    async with pod(continue_as_new_after=0) as p:
        push = p.env.hold("push_contact")
        run = await p.env.start(web.contact_sync, {"contact": "C1"}, id="contact-c1")
        await run.send("changed", 1)
        await p.env.advance(s.minutes(2))
        assert push.held == 1
        await run.send("changed", 2)
        push.release()
        await p.env.advance(s.minutes(2))
        assert crm.pushed == [("C1", 1), ("C1", 2)]


@dataclass
class Applied:
    ops: list[int] = field(default_factory=list)


@s.step
async def apply(op: int) -> int:
    return op


@s.workflow
async def undrained(c: s.Context) -> list[int]:
    ops: list[int] = []
    c.handle("op", ops.append)
    async for a in c.iterate(Applied()):
        await c.wait_until(lambda: bool(ops))
        op = ops.pop(0)
        a.ops.append(await apply(op))
        if op == 99:
            break
    return a.ops


async def test_a_handler_list_left_undrained_loses_a_message_at_a_compression() -> None:
    async with (
        s.testing.Env.open(now=T0) as env,
        env.worker([undrained], preset="service", continue_as_new_after=0),
    ):
        held = env.hold("apply")
        run = await env.start(undrained, id="undrained")
        await run.send("op", 1)
        await env.settle()
        await run.send("op", 2)
        await run.send("op", 3)
        held.release()
        await env.settle()
        await run.send("op", 99)
        assert await run.result() == [1, 2, 99]
        await env.check()


async def test_a_provider_id_is_hashed_and_its_volatile_fields_left_out(crm: Any) -> None:
    async with pod() as p:
        for event in ("evt_1AbC", "evt_1abc"):
            stable = {"event": event, "kind": "invoice.paid", "data": {}}
            await p.env.start(web.provider_event, stable, id=web.run_id("evt", event))
        await p.env.settle()
        assert sorted(crm.events) == ["evt-evt_1AbC", "evt-evt_1abc"]

        stable = {"event": "evt_2", "kind": "invoice.paid", "data": {"invoice": "in_2"}}
        rid = web.run_id("evt", "evt_2")
        await p.env.start(web.provider_event, stable, id=rid)
        reordered = {"kind": "invoice.paid", "event": "evt_2", "data": {"invoice": "in_2"}}
        await p.env.start(web.provider_event, reordered, id=rid)
        with pytest.raises(s.RunExists):
            await p.env.start(web.provider_event, {**stable, "delivery_attempt": 2}, id=rid)


OPS = [("credit", 50), ("debit", 20), ("debit", 40), ("credit", 5)]
OUTCOMES = [
    {"ok": True, "balance": 50},
    {"ok": True, "balance": 30},
    {"ok": False, "balance": 30},
    {"ok": True, "balance": 35},
]


async def send_ops(run: s.Run) -> None:
    for i, (kind, amount) in enumerate(OPS):
        await run.send("op", {"id": f"op-{i}", "kind": kind, "amount": amount}, id=f"op-{i}")


async def test_an_actor_applies_its_ops_in_order_through_a_crash(ledger: Any) -> None:
    async with (
        pod(continue_as_new_after=0) as p,
        p.env.worker(web.WORKFLOWS, preset="service", continue_as_new_after=0),
    ):
        post = p.env.hold("post")
        run = await p.env.start(web.account_actor, {"account": "acc-1"}, id="account-acc-1")
        await send_ops(run)
        await p.env.settle()
        assert post.held == 1
        post.crash()
        await p.env.advance(s.seconds(35))
        assert await run.read("op.op-0") is None
        await p.env.advance(s.seconds(1))
        assert [await run.read(f"op.op-{i}") for i in range(4)] == OUTCOMES
        assert ledger.calls == 4
        assert ledger.posted == {
            "op-0": ("acc-1", 50),
            "op-1": ("acc-1", -20),
            "op-3": ("acc-1", 5),
        }


@pytest.mark.parametrize(("continue_as_new_after", "calls"), [(None, 1), (0, 2)])
async def test_an_op_resent_after_the_dedup_window_runs_again_once_the_actor_continued(
    ledger: Any, continue_as_new_after: int | None, calls: int
) -> None:
    settings = {} if continue_as_new_after is None else {"continue_as_new_after": 0}
    async with pod(**settings) as p:
        run = await p.env.start(web.account_actor, {"account": "acc-1"}, id="account-acc-1")
        op = {"id": "op-1", "kind": "credit", "amount": 30}
        await run.send("op", op, id="op-1")
        await p.env.advance(s.days(7) - s.minutes(1))
        await run.send("op", op, id="op-1")
        await p.env.settle()
        assert ledger.calls == 1
        await p.env.advance(s.minutes(2))
        await run.send("op", op, id="op-1")
        await p.env.settle()
        # A run's own history keeps every id it consumed: only a continued run forgets it.
        assert ledger.calls == calls


async def test_a_pod_that_leaves_hands_its_runs_over_at_the_next_scan(ledger: Any) -> None:
    async with pod() as p:
        post = p.env.hold("post")
        run = await p.env.start(web.account_actor, {"account": "acc-1"}, id="account-acc-1")
        await send_ops(run)
        await p.env.settle()
        assert post.held == 1
        async with p.env.worker(web.WORKFLOWS, preset="service"):
            await p.worker.shutdown()
            await p.env.advance(s.seconds(30))
            assert [await run.read(f"op.op-{i}") for i in range(4)] == OUTCOMES
        assert ledger.calls == 4


async def test_a_workflow_asks_the_actor_and_waits_for_its_reply(ledger: Any) -> None:
    async with pod() as p:
        account = await p.env.start(web.account_actor, {"account": "acc-1"}, id="account-acc-1")
        await account.send("op", {"id": "top-up", "kind": "credit", "amount": 30}, id="top-up")
        order = {"order": "o-1", "account": "acc-1", "amount": 20}
        paid = await p.env.start(web.pay_order, order, id="order-o-1")
        assert await paid.result() is True
        assert await account.read("op.order-o-1") == {"ok": True, "balance": 10}


async def test_the_nightly_job_runs_at_three_in_zurich_across_a_dst_change(
    ledger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with pod(now=datetime(2026, 10, 23, 12, 0, tzinfo=UTC)) as p:
        nights: list[tuple[str, datetime]] = []

        async def report(day: str) -> None:
            nights.append((day, p.env.now))

        monkeypatch.setattr(ledger, "report", report)
        await p.env.start(web.nightly, id="nightly")
        await p.env.advance(s.days(3))
        assert nights == [
            ("2026-10-24", datetime(2026, 10, 24, 1, 0, tzinfo=UTC)),
            ("2026-10-25", datetime(2026, 10, 25, 2, 0, tzinfo=UTC)),
            ("2026-10-26", datetime(2026, 10, 26, 2, 0, tzinfo=UTC)),
        ]


def test_the_engine_is_configured_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUBSTANTIAL_URL", raising=False)
    with pytest.raises(KeyError, match="SUBSTANTIAL_URL"):
        web.app()
    monkeypatch.setenv("SUBSTANTIAL_URL", "memory://webhooks")
    monkeypatch.setenv("SUBSTANTIAL_PARTITIONS", "1")
    monkeypatch.setenv("SUBSTANTIAL_CONCURRENCY", "16")
    assert web.Config.from_env() == web.Config("memory://webhooks", 1, 16)
    assert web.app().router.routes
