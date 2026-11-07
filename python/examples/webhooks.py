"""The service of docs/integration.md: a Starlette app whose lifespan runs the worker, receiving
provider webhooks (deduplicated by event id), CRM changes (debounced) and account operations (an
actor per account), with a nightly job.

    SUBSTANTIAL_URL=file:/tmp/webhooks SUBSTANTIAL_PARTITIONS=1 SUBSTANTIAL_CONCURRENCY=16 \\
    uv run --with uvicorn uvicorn --app-dir python/examples webhooks:app --factory

The third-party APIs are stand-ins that keep what they were asked, once per idempotency key.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

import substantial as s


class Crm:
    """Stands for the CRM's API."""

    def __init__(self) -> None:
        self.events: dict[str, dict] = {}
        self.pushed: list[tuple[str, int]] = []

    async def apply(self, key: str, kind: str, data: dict) -> None:
        self.events.setdefault(key, {"kind": kind, **data})

    async def push(self, contact: str, version: int) -> None:
        self.pushed.append((contact, version))


class Ledger:
    """Stands for the ledger's API."""

    def __init__(self) -> None:
        self.posted: dict[str, tuple[str, int]] = {}
        self.calls = 0
        self.reports: list[str] = []

    async def post(self, key: str, account: str, amount: int) -> None:
        self.calls += 1
        self.posted.setdefault(key, (account, amount))

    async def report(self, day: str) -> None:
        self.reports.append(day)


crm = Crm()
ledger = Ledger()

RETRY = s.Retry(max_attempts=8, initial=s.seconds(1), max=s.minutes(5), multiplier=2)


def run_id(prefix: str, external: str) -> str:
    """A run or message id for a provider's id: ids are lowercase `[a-z0-9._-]{1,100}`, and
    lowercasing a case-sensitive id would merge two of them."""
    return f"{prefix}-{hashlib.sha256(external.encode()).hexdigest()[:32]}"


# A provider webhook, applied once per event id.


@s.step(retry=RETRY)
async def apply_event(event: str, kind: str, data: dict) -> None:
    await crm.apply(f"evt-{event}", kind, data)


@s.workflow
async def provider_event(c: s.Context, event: str, kind: str, data: dict) -> None:
    await apply_event(event, kind, data)


# A durable debounce: each change of a contact pushes its sync back by QUIET, at most MAX_WAIT
# after the first change of a burst.

QUIET = s.minutes(2)
MAX_WAIT = s.minutes(15)


@dataclass
class Burst:
    versions: list[int] = field(default_factory=list)
    deadline: str | None = None


@s.step(retry=RETRY)
async def push_contact(contact: str, version: int) -> None:
    await crm.push(contact, version)


@s.workflow
async def contact_sync(c: s.Context, contact: str) -> None:
    changes: list[int] = []
    c.handle("changed", changes.append)
    async for b in c.iterate(Burst()):
        if not b.versions:
            await c.wait_until(lambda: bool(changes))
        else:
            left = datetime.fromisoformat(str(b.deadline)) - await c.now()
            quiet = left > timedelta(0) and await c.wait_until(
                lambda: bool(changes), timeout=min(QUIET, left)
            )
            if not quiet:
                await push_contact(contact, max(b.versions))
                b.versions, b.deadline = [], None
        if changes and b.deadline is None:
            b.deadline = (await c.now() + MAX_WAIT).isoformat()
        # Last, as messages also arrive while a step runs: only the state crosses a compression.
        b.versions += changes
        changes.clear()


# An actor: one run per account, applying its operations one at a time, in order.


@dataclass
class Account:
    balance: int = 0
    closed: bool = False
    queue: list[dict] = field(default_factory=list)


@s.step(retry=RETRY)
async def post(op: str, account: str, amount: int) -> None:
    await ledger.post(op, account, amount)


async def apply(account: str, a: Account, op: dict) -> dict:
    amount = op["amount"] if op["kind"] == "credit" else -op["amount"]
    if a.balance + amount < 0:
        return {"ok": False, "balance": a.balance}
    await post(op["id"], account, amount)
    a.balance += amount
    return {"ok": True, "balance": a.balance}


@s.workflow
async def account_actor(c: s.Context, account: str) -> None:
    ops: list[dict] = []
    c.handle("op", ops.append)
    async for a in c.iterate(Account()):
        if not a.queue:
            await c.wait_until(lambda: bool(ops))
        else:
            op = a.queue.pop(0)
            if a.closed:
                outcome = {"ok": False, "closed": True}
            elif op["kind"] == "close":
                # The run goes on, closed: its id stays taken, so the account never reopens.
                a.closed = True
                c.publish("closed", True)
                outcome = {"ok": True, "balance": a.balance, "closed": True}
            else:
                outcome = await apply(account, a, op)
            c.publish(f"op.{op['id']}", outcome)
            if "reply_to" in op:
                try:
                    await c.send(op["reply_to"], "reply", outcome)
                except (s.RunEnded, s.RunNotFound):
                    c.log.warning(
                        "op %s: %s no longer waits for its reply", op["id"], op["reply_to"]
                    )
        a.queue += ops
        ops.clear()


@s.workflow
async def pay_order(c: s.Context, order: str, account: str, amount: int) -> bool:
    op = {"id": f"order-{order}", "kind": "debit", "amount": amount, "reply_to": c.chain}
    await c.send(f"account-{account}", "op", op)
    reply = await c.receive("reply")
    return reply["ok"]


# A nightly job at 03:00 in Zurich, an hour that every day has once: 02:00 is skipped in March
# and repeated in October.

ZURICH = ZoneInfo("Europe/Zurich")


def next_night(now: datetime) -> datetime:
    local = now.astimezone(ZURICH)
    night = local.replace(hour=3, minute=0, second=0, microsecond=0)
    if night <= local:
        night += timedelta(days=1)
    return night.astimezone(UTC)


@dataclass
class Nights:
    done: int = 0


@s.step(retry=RETRY)
async def report(day: str) -> None:
    await ledger.report(day)


@s.workflow
async def nightly(c: s.Context) -> None:
    async for n in c.iterate(Nights()):
        night = next_night(await c.now())
        await c.sleep_until(night)
        await report(night.astimezone(ZURICH).date().isoformat())
        n.done += 1


WORKFLOWS = [provider_event, contact_sync, account_actor, pay_order, nightly]


# The app.


def create_app(
    open_engine: Callable[[], AbstractAsyncContextManager[s.Substantial]], concurrency: int
) -> Starlette:
    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        async with (
            open_engine() as sub,
            sub.worker(WORKFLOWS, preset="service", concurrency=concurrency) as worker,
        ):
            await sub.start(nightly, id="nightly")
            app.state.sub, app.state.worker = sub, worker
            yield

    def engine(request: Request) -> s.Substantial:
        return request.app.state.sub

    async def provider_webhook(request: Request) -> Response:
        event = await request.json()
        stable = {"event": event["id"], "kind": event["type"], "data": event["data"]}
        try:
            await engine(request).start(provider_event, stable, id=run_id("evt", event["id"]))
        except s.RunExists as e:
            if e.collected:
                return Response(status_code=200)
            return JSONResponse({"error": "this event id came with another body"}, 409)
        return Response(status_code=200)

    async def crm_webhook(request: Request) -> Response:
        change = await request.json()
        sub = engine(request)
        contact = change["contact"]
        run = await sub.start(contact_sync, {"contact": contact}, id=run_id("contact", contact))
        await run.send("changed", change["version"], id=run_id("delivery", change["delivery"]))
        return Response(status_code=202)

    async def account_op(request: Request) -> Response:
        account, op = request.path_params["account"], await request.json()
        sub = engine(request)
        run = await sub.start(account_actor, {"account": account}, id=f"account-{account}")
        if await run.read("closed"):
            return JSONResponse({"error": f"account {account} is closed"}, 410)
        await run.send("op", op, id=op["id"])
        return Response(status_code=202)

    async def op_outcome(request: Request) -> Response:
        p = request.path_params
        run = engine(request).run(account_actor, f"account-{p['account']}")
        outcome = await run.read(f"op.{p['op']}")
        return JSONResponse(outcome or {"pending": True}, 200 if outcome else 202)

    async def order(request: Request) -> Response:
        body = await request.json()
        sub = engine(request)
        account = body["account"]
        await sub.start(account_actor, {"account": account}, id=f"account-{account}")
        try:
            run = await sub.start(pay_order, body, id=f"order-{body['order']}")
        except s.RunExists as e:
            if e.collected:
                return Response(status_code=200)
            return JSONResponse({"error": "this order id came with another body"}, 409)
        try:
            return JSONResponse({"paid": await run.result(timeout=s.seconds(5))})
        except s.WaitTimeout:
            return JSONResponse({"pending": True}, 202)

    def live(request: Request) -> Response:
        return Response(status_code=500 if request.app.state.worker.error else 200)

    def ready(request: Request) -> Response:
        return Response(status_code=200 if request.app.state.worker.alive else 503)

    async def store_down(request: Request, e: Exception) -> Response:
        return JSONResponse({"error": str(e)}, 503, headers={"Retry-After": "5"})

    return Starlette(
        lifespan=lifespan,
        routes=[
            Route("/webhooks/provider", provider_webhook, methods=["POST"]),
            Route("/webhooks/crm", crm_webhook, methods=["POST"]),
            Route("/accounts/{account}/ops", account_op, methods=["POST"]),
            Route("/accounts/{account}/ops/{op}", op_outcome, methods=["GET"]),
            Route("/orders", order, methods=["POST"]),
            Route("/livez", live),
            Route("/readyz", ready),
        ],
        exception_handlers={s.StoreError: store_down},
    )


@dataclass(frozen=True)
class Config:
    url: str
    partitions: int
    concurrency: int

    @classmethod
    def from_env(cls) -> Config:
        e = os.environ
        return cls(
            e["SUBSTANTIAL_URL"],
            int(e["SUBSTANTIAL_PARTITIONS"]),
            int(e["SUBSTANTIAL_CONCURRENCY"]),
        )

    def open(self) -> Any:
        s3 = self.url.split(":", 1)[0] in {"s3", "h3", "s3+http", "h3+http"}
        credentials = "env" if s3 else None
        return s.Substantial.create(self.url, partitions=self.partitions, credentials=credentials)


def app() -> Starlette:
    config = Config.from_env()
    return create_app(config.open, config.concurrency)
