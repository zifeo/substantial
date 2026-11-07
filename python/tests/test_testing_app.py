"""The test environment of docs/testing.md: an ASGI application given `env.client()`, whose
lifespan starts its own worker and closes its client."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest

import substantial as s
from substantial.client import Opening

T0 = datetime(2030, 1, 7, 9, 0, tzinfo=UTC)
charged: list[str] = []

Message = dict[str, Any]
App = Callable[..., Any]


@s.workflow
async def fulfil(c: s.Context, order: str) -> str:
    async def capture(order: str) -> str:
        charged.append(order)
        return f"cap-{order}"

    return await c.step("capture", capture, order)


def create_app(open_engine: Callable[[], Opening]) -> App:
    """The shop: POST /orders starts a run, `?wait=1` waits 5 s for its result; GET
    /orders/<id> reads its status."""
    state: dict[str, Any] = {}

    async def lifespan(receive: Any, send: Any) -> None:
        async with contextlib.AsyncExitStack() as stack:
            assert (await receive())["type"] == "lifespan.startup"
            sub = await stack.enter_async_context(open_engine())
            await stack.enter_async_context(sub.worker([fulfil], preset="service"))
            state["sub"] = sub
            await send({"type": "lifespan.startup.complete"})
            assert (await receive())["type"] == "lifespan.shutdown"
        await send({"type": "lifespan.shutdown.complete"})

    async def http(scope: Message, receive: Any) -> tuple[int, Any]:
        sub: s.Substantial = state["sub"]
        if scope["method"] == "POST":
            body = json.loads((await receive())["body"])
            run = await sub.start(fulfil, body, id=body["order"])
            if scope["query_string"] != b"wait=1":
                return 202, None
            try:
                return 200, await run.result(timeout=s.seconds(5))
            except s.WaitTimeout:
                return 202, None
        order = scope["path"].rpartition("/")[2]
        return 200, {"status": await sub.run(fulfil, order).status()}

    async def app(scope: Message, receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            await lifespan(receive, send)
            return
        status, body = await http(scope, receive)
        await send({"type": "http.response.start", "status": status})
        await send({"type": "http.response.body", "body": json.dumps(body).encode()})

    return app


async def request(app: App, method: str, path: str, body: Any = None) -> tuple[int, Any]:
    path, _, query = path.partition("?")
    scope = {"type": "http", "method": method, "path": path, "query_string": query.encode()}
    sent: list[Message] = []

    async def receive() -> Message:
        return {"type": "http.request", "body": json.dumps(body).encode()}

    async def send(message: Message) -> None:
        sent.append(message)

    await app(scope, receive, send)
    return sent[0]["status"], json.loads(sent[1]["body"])


@contextlib.asynccontextmanager
async def running(app: App) -> Any:
    """The app's lifespan, as a server runs it around its requests."""
    inbox: asyncio.Queue[Message] = asyncio.Queue()
    outbox: asyncio.Queue[Message] = asyncio.Queue()
    task = asyncio.create_task(app({"type": "lifespan"}, inbox.get, outbox.put))
    await inbox.put({"type": "lifespan.startup"})
    assert (await outbox.get())["type"] == "lifespan.startup.complete"
    yield
    await inbox.put({"type": "lifespan.shutdown"})
    assert (await outbox.get())["type"] == "lifespan.shutdown.complete"
    await task


@pytest.fixture(autouse=True)
def fakes() -> None:
    charged.clear()


async def test_an_asgi_app_given_a_client() -> None:
    async with s.testing.Env.open(now=T0) as env:
        app = create_app(env.client)
        async with running(app):
            for _ in range(2):
                assert await request(app, "POST", "/orders", {"order": "o-1"}) == (202, None)
            assert await request(app, "GET", "/orders/o-1") == (200, {"status": "pending"})
            await env.settle()
            assert await request(app, "GET", "/orders/o-1") == (200, {"status": "completed"})
            assert await env.run(fulfil, "o-1").result() == "cap-o-1"
        assert charged == ["o-1"]
        await env.check()


async def test_an_endpoint_that_waits_answers_at_once_when_the_step_is_slow() -> None:
    async with s.testing.Env.open(now=T0) as env:
        app = create_app(env.client)
        env.hold("capture")
        async with running(app):
            assert await request(app, "POST", "/orders?wait=1", {"order": "o-1"}) == (202, None)
            assert env.now == T0
        await env.check()
