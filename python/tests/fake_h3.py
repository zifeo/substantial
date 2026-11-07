"""A fake h3 server on 127.0.0.1: the S3 calls of the store contract on one in-memory bucket, its
listings held on the wait headers, every request and the headers of every listing recorded."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
from dataclasses import dataclass
from urllib.parse import parse_qsl, unquote, urlsplit
from xml.sax.saxutils import escape

from substantial import Credentials

BUCKET = "h3"
CREDENTIALS = Credentials("fake", "fake-secret")


@dataclass
class Listed:
    """A listing as the fake received it; `woken` when held on its wait etag, then answered with
    another page: a change ended the wait."""

    prefix: str
    headers: dict[str, str]
    woken: bool


@dataclass
class Answer:
    status: int
    body: bytes = b""
    headers: dict[str, str] | None = None


def _error(status: int, code: str) -> Answer:
    xml = f'<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code></Error>'
    return Answer(status, xml.encode())


class FakeH3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.listed: list[Listed] = []
        # The method and target of every request, in the order received.
        self.requests: list[tuple[str, str]] = []
        self.holds = 0
        self.omit_etag = False
        self._changed = asyncio.Event()
        self._server: asyncio.Server | None = None
        self._port = 0

    async def __aenter__(self) -> FakeH3:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self._port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._server is not None:
            self._server.close()
            self._server.close_clients()
            await self._server.wait_closed()

    def url(self, namespace: str) -> str:
        return f"h3+http://127.0.0.1:{self._port}/{BUCKET}/{namespace}?region=us-east-1"

    def _change(self) -> None:
        self._changed.set()
        self._changed = asyncio.Event()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(asyncio.IncompleteReadError, ConnectionError):
            while True:
                line = await reader.readline()
                if not line:
                    break
                method, target, _ = line.decode().split(" ", 2)
                headers: dict[str, str] = {}
                while (header := await reader.readline()) not in (b"\r\n", b"\n", b""):
                    name, _, value = header.decode().partition(":")
                    headers[name.strip().lower()] = value.strip()
                body = await reader.readexactly(int(headers.get("content-length", "0")))
                answer = await self._answer(method, target, headers, body)
                extra = "".join(f"{k}: {v}\r\n" for k, v in (answer.headers or {}).items())
                head = f"HTTP/1.1 {answer.status} X\r\ncontent-length: {len(answer.body)}\r\n"
                writer.write(f"{head}{extra}\r\n".encode() + answer.body)
                await writer.drain()
        writer.close()

    async def _answer(
        self, method: str, target: str, headers: dict[str, str], body: bytes
    ) -> Answer:
        self.requests.append((method, target))
        parts = urlsplit(target)
        path = unquote(parts.path)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        if not path.startswith(f"/{BUCKET}"):
            return _error(404, "NoSuchBucket")
        key = path.removeprefix(f"/{BUCKET}").removeprefix("/")
        if method == "GET" and not key and query.get("list-type") == "2":
            return await self._list(headers, query)
        if method == "PUT" and key:
            if headers.get("if-none-match") == "*" and key in self.objects:
                return _error(412, "PreconditionFailed")
            self.objects[key] = body
            self._change()
            return Answer(200)
        if method == "GET" and key:
            if key not in self.objects:
                return _error(404, "NoSuchKey")
            return Answer(200, self.objects[key])
        if method == "DELETE" and key:
            self.objects.pop(key, None)
            self._change()
            return Answer(204)
        return _error(400, "InvalidRequest")

    def _page(self, prefix: str, after: str, max_keys: int) -> tuple[list[str], bool]:
        keys = [k for k in sorted(self.objects) if k.startswith(prefix) and k > after]
        return keys[:max_keys], len(keys) > max_keys

    async def _list(self, headers: dict[str, str], query: dict[str, str]) -> Answer:
        prefix = query.get("prefix", "")
        after = query.get("start-after", "")
        max_keys = int(query.get("max-keys", "1000"))
        seconds = headers.get("x-substantial-wait-seconds")
        wait_etag = headers.get("x-substantial-wait-etag")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + (int(seconds) if seconds is not None else 0)
        held = False
        while True:
            changed = self._changed
            keys, truncated = self._page(prefix, after, max_keys)
            etag = hashlib.sha256(repr((keys, truncated)).encode()).hexdigest()[:16]
            if seconds is None or wait_etag != etag or loop.time() >= deadline:
                break
            if not held:
                held = True
                self.holds += 1
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout_at(deadline):
                    await changed.wait()
        self.listed.append(Listed(prefix, headers, woken=held and wait_etag != etag))
        contents = "".join(f"<Contents><Key>{escape(k)}</Key></Contents>" for k in keys)
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>'
            f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>{contents}"
            "</ListBucketResult>"
        )
        extra = {} if seconds is None or self.omit_etag else {"x-substantial-listing-etag": etag}
        return Answer(200, xml.encode(), extra)
