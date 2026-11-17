"""The S3 adapter: four S3 requests sent with aiohttp and signed with botocore, both installed by
the `s3` extra."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import ssl
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Any
from urllib.parse import quote, unquote, unquote_plus, urlsplit
from urllib.request import getproxies, proxy_bypass

import aiohttp
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import ReadOnlyCredentials
from botocore.httpsession import get_cert_path
from yarl import URL

from substantial.errors import ConfigError
from substantial.stores import retry
from substantial.stores.attempts import sent
from substantial.stores.base import Capabilities, Create, Listing, ListWait, refuse_wait
from substantial.stores.credentials import (
    REFRESH_BEFORE,
    Credentials,
    CredentialsArg,
    Provider,
    provide,
    resolve,
)
from substantial.stores.retry import STORE_RETRY, Ambiguous, Fault, Permanent, Transient
from substantial.stores.s3 import (
    ATTEMPT_TIMEOUT,
    CONNECT_TIMEOUT,
    LISTING_ETAG,
    PAGE,
    S3Location,
    wait_headers,
)

# Below the 20 s after which S3 closes an idle connection, so a kept connection is not reused as
# the server closes it.
KEEPALIVE_TIMEOUT = 12.0


class _Keys:
    """The keys every request is signed with: given once, or a provider's, asked for at the first
    request and again by any request made within `REFRESH_BEFORE` of their `expires_at`."""

    def __init__(self, given: Credentials | Provider) -> None:
        self._given = given
        self._keys: Credentials | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> Credentials:
        if isinstance(self._given, Credentials):
            return self._given
        async with self._lock:
            keys = self._keys
            if keys is None or (
                keys.expires_at is not None
                and datetime.now(UTC) >= keys.expires_at - REFRESH_BEFORE
            ):
                try:
                    keys = await provide(self._given, Credentials)
                except Exception as e:
                    raise Permanent(f"credentials provider: {type(e).__name__}: {e}") from e
                self._keys = keys
        return keys


def _proxy(location: S3Location) -> tuple[URL | None, dict[str, str]]:
    """The proxy that `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` name for the endpoint, and the
    `Proxy-Authorization` of the credentials in its URL."""
    scheme = urlsplit(location.endpoint).scheme
    proxy = getproxies().get(scheme)
    if not proxy or proxy_bypass(location.host):
        return None, {}
    if not proxy.startswith(("http:", "https:")):
        proxy = f"http:{proxy}" if proxy.startswith("//") else f"http://{proxy}"
    parts = urlsplit(proxy)
    if parts.username is None:
        return URL(proxy), {}
    user = f"{unquote(parts.username)}:{unquote(parts.password or '')}"
    authorization = f"Basic {base64.b64encode(user.encode()).decode()}"
    bare = parts._replace(netloc=parts.netloc.rpartition("@")[2]).geturl()
    return URL(bare), {"Proxy-Authorization": authorization}


def _local(tag: str) -> str:
    return tag.rpartition("}")[2]


class _Refusal(Exception):
    """An S3 error answer."""

    def __init__(self, what: str, status: int, body: bytes) -> None:
        fields: dict[str, str] = {}
        with contextlib.suppress(ET.ParseError):
            fields = {_local(e.tag): (e.text or "").strip() for e in ET.fromstring(body).iter()}
        # Without an S3 error in the body, the status names the error, as it does in the SDKs.
        code = fields.get("Code") or str(status)
        message = fields.get("Message") if "Code" in fields else _phrase(status)
        text = f"{code}: {message}" if message else code
        super().__init__(f"{what}: HTTP {status} {text}")
        self.status = status
        self.code = code
        self.message = str(self)


def _phrase(status: int) -> str:
    try:
        return HTTPStatus(status).phrase
    except ValueError:
        return ""


def _status_fault(refusal: _Refusal) -> Fault:
    """Classifies the failure of an operation other than a create, which none makes ambiguous."""
    s = refusal.status
    if s >= 500 or s in (408, 409, 429):
        return Transient(refusal.message)
    return Permanent(refusal.message)


def _encode(value: str, *, keep_slash: bool = False) -> str:
    """URI-encodes as SigV4 requires: every byte but the unreserved characters."""
    return quote(value, safe="-_.~/" if keep_slash else "-_.~")


class S3Store:
    """`s3://<endpoint>/<bucket>/<ns>?region=<r>`: one object per key under the prefix `<ns>/`,
    created with `If-None-Match: *`. `h3://` is the same adapter, its listings declared able
    to wait; `s3+http://` and `h3+http://` reach the endpoint over plain HTTP.

    On `h3://`, a listing given a `wait` sends it as `x-substantial-wait-seconds` and
    `x-substantial-wait-etag`, and answers the `x-substantial-listing-etag` the store returns.

    Requests address the bucket in the path, sign their body's SHA-256 and carry no checksum
    header. Nothing retries or follows a redirect but `STORE_RETRY`, and no AWS variable or file
    is read; the endpoint's proxy is taken from `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY`.
    """

    def __init__(self, location: S3Location, credentials: CredentialsArg) -> None:
        if not isinstance(location, S3Location):
            raise ConfigError(f"an S3 store needs an S3Location, not {location!r}")
        self._keys = _Keys(resolve(credentials))
        self._location = location
        self._proxy, proxy_auth = _proxy(location)
        # Over HTTPS the proxy is asked for a tunnel, over HTTP sent each request.
        tunnel = location.endpoint.startswith("https:")
        self._tunnel_headers = proxy_auth if tunnel else {}
        # An answer is never decoded, so it is asked for as stored.
        self._unsigned = {"Accept-Encoding": "identity", **({} if tunnel else proxy_auth)}
        # The authorities the SDK verified with, botocore's bundled list or certifi's: a Python
        # without the system's still reaches AWS, and no `AWS_CA_BUNDLE` is read.
        self._tls = ssl.create_default_context(cafile=get_cert_path(True))
        self._prefix = f"{location.namespace}/"
        self.capabilities = Capabilities(wait=location.wait)
        self._http: aiohttp.ClientSession | None = None
        self._opening = asyncio.Lock()
        self._closed = False

    def _full(self, key: str) -> str:
        return f"{self._prefix}{key}"

    async def _session(self) -> aiohttp.ClientSession:
        """The connection pool, made at the first request since it belongs to the running loop."""
        async with self._opening:
            if self._closed:
                raise Permanent("the store is closed")
            if self._http is None:
                self._http = aiohttp.ClientSession(
                    # No cap, as in every host: a waiting listing holds its connection up to
                    # 20 s, one per workflow served and per `result` waiter, and a capped pool
                    # would queue every other request behind them.
                    connector=aiohttp.TCPConnector(
                        limit=0, keepalive_timeout=KEEPALIVE_TIMEOUT, ssl=self._tls
                    ),
                    timeout=aiohttp.ClientTimeout(
                        total=None, sock_connect=CONNECT_TIMEOUT, sock_read=ATTEMPT_TIMEOUT
                    ),
                    auto_decompress=False,
                    skip_auto_headers=("Content-Type",),
                )
            return self._http

    async def _send(
        self,
        what: str,
        method: str,
        key: str | None,
        *,
        query: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
        create: bool = False,
    ) -> tuple[Any, bytes]:
        """The headers and body of the answer to one request: an answer other than 2xx raises
        `_Refusal`, no answer `Ambiguous` for a create and `Transient` otherwise."""
        loc = self._location
        path = f"/{_encode(loc.bucket)}"
        if key is not None:
            path += f"/{_encode(self._full(key), keep_slash=True)}"
        pairs = sorted((_encode(k), _encode(v)) for k, v in (query or {}).items())
        url = loc.endpoint + path + ("?" + "&".join(f"{k}={v}" for k, v in pairs) if pairs else "")
        session = await self._session()
        try:
            # A request that hangs must become an ambiguous outcome, not a stuck worker.
            async with asyncio.timeout(ATTEMPT_TIMEOUT):
                keys = await self._keys.get()
                request = AWSRequest(
                    method=method, url=url, headers={"Host": loc.host, **(headers or {})}, data=body
                )
                signer = S3SigV4Auth(
                    ReadOnlyCredentials(
                        keys.access_key_id, keys.secret_access_key, keys.session_token
                    ),
                    "s3",
                    loc.region,
                )
                signer.add_auth(request)
                async with session.request(
                    method,
                    URL(url, encoded=True),
                    data=body if body or method == "PUT" else None,
                    headers={**dict(request.headers.items()), **self._unsigned},
                    allow_redirects=False,
                    proxy=self._proxy,
                    proxy_headers=self._tunnel_headers or None,
                ) as response:
                    answer = await response.read()
        # aiohttp's own errors reach here from reading the body, a truncated one included.
        except (aiohttp.ClientError, TimeoutError) as e:
            fault = Ambiguous if create else Transient
            raise fault(f"{what}: {type(e).__name__}: {e}") from e
        if response.status >= 300:
            raise _Refusal(what, response.status, answer)
        return response.headers, answer

    async def _put(self, key: str, data: bytes) -> Create:
        what = f"create {key}"
        try:
            await self._send(
                what, "PUT", key, body=data, headers={"If-None-Match": "*"}, create=True
            )
        except _Refusal as r:
            if r.status == 412:
                return "exists"
            # AWS: a concurrent conditional write of the key was in flight, this one did not happen.
            if r.status in (409, 429):
                raise Transient(r.message) from None
            if r.status >= 500 or r.status == 408:
                raise Ambiguous(r.message) from None
            raise Permanent(r.message) from None
        return "created"

    async def _read(self, key: str) -> bytes | None:
        try:
            _, answer = await self._send(f"get {key}", "GET", key)
        except _Refusal as r:
            if r.status == 404 and r.code == "NoSuchKey":
                return None
            raise _status_fault(r) from None
        return answer

    async def _list_once(
        self, prefix: str, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        keys: list[str] = []
        etag: str | None = None
        after = self._full(start_after) if start_after is not None else None
        headers = wait_headers(wait, max_keys)
        what = f"list {prefix}"
        while len(keys) < max_keys:
            # Keys come URL-encoded, since XML 1.0 cannot hold every character a key may.
            query = {
                "list-type": "2",
                "prefix": self._full(prefix),
                "max-keys": str(min(max_keys - len(keys), PAGE)),
                "encoding-type": "url",
            }
            if after is not None:
                query["start-after"] = after
            try:
                answer_headers, body = await self._send(
                    what, "GET", None, query=query, headers=headers
                )
            except _Refusal as r:
                raise _status_fault(r) from None
            if wait is not None:
                etag = answer_headers.get(LISTING_ETAG)
            try:
                root = ET.fromstring(body)
            except ET.ParseError as e:
                raise Transient(f"{what}: {e}") from e
            fields = {_local(e.tag): e for e in root}
            encoded = getattr(fields.get("EncodingType"), "text", None) == "url"
            for element in root:
                if _local(element.tag) != "Contents":
                    continue
                full = next((c.text for c in element if _local(c.tag) == "Key"), None) or ""
                if encoded:
                    full = unquote_plus(full)
                if not full.startswith(self._prefix):
                    raise Permanent(f"listed {full!r} outside the namespace")
                keys.append(full.removeprefix(self._prefix))
            truncated = getattr(fields.get("IsTruncated"), "text", None) == "true" and keys
            if wait is not None or not truncated or len(keys) >= max_keys:
                break
            # The retry layer counted the first page's request.
            sent()
            after = self._full(keys[-1])
        if wait is not None and not etag:
            raise Permanent(f"a waiting listing of {prefix} answered without {LISTING_ETAG}")
        return Listing(keys=keys[:max_keys], etag=etag)

    async def _remove(self, key: str) -> None:
        try:
            await self._send(f"delete {key}", "DELETE", key)
        except _Refusal as r:
            if r.status == 404 and r.code == "NoSuchKey":
                return
            raise _status_fault(r) from None

    async def open(self) -> None:
        pass

    async def create(self, key: str, data: bytes) -> Create:
        data = bytes(data)
        return await retry.create(
            STORE_RETRY, data, lambda: self._put(key, data), lambda: self._read(key)
        )

    async def get(self, key: str) -> bytes | None:
        return await retry.with_retry(STORE_RETRY, lambda: self._read(key))

    async def list(
        self, prefix: str, *, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        if not self.capabilities.wait:
            refuse_wait(wait)
        return await retry.with_retry(
            STORE_RETRY, lambda: self._list_once(prefix, start_after, max_keys, wait)
        )

    async def delete(self, key: str) -> None:
        await retry.with_retry(STORE_RETRY, lambda: self._remove(key))

    async def close(self) -> None:
        async with self._opening:
            self._closed = True
            if self._http is not None:
                await self._http.close()
