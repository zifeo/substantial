from __future__ import annotations

import asyncio
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlsplit

import httpx

from substantial.errors import ConfigError
from substantial.stores import retry
from substantial.stores.base import (
    Capabilities,
    Create,
    Listing,
    ListWait,
    Store,
    StoreFailure,
    refuse_wait,
)
from substantial.stores.credentials import CredentialsArg, Keys
from substantial.stores.retry import STORE_RETRY, Ambiguous, Fault, Permanent, Transient
from substantial.stores.sigv4 import canonical_query, encode, sign

# ListObjectsV2 returns at most this many keys a page.
PAGE = 1000
CONNECT_TIMEOUT = 5.0
ATTEMPT_TIMEOUT = 60.0
WAIT_SECONDS = "x-substantial-wait-seconds"
WAIT_ETAG = "x-substantial-wait-etag"
LISTING_ETAG = "x-substantial-listing-etag"
MAX_WAIT_SECONDS = 20
_SCHEMES = {
    "s3": (False, "https"),
    "s3+http": (False, "http"),
    "h3": (True, "https"),
    "h3+http": (True, "http"),
}


@dataclass(frozen=True)
class S3Location:
    """The parts of an `s3://` or `h3://` URL, every one required (I-16)."""

    endpoint: str
    host: str
    bucket: str
    namespace: str
    region: str
    wait: bool

    @staticmethod
    def parse(url: str) -> S3Location:
        def invalid(why: str) -> ConfigError:
            return ConfigError(f"{url!r}: {why}")

        parts = urlsplit(url)
        if parts.scheme not in _SCHEMES:
            raise invalid("not an s3:// or h3:// URL")
        wait, protocol = _SCHEMES[parts.scheme]
        if "@" in parts.netloc:
            raise invalid("credentials go in `credentials`, not in the URL")
        if "#" in url:
            raise invalid("a store URL takes no fragment")
        if not parts.hostname:
            raise invalid("names no endpoint")
        try:
            port = parts.port
        except ValueError as e:
            raise invalid(str(e)) from e
        host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname
        if port is not None:
            host = f"{host}:{port}"
        bucket, _, namespace = parts.path.removeprefix("/").partition("/")
        if not bucket:
            raise invalid("names no bucket")
        if any(c == "" for c in namespace.split("/")):
            raise invalid("names no namespace, or one with an empty component")
        query: dict[str, str] = {}
        for name, value in parse_qsl(parts.query, keep_blank_values=True):
            if name in query:
                raise invalid(f"{name} is given twice")
            query[name] = value
        region = query.pop("region", "")
        if not region:
            raise invalid("the query parameter region is required")
        if query:
            raise invalid(f"unknown query parameter {next(iter(query))}")
        return S3Location(f"{protocol}://{host}", host, bucket, namespace, region, wait)


def _wait_headers(wait: ListWait | None, max_keys: int) -> dict[str, str]:
    """The headers of a waiting listing, signed with the request."""
    if wait is None:
        return {}
    # The listing ETag covers one page, so a waiting listing is never continued.
    if max_keys > PAGE:
        raise Permanent(f"a waiting listing takes at most {PAGE} keys, not {max_keys}")
    if wait.seconds > MAX_WAIT_SECONDS:
        raise Permanent(
            f"a waiting listing waits at most {MAX_WAIT_SECONDS} s, not {wait.seconds} s"
        )
    graphic = wait.etag is None or (
        wait.etag.isascii() and wait.etag.isprintable() and " " not in wait.etag
    )
    if not graphic:
        raise Permanent(f"the listing etag {wait.etag!r} cannot be sent in {WAIT_ETAG}")
    headers = {WAIT_SECONDS: str(wait.seconds)}
    if wait.etag is not None:
        headers[WAIT_ETAG] = wait.etag
    return headers


def _local(tag: str) -> str:
    return tag.rpartition("}")[2]


def _error_code(body: bytes) -> str | None:
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None
    for element in root.iter():
        if _local(element.tag) == "Code":
            return element.text
    return None


def _error_text(response: httpx.Response) -> str:
    """An S3 error answer as `Code: Message`, or the start of a body that is not one."""
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError:
        return response.text[:200]
    fields = {_local(e.tag): (e.text or "").strip() for e in root.iter()}
    code, message = fields.get("Code"), fields.get("Message")
    if not code:
        return response.text[:200]
    return f"{code}: {message}" if message else code


def _status_fault(what: str, response: httpx.Response) -> Fault:
    """Classifies the failure of an operation other than a create, which none makes ambiguous."""
    message = f"{what}: HTTP {response.status_code} {_error_text(response)}"
    s = response.status_code
    if s >= 500 or s in (408, 409, 429):
        return Transient(message)
    return Permanent(message)


class S3Store:
    """`s3://<endpoint>/<bucket>/<ns>?region=<r>`: one object per key under the prefix `<ns>/`,
    created with `If-None-Match: *`. `h3://` is the same adapter, its listings declared able
    to wait; `s3+http://` and `h3+http://` reach the endpoint over plain HTTP.

    On `h3://`, a listing given a `wait` sends it as `x-substantial-wait-seconds` and
    `x-substantial-wait-etag`, and answers the `x-substantial-listing-etag` the store returns.

    The HTTP client resolves nothing on its own (no proxies, netrc or certificate variables from
    the environment) and never retries: `STORE_RETRY` is the only retry.
    """

    def __init__(self, location: S3Location, credentials: CredentialsArg) -> None:
        if not isinstance(location, S3Location):
            raise ConfigError(f"an S3 store needs an S3Location, not {location!r}")
        self._keys = Keys(credentials)
        self._location = location
        self._prefix = f"{location.namespace}/"
        self.capabilities = Capabilities(wait=location.wait)
        self._client = httpx.AsyncClient(
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(ATTEMPT_TIMEOUT, connect=CONNECT_TIMEOUT),
        )

    def _full(self, key: str) -> str:
        return f"{self._prefix}{key}"

    async def _send(
        self,
        method: str,
        key: str | None,
        params: dict[str, str] | None = None,
        body: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        loc = self._location
        path = f"/{encode(loc.bucket)}"
        if key is not None:
            path += f"/{encode(self._full(key), keep_slash=True)}"
        query = canonical_query(params or {})
        try:
            credentials = await self._keys.get()
        except Exception as e:
            raise Permanent(f"credentials provider: {type(e).__name__}: {e}") from e
        signed = sign(
            method=method,
            host=loc.host,
            path=path,
            query=query,
            headers=headers or {},
            payload=body,
            credentials=credentials,
            region=loc.region,
        )
        url = f"{loc.endpoint}{path}" + (f"?{query}" if query else "")
        # A request that hangs must become an ambiguous outcome, not a stuck worker.
        async with asyncio.timeout(ATTEMPT_TIMEOUT):
            return await self._client.request(method, url, content=body, headers=signed)

    async def _put(self, key: str, data: bytes) -> Create:
        try:
            response = await self._send("PUT", key, body=data, headers={"if-none-match": "*"})
        except (httpx.HTTPError, TimeoutError) as e:
            raise Ambiguous(f"create {key}: {e!r}") from e
        s = response.status_code
        if 200 <= s < 300:
            return "created"
        message = f"create {key}: HTTP {s} {_error_text(response)}"
        if s == 412:
            return "exists"
        # AWS: a concurrent conditional write of the key was in flight, this one did not happen.
        if s in (409, 429):
            raise Transient(message)
        if s >= 500 or s == 408:
            raise Ambiguous(message)
        raise Permanent(message)

    async def _read(self, key: str) -> bytes | None:
        try:
            response = await self._send("GET", key)
        except (httpx.HTTPError, TimeoutError) as e:
            raise Transient(f"get {key}: {e!r}") from e
        if response.status_code == 200:
            return response.content
        if response.status_code == 404 and _error_code(response.content) == "NoSuchKey":
            return None
        raise _status_fault(f"get {key}", response)

    async def _list_once(
        self, prefix: str, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        keys: list[str] = []
        etag: str | None = None
        after = self._full(start_after) if start_after is not None else None
        headers = _wait_headers(wait, max_keys)
        while len(keys) < max_keys:
            params = {
                "list-type": "2",
                "prefix": self._full(prefix),
                "max-keys": str(min(max_keys - len(keys), PAGE)),
            }
            if after is not None:
                params["start-after"] = after
            try:
                response = await self._send("GET", None, params=params, headers=headers)
            except (httpx.HTTPError, TimeoutError) as e:
                raise Transient(f"list {prefix}: {e!r}") from e
            if response.status_code != 200:
                raise _status_fault(f"list {prefix}", response)
            if wait is not None:
                etag = response.headers.get(LISTING_ETAG)
            try:
                root = ET.fromstring(response.content)
            except ET.ParseError as e:
                raise Transient(f"list {prefix}: {e}") from e
            truncated = False
            for element in root:
                tag = _local(element.tag)
                if tag == "IsTruncated":
                    truncated = element.text == "true"
                elif tag == "Contents":
                    full = next((c.text for c in element if _local(c.tag) == "Key"), None) or ""
                    if not full.startswith(self._prefix):
                        raise Permanent(f"listed {full!r} outside the namespace")
                    keys.append(full.removeprefix(self._prefix))
            if wait is not None or not (truncated and keys):
                break
            after = self._full(keys[-1])
        if wait is not None and not etag:
            raise Permanent(f"a waiting listing of {prefix} answered without {LISTING_ETAG}")
        return Listing(keys=keys[:max_keys], etag=etag)

    async def _remove(self, key: str) -> None:
        try:
            response = await self._send("DELETE", key)
        except (httpx.HTTPError, TimeoutError) as e:
            raise Transient(f"delete {key}: {e!r}") from e
        if 200 <= response.status_code < 300:
            return
        if response.status_code == 404 and _error_code(response.content) == "NoSuchKey":
            return
        raise _status_fault(f"delete {key}", response)

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
        await self._client.aclose()


async def result_created(store: Store, key: str) -> None:
    """Returns once the `result` at `key` exists, holding waiting listings that take its key as
    their prefix: a result waited on is seen as soon as it is created, in one request per
    `MAX_WAIT_SECONDS` meanwhile."""
    wait = ListWait(0)
    while True:
        page = await store.list(key, start_after=None, max_keys=1, wait=wait)
        if key in page.keys:
            return
        if page.etag is None:
            raise StoreFailure(f"a waiting listing answered without {LISTING_ETAG}")
        wait = ListWait(MAX_WAIT_SECONDS, page.etag)
