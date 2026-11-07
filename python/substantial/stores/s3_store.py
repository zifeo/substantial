"""The S3 adapter, over aiobotocore, which the `s3` extra installs."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

import aiohttp
from aiobotocore.config import AioConfig
from aiobotocore.credentials import AioCredentialResolver, AioDeferredRefreshableCredentials
from aiobotocore.session import AioSession
from botocore.credentials import CredentialProvider
from botocore.exceptions import BotoCoreError, ClientError, ParamValidationError
from botocore.handlers import add_expect_header

from substantial.errors import ConfigError
from substantial.stores import retry
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

# The wait headers of the listing being sent, which a hook adds to it before it is signed.
_WAIT_HEADERS: ContextVar[dict[str, str]] = ContextVar("wait_headers")
# botocore's refreshable keys need an expiry: a provider's keys without one are never refreshed.
_NEVER = datetime.max.replace(tzinfo=UTC)


class _ProviderFailed(Exception):
    pass


class _ProviderKeys(AioDeferredRefreshableCredentials):
    """A provider's keys, asked for at the first request and again before any request made within
    `REFRESH_BEFORE` of their `expires_at`."""

    _advisory_refresh_timeout = _mandatory_refresh_timeout = REFRESH_BEFORE.total_seconds()

    def __init__(self, provider: Provider) -> None:
        async def refresh() -> dict[str, str | None]:
            keys = await provide(provider, Credentials)
            return {
                "access_key": keys.access_key_id,
                "secret_key": keys.secret_access_key,
                "token": keys.session_token,
                "expiry_time": (keys.expires_at or _NEVER).isoformat(),
            }

        super().__init__(refresh, "substantial-provider")

    async def get_frozen_credentials(self) -> Any:
        try:
            return await super().get_frozen_credentials()
        except Exception as e:
            raise _ProviderFailed(f"credentials provider: {type(e).__name__}: {e}") from e


class _Provided(CredentialProvider):
    """The only source of keys of the store's session, in place of the SDK's chain."""

    METHOD = "substantial-provider"

    def __init__(self, keys: _ProviderKeys) -> None:
        self._keys = keys

    async def load(self) -> _ProviderKeys:
        return self._keys


def _add_wait_headers(request: Any, **_: Any) -> None:
    for name, value in _WAIT_HEADERS.get().items():
        request.headers[name] = value


def _no_region_redirect(context: dict[str, Any], **_: Any) -> None:
    # The SDK resends a request answered with another region, after a HeadBucket to find it: a
    # store operation attempt stays one request.
    context["s3_redirect"]["redirected"] = True


def _session() -> AioSession:
    """A session reading no AWS variable or file: the URL and the credentials alone define the
    store, as in every host."""
    variables = {
        name: (None, None, default, convert)
        for name, (_, _, default, convert) in AioSession.SESSION_VARIABLES.items()
    }
    # Without a config file there is no profile, whose `s3` section would change the client.
    for name in ("config_file", "credentials_file", "s3", "proxies_config"):
        variables[name] = (None, None, None, None)
    return AioSession(session_vars=variables)


class _Refusal(Exception):
    """An S3 error answer."""

    def __init__(self, status: int, code: str | None, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def _status_fault(refusal: _Refusal) -> Fault:
    """Classifies the failure of an operation other than a create, which none makes ambiguous."""
    s = refusal.status
    if s >= 500 or s in (408, 409, 429):
        return Transient(refusal.message)
    return Permanent(refusal.message)


class S3Store:
    """`s3://<endpoint>/<bucket>/<ns>?region=<r>`: one object per key under the prefix `<ns>/`,
    created with `If-None-Match: *`. `h3://` is the same adapter, its listings declared able
    to wait; `s3+http://` and `h3+http://` reach the endpoint over plain HTTP.

    On `h3://`, a listing given a `wait` sends it as `x-substantial-wait-seconds` and
    `x-substantial-wait-etag`, and answers the `x-substantial-listing-etag` the store returns.

    The SDK's own retries and region redirects are off: `STORE_RETRY` is the only retry.
    """

    def __init__(self, location: S3Location, credentials: CredentialsArg) -> None:
        if not isinstance(location, S3Location):
            raise ConfigError(f"an S3 store needs an S3Location, not {location!r}")
        self._session = _session()
        given = resolve(credentials)
        if isinstance(given, Credentials):
            self._session.set_credentials(
                given.access_key_id, given.secret_access_key, given.session_token
            )
        else:
            resolver = AioCredentialResolver([_Provided(_ProviderKeys(given))])
            self._session.register_component("credential_provider", resolver)
        self._location = location
        self._prefix = f"{location.namespace}/"
        self.capabilities = Capabilities(wait=location.wait)
        self._client: Any = None
        self._opening = asyncio.Lock()
        self._resources = AsyncExitStack()
        self._closed = False

    def _full(self, key: str) -> str:
        return f"{self._prefix}{key}"

    async def _sdk_client(self) -> Any:
        """The SDK's client, made at the first request since making it is async."""
        async with self._opening:
            if self._closed:
                raise Permanent("the store is closed")
            if self._client is None:
                loc = self._location
                config = AioConfig(
                    connect_timeout=CONNECT_TIMEOUT,
                    read_timeout=ATTEMPT_TIMEOUT,
                    retries={"mode": "standard", "total_max_attempts": 1},
                    # No cap, as in every host: a waiting listing holds its connection up to 20 s,
                    # one per workflow and partition served and per `result` waiter, and a capped
                    # pool would queue every other request behind them.
                    max_pool_connections=0,
                    s3={"addressing_style": "path"},
                    # S3-compatible stores that predate flexible checksums refuse their headers.
                    request_checksum_calculation="when_required",
                    response_checksum_validation="when_required",
                )
                client = await self._resources.enter_async_context(
                    self._session.create_client(
                        "s3", region_name=loc.region, endpoint_url=loc.endpoint, config=config
                    )
                )
                client.meta.events.register("before-call.s3", _no_region_redirect)
                # `Expect: 100-continue` costs a round trip per create, of a few bytes each.
                client.meta.events.unregister("before-call.s3", add_expect_header)
                client.meta.events.register("before-sign.s3.ListObjectsV2", _add_wait_headers)
                self._client = client
            return self._client

    async def _send(self, what: str, operation: str, *, create: bool = False, **params: Any) -> Any:
        """One request: an S3 error answer raises `_Refusal`, no answer `Ambiguous` for a create
        and `Transient` otherwise."""
        call = getattr(await self._sdk_client(), operation)
        try:
            # A request that hangs must become an ambiguous outcome, not a stuck worker.
            async with asyncio.timeout(ATTEMPT_TIMEOUT):
                response = await call(Bucket=self._location.bucket, **params)
                if "Body" in response:
                    response["Body"] = await response["Body"].read()
                return response
        except ClientError as e:
            error = e.response.get("Error", {})
            status = e.response["ResponseMetadata"]["HTTPStatusCode"]
            code, message = error.get("Code"), error.get("Message")
            text = f"{code}: {message}" if code and message else (code or str(e))
            raise _Refusal(status, code, f"{what}: HTTP {status} {text}") from e
        except _ProviderFailed as e:
            raise Permanent(str(e)) from e
        except ParamValidationError as e:
            raise Permanent(f"{what}: {e}") from e
        # aiohttp's own errors reach here from reading the body, a truncated one included.
        except (BotoCoreError, aiohttp.ClientError, TimeoutError) as e:
            fault = Ambiguous if create else Transient
            raise fault(f"{what}: {type(e).__name__}: {e}") from e

    async def _put(self, key: str, data: bytes) -> Create:
        what = f"create {key}"
        try:
            await self._send(
                what, "put_object", create=True, Key=self._full(key), Body=data, IfNoneMatch="*"
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
            response = await self._send(f"get {key}", "get_object", Key=self._full(key))
        except _Refusal as r:
            if r.status == 404 and r.code == "NoSuchKey":
                return None
            raise _status_fault(r) from None
        return response["Body"]

    async def _list_once(
        self, prefix: str, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        keys: list[str] = []
        etag: str | None = None
        after = self._full(start_after) if start_after is not None else None
        headers = wait_headers(wait, max_keys)
        while len(keys) < max_keys:
            params: dict[str, Any] = {
                "Prefix": self._full(prefix),
                "MaxKeys": min(max_keys - len(keys), PAGE),
            }
            if after is not None:
                params["StartAfter"] = after
            token = _WAIT_HEADERS.set(headers)
            try:
                response = await self._send(f"list {prefix}", "list_objects_v2", **params)
            except _Refusal as r:
                raise _status_fault(r) from None
            finally:
                _WAIT_HEADERS.reset(token)
            if wait is not None:
                etag = response["ResponseMetadata"]["HTTPHeaders"].get(LISTING_ETAG)
            for item in response.get("Contents", []):
                full = item["Key"]
                if not full.startswith(self._prefix):
                    raise Permanent(f"listed {full!r} outside the namespace")
                keys.append(full.removeprefix(self._prefix))
            if wait is not None or not (response.get("IsTruncated", False) and keys):
                break
            after = self._full(keys[-1])
        if wait is not None and not etag:
            raise Permanent(f"a waiting listing of {prefix} answered without {LISTING_ETAG}")
        return Listing(keys=keys[:max_keys], etag=etag)

    async def _remove(self, key: str) -> None:
        try:
            await self._send(f"delete {key}", "delete_object", Key=self._full(key))
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
            await self._resources.aclose()
