"""What the S3 adapter does beyond the shared contract; all but URL parsing and the fake h3 server
run only when `SUBSTANTIAL_S3_URL` is set (see `dev/rustfs.compose.yaml`)."""

import asyncio
import base64
import contextlib
import hashlib
import os
import ssl
import sys
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from botocore.httpsession import get_cert_path
from conftest import LONG, require_s3, script
from fake_h3 import BUCKET, CREDENTIALS, Answer, FakeH3, error

from substantial import (
    ConfigError,
    Context,
    Credentials,
    LayoutError,
    StoreError,
    Substantial,
    workflow,
)
from substantial.stores import ListWait, S3Location, StoreFailure, open_store, s3_store


def test_urls_need_every_part() -> None:
    ok = S3Location.parse("h3+http://127.0.0.1:9/b/ns/sub?region=r")
    assert ok == S3Location("http://127.0.0.1:9", "127.0.0.1:9", "b", "ns/sub", "r", wait=True)
    aws = S3Location.parse("s3://s3.eu-central-1.amazonaws.com/b/ns?region=eu-central-1")
    assert aws.endpoint == "https://s3.eu-central-1.amazonaws.com"
    assert not aws.wait
    assert S3Location.parse("s3+http://[::1]:9000/b/ns?region=r").endpoint == "http://[::1]:9000"
    assert S3Location.parse("h3://h/b/ns?region=r").wait
    assert S3Location.parse(f"s3://h/b/ns?region={'r' * 63}").region == "r" * 63
    for url in [
        "s3://h/b/ns",
        "s3://h/b/ns?region=",
        "s3://h/b/ns?region=r&region=s",
        "s3://h/b/ns?region=us_east",
        "s3://h/b/ns?region=US-EAST-1",
        "s3://h/b/ns?region=-r",
        "s3://h/b/ns?region=r-",
        "s3://h/b/ns?region=us%20east",
        f"s3://h/b/ns?region={'r' * 64}",
        "s3://h/b/ns?region=r&endpoint=http://h",
        "s3://h/b/ns?region=r&wait=true",
        "s3://h/b?region=r",
        "s3://h/b/?region=r",
        "s3://h/?region=r",
        "s3://h?region=r",
        "s3://h//ns?region=r",
        "s3://h/b/ns/?region=r",
        "s3://h/b/a//c?region=r",
        "s3:///b/ns?region=r",
        "s3://k:s@h/b/ns?region=r",
        "s3://@h/b/ns?region=r",
        "s3://:@h/b/ns?region=r",
        "s3://h/b/ns?region=r#x",
        "s3://h:port/b/ns?region=r",
        "s3+https://h/b/ns?region=r",
        "gs://h/b/ns?region=r",
    ]:
        with pytest.raises(ConfigError):
            S3Location.parse(url)


def test_s3_urls_without_the_extra_name_it(monkeypatch: pytest.MonkeyPatch) -> None:
    for package in ("aiohttp", "botocore"):
        monkeypatch.delitem(sys.modules, "substantial.stores.s3_store", raising=False)
        for name in [*(n for n in sys.modules if n.startswith(f"{package}.")), package]:
            monkeypatch.setitem(sys.modules, name, None)
        with pytest.raises(ConfigError, match=r'pip install "substantial\[s3\]"'):
            open_store("h3+http://127.0.0.1:1/b/ns?region=r", Credentials("id", "s"))
        monkeypatch.undo()
    open_store("memory://no-extra", None)


async def test_each_operation_is_one_request() -> None:
    async with FakeH3() as h3:
        store = open_store(h3.url("ns"), CREDENTIALS)
        assert await store.create("q/a", b"a") == "created"
        assert await store.create("q/a", b"b") == "exists"
        assert await store.get("q/a") == b"a"
        assert await store.get("q/b") is None
        assert (await store.list("q/", start_after=None, max_keys=10, wait=None)).keys == ["q/a"]
        await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0))
        await store.delete("q/a")
        await store.close()
        methods = [method for method, _ in h3.requests]
        assert methods == ["PUT", "PUT", "GET", "GET", "GET", "GET", "DELETE"], h3.requests


# What each S3 answer means for each operation, as `(status, code)` injected for the first request
# of the operation: then the outcome and the methods of every request sent.
CREATE_ANSWERS = [
    ((412, "PreconditionFailed"), "exists", ["PUT"]),
    # A concurrent conditional write of the key was in flight: retried.
    ((409, "ConditionalRequestConflict"), "created", ["PUT", "PUT"]),
    ((429, "SlowDown"), "created", ["PUT", "PUT"]),
    # Ambiguous: a get finds the key missing, and the create is sent again.
    ((500, "InternalError"), "created", ["PUT", "GET", "PUT"]),
    ((503, "SlowDown"), "created", ["PUT", "GET", "PUT"]),
    ((408, "RequestTimeout"), "created", ["PUT", "GET", "PUT"]),
    ((403, "AccessDenied"), StoreFailure, ["PUT"]),
    ((400, "InvalidRequest"), StoreFailure, ["PUT"]),
    ((301, "PermanentRedirect"), StoreFailure, ["PUT"]),
]
# For a get, a delete and a listing alike.
READ_ANSWERS = [
    ((404, "NoSuchKey"), "missing", 1),
    ((404, "NoSuchBucket"), StoreFailure, 1),
    ((500, "InternalError"), "retried", 2),
    ((503, "SlowDown"), "retried", 2),
    ((408, "RequestTimeout"), "retried", 2),
    ((409, "OperationAborted"), "retried", 2),
    ((429, "SlowDown"), "retried", 2),
    ((403, "AccessDenied"), StoreFailure, 1),
    ((400, "InvalidArgument"), StoreFailure, 1),
    ((301, "PermanentRedirect"), StoreFailure, 1),
]


@pytest.mark.parametrize(("answer", "outcome", "methods"), CREATE_ANSWERS, ids=lambda c: str(c))
async def test_create_answers(answer: tuple[int, str], outcome: object, methods: list[str]) -> None:
    async with FakeH3() as h3:
        store = open_store(h3.url("ns"), CREDENTIALS)
        h3.inject["PUT"] = [error(*answer)]
        if outcome is StoreFailure:
            with pytest.raises(StoreFailure, match=rf"create q/a: HTTP {answer[0]} {answer[1]}$"):
                await store.create("q/a", b"a")
        else:
            assert await store.create("q/a", b"a") == outcome
        assert [m for m, _ in h3.requests] == methods
        await store.close()


async def test_an_ambiguous_create_that_landed_is_created() -> None:
    async with FakeH3() as h3:
        store = open_store(h3.url("ns"), CREDENTIALS)
        h3.inject["PUT"] = [error(500, "InternalError", applied=True)]
        assert await store.create("q/a", b"a") == "created"
        h3.inject["PUT"] = [error(503, "SlowDown", applied=True)]
        assert await store.create("q/b", b"b") == "created"
        assert await store.create("q/b", b"c") == "exists"
        assert [m for m, _ in h3.requests] == ["PUT", "GET", "PUT", "GET", "PUT"]
        await store.close()


@pytest.mark.parametrize(("answer", "outcome", "sent"), READ_ANSWERS, ids=lambda c: str(c))
@pytest.mark.parametrize("operation", ["get", "delete", "list"])
async def test_get_delete_and_list_answers(
    operation: str, answer: tuple[int, str], outcome: object, sent: int
) -> None:
    async with FakeH3() as h3:
        store = open_store(h3.url("ns"), CREDENTIALS)
        await store.create("q/a", b"a")
        method = {"get": "GET", "delete": "DELETE", "list": "GET"}[operation]
        h3.inject[method] = [error(*answer)]
        before = len(h3.requests)

        async def call() -> object:
            if operation == "get":
                return await store.get("q/a")
            if operation == "delete":
                return await store.delete("q/a")
            return (await store.list("q/", start_after=None, max_keys=10, wait=None)).keys

        missing = outcome == "missing" and operation != "list"
        if outcome is StoreFailure or (outcome == "missing" and operation == "list"):
            with pytest.raises(StoreFailure, match=rf"HTTP {answer[0]} {answer[1]}$"):
                await call()
        elif missing:
            assert await call() is None
        else:
            assert await call() == {"get": b"a", "delete": None, "list": ["q/a"]}[operation]
        assert len(h3.requests) - before == sent
        await store.close()


async def test_an_error_answer_without_an_s3_error_is_named_by_its_status() -> None:
    async with FakeH3() as h3:
        store = open_store(h3.url("ns"), CREDENTIALS)
        h3.inject["GET"] = [Answer(403, b"<html>no</html")]
        with pytest.raises(StoreFailure, match=r"get q/a: HTTP 403 403: Forbidden$"):
            await store.get("q/a")
        await store.close()


async def test_requests_sign_their_body_and_carry_no_checksum() -> None:
    async with FakeH3() as h3:
        store = open_store(h3.url("ns"), CREDENTIALS)
        await store.create("q/a", b"abc")
        await store.get("q/a")
        put, get = h3.headers
        assert put["x-amz-content-sha256"] == hashlib.sha256(b"abc").hexdigest()
        assert get["x-amz-content-sha256"] == hashlib.sha256(b"").hexdigest()
        for headers in (put, get):
            checksums = [n for n in headers if n.startswith("x-amz-checksum") or "md5" in n]
            assert not checksums
            assert "x-amz-sdk-checksum-algorithm" not in headers
            assert "expect" not in headers
            assert headers["accept-encoding"] == "identity"
        signed = put["authorization"].split("SignedHeaders=")[1].split(",")[0].split(";")
        assert signed == ["host", "if-none-match", "x-amz-content-sha256", "x-amz-date"]
        await store.close()


def test_tls_is_verified_against_the_authorities_of_botocore(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    for name in ("SSL_CERT_FILE", "SSL_CERT_DIR", "AWS_CA_BUNDLE"):
        monkeypatch.setenv(name, str(tmp_path / "none.pem"))
    store = open_store("s3://s3.us-east-1.amazonaws.com/b/ns?region=us-east-1", CREDENTIALS)
    bundle = ssl.create_default_context(cafile=get_cert_path(True)).get_ca_certs()
    tls = store._tls  # type: ignore[attr-defined]
    assert bundle
    assert tls.get_ca_certs() == bundle
    assert tls.verify_mode == ssl.CERT_REQUIRED
    assert tls.check_hostname


async def test_a_session_token_is_signed() -> None:
    async with FakeH3() as h3:
        keys = Credentials("fake", "fake-secret", session_token="token")
        store = open_store(h3.url("ns"), keys)
        await store.get("q/a")
        (headers,) = h3.headers
        assert headers["x-amz-security-token"] == "token"
        assert "x-amz-security-token" in headers["authorization"]
        await store.close()


PROXY_VARIABLES = ["HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY"]


async def test_the_endpoint_is_reached_through_the_proxy_the_environment_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in PROXY_VARIABLES:
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.lower(), raising=False)
    async with FakeH3() as proxy:
        port = proxy.url("ns").split(":")[2].split("/")[0]
        monkeypatch.setenv("HTTP_PROXY", f"http://user:p%40ss@127.0.0.1:{port}")
        store = open_store(f"s3+http://substantial.invalid:9/{BUCKET}/ns?region=r", CREDENTIALS)
        assert await store.create("q/a", b"a") == "created"
        await store.close()
        assert proxy.requests == [("PUT", f"http://substantial.invalid:9/{BUCKET}/ns/q/a")]
        (headers,) = proxy.headers
        assert headers["proxy-authorization"] == "Basic " + base64.b64encode(b"user:p@ss").decode()
        assert headers["host"] == "substantial.invalid:9"
        # A proxy that no one answers, bypassed for the endpoint.
        monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9")
        monkeypatch.setenv("NO_PROXY", "127.0.0.1")
        direct = open_store(proxy.url("ns"), CREDENTIALS)
        assert await direct.get("q/a") == b"a"
        await direct.close()


async def test_keys_with_reserved_characters_round_trip() -> None:
    store = open_store(require_s3(), "env")
    keys = sorted(["q/a b", "q/a+b", "q/a%2Bb", "q/a&b=c?d", "q/é~:@,;", "q/a#b"])
    for key in keys:
        assert await store.create(key, key.encode()) == "created"
        assert await store.get(key) == key.encode()
    listed = await store.list("q/", start_after=None, max_keys=10, wait=None)
    assert listed.keys == sorted(keys, key=str.encode)
    after = await store.list("q/", start_after=listed.keys[0], max_keys=10, wait=None)
    assert after.keys == listed.keys[1:]
    for key in keys:
        await store.delete(key)
        assert await store.get(key) is None
    await store.close()


@contextlib.asynccontextmanager
async def raw_server(answer: bytes) -> AsyncIterator[tuple[str, list[str]]]:
    """A server on 127.0.0.1 answering every request with `answer`, then closing the connection;
    yields its URL and the method of every request."""
    methods: list[str] = []

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        line = await reader.readline()
        while await reader.readline() not in (b"\r\n", b"\n", b""):
            pass
        methods.append(line.decode().split(" ")[0])
        writer.write(answer)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"s3+http://127.0.0.1:{port}/b/ns?region=us-east-1", methods
    finally:
        server.close()


async def test_a_body_cut_short_is_retried_as_transient() -> None:
    async with raw_server(b"HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\nabc") as (url, methods):
        store = open_store(url, CREDENTIALS)
        with pytest.raises(StoreFailure, match=r"\(after 5 attempts\)"):
            await store.get("k")
        assert methods == ["GET"] * 5
        await store.close()


async def test_a_listing_without_is_truncated_is_complete() -> None:
    body = (
        b'<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>'
        b"<Contents><Key>ns/q/a</Key></Contents></ListBucketResult>"
    )
    answer = b"HTTP/1.1 200 OK\r\ncontent-length: %d\r\n\r\n" % len(body) + body
    async with raw_server(answer) as (url, methods):
        store = open_store(url, CREDENTIALS)
        listing = await store.list("q/", start_after=None, max_keys=10, wait=None)
        assert listing.keys == ["q/a"]
        assert methods == ["GET"]
        await store.close()


# Settings of the AWS environment and shared files that would change how the SDK reaches the
# endpoint, the profile's `s3` section included.
AMBIENT = [
    {"AWS_PROFILE": "absent"},
    {"AWS_CONFIG_FILE": "accelerate"},
    {"AWS_USE_FIPS_ENDPOINT": "true"},
    {"AWS_USE_DUALSTACK_ENDPOINT": "true"},
]


@pytest.mark.parametrize("ambient", AMBIENT, ids=lambda a: next(iter(a)))
async def test_the_aws_environment_and_files_are_not_read(
    ambient: dict[str, str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = tmp_path / "config"
    config.write_text("[default]\ns3 =\n  use_accelerate_endpoint = true\n")
    for name, value in ambient.items():
        monkeypatch.setenv(name, str(config) if value == "accelerate" else value)
    async with FakeH3() as h3:
        store = open_store(h3.url("ambient"), CREDENTIALS)
        assert await store.create("q/a", b"a") == "created"
        assert await store.get("q/a") == b"a"
        await store.close()


async def test_create_and_open_run_the_probe() -> None:
    url = require_s3()
    with pytest.raises(LayoutError):
        await Substantial.open(url, credentials="env")
    created = await Substantial.create(url, credentials="env")
    opened = await Substantial.open(url, credentials="env")
    await created.close()
    await opened.close()
    store = open_store(url, "env")
    probes = await store.list("probe/", start_after=None, max_keys=10, wait=None)
    assert probes.keys == ["probe/exclusive"], "one probe key, kept"
    await store.close()


async def test_wrong_credentials_are_a_store_error() -> None:
    url = require_s3()
    wrong = Credentials(os.environ["AWS_ACCESS_KEY_ID"], "not-the-secret")
    with pytest.raises(StoreError, match=r"HTTP 403 SignatureDoesNotMatch: [^<]*$"):
        await Substantial.create(url, credentials=wrong)


async def test_a_missing_bucket_is_a_store_error_not_a_missing_key() -> None:
    url = require_s3()
    scheme, _, rest = url.partition("://")
    endpoint, _, path = rest.partition("/")
    _, _, ns = path.partition("/")
    store = open_store(f"{scheme}://{endpoint}/substantial-no-such-bucket/{ns}", "env")
    with pytest.raises(StoreFailure):
        await store.get("runs/a/start")
    with pytest.raises(StoreFailure):
        await store.delete("runs/a/start")
    await store.close()


async def test_listings_go_past_one_page() -> None:
    store = open_store(require_s3(), "env")
    keys = [f"due/{i:05}" for i in range(1005)]
    gate = asyncio.Semaphore(32)

    async def create(key: str) -> str:
        async with gate:
            return await store.create(key, b"")

    assert set(await asyncio.gather(*(create(k) for k in keys))) == {"created"}
    listed = await store.list("due/", start_after=None, max_keys=2000, wait=None)
    assert listed.keys == keys
    after = await store.list("due/", start_after="due/00002", max_keys=1001, wait=None)
    assert after.keys == keys[3:1004]
    assert (await store.list("due/", start_after=None, max_keys=0, wait=None)).keys == []
    await store.close()


async def test_h3_needs_the_listing_etag_of_a_waiting_listing() -> None:
    store = open_store(require_s3("h3"), "env")
    assert store.capabilities.wait
    await store.create("q/a", b"a")
    assert (await store.list("q/", start_after=None, max_keys=10, wait=None)).keys == ["q/a"]
    with pytest.raises(StoreFailure, match="x-substantial-listing-etag"):
        await store.list("q/", start_after=None, max_keys=10, wait=ListWait(seconds=1))
    await store.close()


async def test_waiting_listings_send_and_answer_the_wait_headers() -> None:
    async with FakeH3() as h3:
        store = open_store(h3.url("ns"), CREDENTIALS)
        await store.create("q/a&b", b"x")
        first = await store.list("q/", start_after=None, max_keys=10, wait=ListWait(5))
        assert first.keys == ["q/a&b"]
        assert first.etag is not None
        assert "x-substantial-wait-etag" not in h3.listed[-1].headers
        listing = await store.list("q/", start_after=None, max_keys=10, wait=ListWait(0, "e1"))
        assert listing == first
        headers = h3.listed[-1].headers
        assert (headers["x-substantial-wait-seconds"], headers["x-substantial-wait-etag"]) == (
            "0",
            "e1",
        )
        signed = headers["authorization"].split("SignedHeaders=")[1].split(",")[0].split(";")
        assert {"x-substantial-wait-seconds", "x-substantial-wait-etag"} <= set(signed)
        sent = len(h3.requests)
        for bad in [ListWait(21), ListWait(5, "e\n1")]:
            with pytest.raises(StoreFailure):
                await store.list("q/", start_after=None, max_keys=10, wait=bad)
        with pytest.raises(StoreFailure):
            await store.list("q/", start_after=None, max_keys=1001, wait=ListWait(5))
        assert len(h3.requests) == sent, "refused before any request"
        s3 = open_store(h3.url("ns").replace("h3+http", "s3+http"), CREDENTIALS)
        with pytest.raises(StoreFailure):
            await s3.list("q/", start_after=None, max_keys=10, wait=ListWait(5))
        await store.close()
        with pytest.raises(StoreFailure, match="closed"):
            await store.get("q/a&b")
        await s3.close()


def env_keys(expires_in: timedelta | None, secret: str | None = None) -> Credentials:
    expires_at = None if expires_in is None else datetime.now(UTC) + expires_in
    return Credentials(
        os.environ["AWS_ACCESS_KEY_ID"],
        secret or os.environ["AWS_SECRET_ACCESS_KEY"],
        expires_at=expires_at,
    )


async def test_a_provider_is_asked_again_near_the_expiry_of_its_keys() -> None:
    url = require_s3()
    calls: list[int] = []

    def wrong_then_right() -> Credentials:
        calls.append(1)
        # Within the 5 minutes before expiry: every request asks again.
        return env_keys(timedelta(minutes=1), "not-the-secret" if len(calls) == 1 else None)

    store = open_store(url, wrong_then_right)
    with pytest.raises(StoreFailure, match="SignatureDoesNotMatch"):
        await store.get("runs/a/start")
    assert await store.get("runs/a/start") is None
    assert await store.create("runs/a/start", b"x") == "created"
    assert len(calls) == 3, "once per request"
    await store.close()


@pytest.mark.parametrize("expires_in", [None, timedelta(hours=1)])
async def test_a_provider_is_asked_once_for_keys_far_from_expiry(
    expires_in: timedelta | None,
) -> None:
    url = require_s3()
    calls: list[int] = []

    async def provider() -> Credentials:
        calls.append(1)
        return env_keys(expires_in)

    sub = await Substantial.create(url, credentials=provider)

    @workflow
    async def answer(c: Context) -> int:
        return 42

    async with script(sub, [answer]):
        assert await (await sub.start(answer)).result(timeout=LONG) == 42
    await sub.close()
    assert len(calls) == 1


async def test_a_provider_that_raises_fails_the_request_and_is_asked_again() -> None:
    url = require_s3()
    calls: list[int] = []

    def flaky() -> Credentials:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("metadata endpoint down")
        return env_keys(None)

    store = open_store(url, flaky)
    with pytest.raises(StoreFailure, match="credentials provider: RuntimeError: metadata"):
        await store.get("runs/a/start")
    assert await store.get("runs/a/start") is None
    assert len(calls) == 2
    await store.close()


async def test_a_provider_that_never_answers_fails_each_attempt_within_its_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(s3_store, "ATTEMPT_TIMEOUT", 0.05)
    calls: list[int] = []

    async def hung() -> Credentials:
        calls.append(1)
        await asyncio.Event().wait()
        raise AssertionError("never answers")

    store = open_store("s3+http://127.0.0.1:1/b/ns?region=r", hung)
    with pytest.raises(StoreFailure, match=r"TimeoutError.*\(after 5 attempts\)"):
        await store.get("k")
    assert len(calls) == 5, "asked again at each attempt"
    await store.close()


async def test_providers_and_their_keys_are_checked() -> None:
    later = datetime.now(UTC) + timedelta(hours=1)
    with pytest.raises(ConfigError, match="aware datetime"):
        Credentials("id", "s", expires_at=later.replace(tzinfo=None))
    s3 = "s3+http://127.0.0.1:1/b/ns?region=r"
    with pytest.raises(ConfigError, match="a provider returns"):
        open_store(s3, Credentials("id", "s", expires_at=later))
    store = open_store(s3, lambda: ("id", "s"))  # type: ignore[arg-type, return-value]
    with pytest.raises(StoreFailure, match="returns Credentials, not"):
        await store.get("k")
    await store.close()
