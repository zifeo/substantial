"""What the S3 adapter does beyond the shared contract; all but URL parsing and signing run only
when `SUBSTANTIAL_S3_URL` is set (see `dev/rustfs.compose.yaml`)."""

import asyncio
import os
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from conftest import LONG, require_s3, script

from substantial import (
    ConfigError,
    Context,
    Credentials,
    LayoutError,
    StoreError,
    Substantial,
    workflow,
)
from substantial.stores import ListWait, S3Location, StoreFailure, open_store
from substantial.stores.sigv4 import sign


def test_urls_need_every_part() -> None:
    ok = S3Location.parse("h3+http://127.0.0.1:9/b/ns/sub?region=r")
    assert ok == S3Location("http://127.0.0.1:9", "127.0.0.1:9", "b", "ns/sub", "r", wait=True)
    aws = S3Location.parse("s3://s3.eu-central-1.amazonaws.com/b/ns?region=eu-central-1")
    assert aws.endpoint == "https://s3.eu-central-1.amazonaws.com"
    assert not aws.wait
    assert S3Location.parse("s3+http://[::1]:9000/b/ns?region=r").endpoint == "http://[::1]:9000"
    assert S3Location.parse("h3://h/b/ns?region=r").wait
    for url in [
        "s3://h/b/ns",
        "s3://h/b/ns?region=",
        "s3://h/b/ns?region=r&region=s",
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
        "s3://h/b/ns?region=r#x",
        "s3://h:port/b/ns?region=r",
        "s3+https://h/b/ns?region=r",
        "gs://h/b/ns?region=r",
    ]:
        with pytest.raises(ConfigError):
            S3Location.parse(url)


def test_signature_matches_the_aws_example() -> None:
    # The GET Object example of the S3 SigV4 documentation, with its credentials and time.
    credentials = Credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    headers = sign(
        method="GET",
        host="examplebucket.s3.amazonaws.com",
        path="/test.txt",
        query="",
        headers={"range": "bytes=0-9"},
        payload=b"",
        credentials=credentials,
        region="us-east-1",
        at=datetime(2013, 5, 24, tzinfo=UTC),
    )
    expected = "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
    assert headers["authorization"].endswith(f"Signature={expected}")


async def test_create_and_open_run_the_probe() -> None:
    url = require_s3()
    with pytest.raises(LayoutError):
        await Substantial.open(url, credentials="env")
    created = await Substantial.create(url, partitions=2, credentials="env")
    assert created.partitions == 2
    opened = await Substantial.open(url, credentials="env")
    assert opened.partitions == 2
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
        await Substantial.create(url, partitions=1, credentials=wrong)


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
    seen: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        body = (
            "<ListBucketResult><IsTruncated>false</IsTruncated>"
            "<Contents><Key>ns/q/a</Key></Contents></ListBucketResult>"
        )
        return httpx.Response(200, text=body, headers={"x-substantial-listing-etag": "e2"})

    store = open_store("h3+http://h:1/b/ns?region=r", Credentials("id", "s"))
    await store.close()
    store._client = httpx.AsyncClient(transport=httpx.MockTransport(answer))  # type: ignore[attr-defined]
    listing = await store.list("q/", start_after=None, max_keys=10, wait=ListWait(20, "e1"))
    assert listing.keys == ["q/a"]
    assert listing.etag == "e2"
    headers = seen[0].headers
    assert (headers["x-substantial-wait-seconds"], headers["x-substantial-wait-etag"]) == (
        "20",
        "e1",
    )
    signed = headers["authorization"].split("SignedHeaders=")[1].split(",")[0].split(";")
    assert {"x-substantial-wait-seconds", "x-substantial-wait-etag"} <= set(signed)
    first = await store.list("q/", start_after=None, max_keys=10, wait=ListWait(5))
    assert "x-substantial-wait-etag" not in seen[1].headers
    assert first.etag == "e2"
    for bad in [ListWait(21), ListWait(5, "e\n1")]:
        with pytest.raises(StoreFailure):
            await store.list("q/", start_after=None, max_keys=10, wait=bad)
    with pytest.raises(StoreFailure):
        await store.list("q/", start_after=None, max_keys=1001, wait=ListWait(5))
    assert len(seen) == 2, "refused before any request"
    s3 = open_store("s3+http://h:1/b/ns?region=r", Credentials("id", "s"))
    with pytest.raises(StoreFailure):
        await s3.list("q/", start_after=None, max_keys=10, wait=ListWait(5))
    await store.close()
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

    sub = await Substantial.create(url, partitions=1, credentials=provider)

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
