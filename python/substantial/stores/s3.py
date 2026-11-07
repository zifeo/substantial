"""S3 store URLs and waiting listings; the adapter is in `s3_store`, which needs the `s3` extra."""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlsplit

from substantial.errors import ConfigError
from substantial.stores.base import ListWait, Store, StoreFailure
from substantial.stores.retry import Permanent

# ListObjectsV2 returns at most this many keys a page.
PAGE = 1000
CONNECT_TIMEOUT = 5.0
ATTEMPT_TIMEOUT = 60.0
WAIT_SECONDS = "x-substantial-wait-seconds"
WAIT_ETAG = "x-substantial-wait-etag"
LISTING_ETAG = "x-substantial-listing-etag"
MAX_WAIT_SECONDS = 20
# A region as AWS names them, a DNS label of lowercase letters, digits and hyphens.
_REGION = re.compile(r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?")
_REGION_RULE = (
    "1 to 63 lowercase letters, digits and hyphens, neither starting nor ending with a hyphen"
)
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
        if not _REGION.fullmatch(region):
            raise invalid(f"the region {region!r} is not {_REGION_RULE}")
        if query:
            raise invalid(f"unknown query parameter {next(iter(query))}")
        return S3Location(f"{protocol}://{host}", host, bucket, namespace, region, wait)


def wait_headers(wait: ListWait | None, max_keys: int) -> dict[str, str]:
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
