"""Store adapters and the store contract (I-26)."""

from __future__ import annotations

import logging

from substantial._proto import boundary
from substantial.errors import ConfigError
from substantial.stores.attempts import measured
from substantial.stores.base import (
    Capabilities,
    Cas,
    Create,
    Created,
    Listing,
    ListWait,
    Object,
    Store,
    StoreFailure,
    Written,
)
from substantial.stores.credentials import Credentials, CredentialsArg, Password, refuse
from substantial.stores.fs import FsStore
from substantial.stores.memory import MemoryStore
from substantial.stores.redis import SCHEMES as REDIS_SCHEMES
from substantial.stores.redis import RedisLocation
from substantial.stores.s3 import S3Location

__all__ = [
    "Capabilities",
    "Cas",
    "Create",
    "Created",
    "Credentials",
    "FsStore",
    "ListWait",
    "Listing",
    "MemoryStore",
    "Object",
    "Password",
    "RedisLocation",
    "S3Location",
    "Store",
    "StoreFailure",
    "Written",
    "open_store",
    "perform",
]

log = logging.getLogger("substantial")


def _namespace(url: str, rest: str) -> str:
    # § e9ern38bz.b@c05pc
    if "?" in rest:
        raise ConfigError(f"{url!r} takes no query parameters")
    if not rest:
        raise ConfigError(f"{url!r} names no namespace")
    return rest


def open_store(url: str, credentials: CredentialsArg) -> Store:
    """Opens the adapter a URL names (I-16); the scheme alone picks it, nothing is detected."""
    # § e9ern38bz@4sb7t
    if not isinstance(url, str):
        raise ConfigError(f"a store is a URL, not {url!r}")
    if url.startswith("memory://"):
        ns = _namespace(url, url.removeprefix("memory://"))
        refuse(url, credentials)
        return MemoryStore(ns)
    if url.startswith("file:"):
        rest = url.removeprefix("file:")
        path = _namespace(url, rest.removeprefix("//"))
        if path.startswith("~"):
            raise ConfigError(f"{url!r}: give an absolute or relative path, ~ is not expanded")
        refuse(url, credentials)
        return FsStore(path)
    if url.startswith(("s3://", "s3+http://", "h3://", "h3+http://")):
        location = S3Location.parse(url)
        try:
            from substantial.stores.s3_store import S3Store
        except ModuleNotFoundError as e:
            if e.name is None or e.name.partition(".")[0] not in ("aiohttp", "botocore", "yarl"):
                raise
            raise ConfigError(
                f'{url!r} needs aiohttp and botocore, which `pip install "substantial[s3]"` '
                "installs"
            ) from e
        return S3Store(location, credentials)
    if url.startswith(tuple(f"{scheme}://" for scheme in REDIS_SCHEMES)):
        location = RedisLocation.parse(url)
        try:
            from substantial.stores.redis_store import RedisStore
        except ModuleNotFoundError as e:
            if e.name is None or e.name.partition(".")[0] != "redis":
                raise
            raise ConfigError(
                f'{url!r} needs redis-py, which `pip install "substantial[redis]"` installs'
            ) from e
        return RedisStore(location, credentials)
    for scheme in ("sqlite:", "postgres://", "mysql://"):
        if url.startswith(scheme):
            raise ConfigError(f"{scheme} stores are not available in this host yet; {_SUPPORTED}")
    # § one e9ern38bz.a@3rh9b
    raise ConfigError(f"unknown scheme in {url!r}; {_SUPPORTED}")


_SUPPORTED = (
    "the supported schemes are memory://, file:, s3://, s3+http://, h3://, h3+http://, "
    "redis://, rediss://, redis+sentinel://, rediss+sentinel://, redis+cluster:// and "
    "rediss+cluster://"
)


async def _perform(store: Store, op: boundary.StoreOp) -> boundary.OpResult:
    empty = boundary.Empty()
    which = op.WhichOneof("op")
    if which == "create":
        c = op.create
        expiry = c.expiry_days if c.HasField("expiry_days") else None
        created = await store.create(c.key, c.bytes, expiry=expiry)
        if created == "exists":
            return boundary.OpResult(exists=empty)
        return boundary.OpResult(created=boundary.OpResult.Written(etag=created.etag))
    if which == "cas":
        c = op.cas
        expiry = c.expiry_days if c.HasField("expiry_days") else None
        written = await store.cas(c.key, c.bytes, c.etag, expiry=expiry)
        if written == "mismatch":
            return boundary.OpResult(mismatch=empty)
        return boundary.OpResult(written=boundary.OpResult.Written(etag=written.etag))
    if which == "get":
        found = await store.get(op.get)
        if found is None:
            return boundary.OpResult(missing=empty)
        return boundary.OpResult(object=boundary.OpResult.Object(bytes=found.data, etag=found.etag))
    if which == "list":
        ls = op.list
        wait = None
        if ls.HasField("wait"):
            wait = ListWait(ls.wait.seconds, ls.wait.etag if ls.wait.HasField("etag") else None)
        start_after = ls.start_after if ls.HasField("start_after") else None
        listing = await store.list(
            ls.prefix, start_after=start_after, max_keys=ls.max_keys, wait=wait
        )
        return boundary.OpResult(
            listing=boundary.OpResult.Listing(keys=listing.keys, etag=listing.etag)
        )
    if which == "delete":
        await store.delete(op.delete)
        return boundary.OpResult(deleted=empty)
    raise StoreFailure("empty store op from the core")


async def perform(store: Store, op: boundary.StoreOp) -> boundary.OpResult:
    """Performs one `StoreOp` of the core and answers with its `OpResult`, with the requests the
    adapter sent."""
    with measured() as sent:
        result = await _perform_once(store, op)
    result.attempts = sent.n
    return result


async def _perform_once(store: Store, op: boundary.StoreOp) -> boundary.OpResult:
    # § pgv9r1j12.b@jq4kv
    try:
        return await _perform(store, op)
    except StoreFailure as e:
        message = e.message
    except Exception as e:
        # An adapter's bug must not leave the core waiting for an answer that never comes.
        log.exception("store adapter failed on %s", op.WhichOneof("op"))
        message = f"{type(e).__name__}: {e}"
    return boundary.OpResult(failed=boundary.StoreFailure(message=message))
