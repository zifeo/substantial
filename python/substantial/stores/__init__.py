"""Store adapters and the store contract (I-26)."""

from __future__ import annotations

import logging

from substantial._proto import boundary
from substantial.errors import ConfigError
from substantial.stores.base import (
    Capabilities,
    Create,
    Listing,
    ListWait,
    Store,
    StoreFailure,
)
from substantial.stores.credentials import Credentials, CredentialsArg, Password, refuse
from substantial.stores.fs import FsStore
from substantial.stores.memory import MemoryStore
from substantial.stores.redis import SCHEMES as REDIS_SCHEMES
from substantial.stores.redis import RedisLocation
from substantial.stores.s3 import S3Location

__all__ = [
    "Capabilities",
    "Create",
    "Credentials",
    "FsStore",
    "ListWait",
    "Listing",
    "MemoryStore",
    "Password",
    "RedisLocation",
    "S3Location",
    "Store",
    "StoreFailure",
    "open_store",
    "perform",
]

log = logging.getLogger("substantial")


def _namespace(url: str, rest: str) -> str:
    if "?" in rest:
        raise ConfigError(f"{url!r} takes no query parameters")
    if not rest:
        raise ConfigError(f"{url!r} names no namespace")
    return rest


def open_store(url: str, credentials: CredentialsArg) -> Store:
    """Opens the adapter a URL names (I-16); the scheme alone picks it, nothing is detected."""
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
            if e.name is None or e.name.partition(".")[0] != "aiobotocore":
                raise
            raise ConfigError(
                f'{url!r} needs aiobotocore, which `pip install "substantial[s3]"` installs'
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
        if await store.create(op.create.key, op.create.bytes) == "created":
            return boundary.OpResult(created=empty)
        return boundary.OpResult(exists=empty)
    if which == "get":
        data = await store.get(op.get)
        return boundary.OpResult(missing=empty) if data is None else boundary.OpResult(bytes=data)
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
    """Performs one `StoreOp` of the core and answers with its `OpResult`."""
    try:
        return await _perform(store, op)
    except StoreFailure as e:
        message = e.message
    except Exception as e:
        # An adapter's bug must not leave the core waiting for an answer that never comes.
        log.exception("store adapter failed on %s", op.WhichOneof("op"))
        message = f"{type(e).__name__}: {e}"
    return boundary.OpResult(failed=boundary.StoreFailure(message=message))
