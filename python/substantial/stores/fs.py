from __future__ import annotations

import asyncio
import itertools
import logging
import os
import secrets
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from substantial.errors import ConfigError, StoreError
from substantial.stores import retry
from substantial.stores.base import (
    DAY_MS,
    Capabilities,
    Cas,
    Create,
    Created,
    Listing,
    ListWait,
    Object,
    Written,
    check_all_days,
    check_days,
    in_range,
    refuse_wait,
)
from substantial.stores.memory import Clock, wall_clock
from substantial.stores.retry import STORE_RETRY, Ambiguous, Permanent, Transient

TMP = ".tmp"
# The bytes before a version's value: the instant it expires at, ms since the epoch big-endian, 0
# for none, `TOMBSTONE` for a tombstone, then the key's incarnation, drawn at each create, which
# an ETag names so that no ETag of a removed key matches the key created again.
HEADER = 24
# The header of a tombstone, the version a delete or the sweep writes.
TOMBSTONE = -1
# A version is removed only once its successor is this old, and a tombstone, latest of its key,
# once it is this old itself; an operation that decides on a read stops and reads again once it
# is this old: an older read could predate the removal of a version it relies on.
SUPERSEDED_KEPT_S = 600.0

log = logging.getLogger("substantial")


def _component_error(c: str) -> str | None:
    if c == "":
        return "empty"
    if c in (".", ".."):
        return "relative"
    if len(c.encode()) > 255:
        return "longer than 255 bytes"
    if any("A" <= ch <= "Z" for ch in c):
        return "uppercase, which a file system may fold"
    if "\0" in c:
        return "holds a NUL byte"
    if len(c) > 1 and c[0] == "." and c[1:].isascii() and c[1:].isdigit():
        return "a dot and digits, the name of a version"
    return None


def check_key(key: str) -> None:
    for i, c in enumerate(key.split("/")):
        why = _component_error(c) or (
            "reserved for temporary files" if i == 0 and c == TMP else None
        )
        if why is not None:
            raise Permanent(f"invalid key component {c!r}: {why}")


def _is_component(name: str) -> bool:
    return _component_error(name) is None


def _sync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _ensure_dirs(root: Path, directory: Path) -> bool:
    """Creates the missing directories between `root` and `directory`, each fsynced in its
    parent; false when a sweep removed one of them meanwhile, for the caller to try again."""
    # § c7pz5rk26.b@petmz
    missing = []
    at = directory
    while at != root and not at.is_dir():
        missing.append(at)
        at = at.parent
    for d in reversed(missing):
        try:
            d.mkdir()
        except FileExistsError:
            pass
        except NotADirectoryError as e:
            raise Permanent(f"{d} lies under another key: {e}") from e
        except OSError as e:
            # macOS answers EINVAL in a directory being removed.
            if not d.parent.is_dir():
                return False
            raise Transient(f"create directory {d}: {e}") from e
        try:
            _sync_dir(d.parent)
        except FileNotFoundError:
            return False
        except OSError as e:
            raise Transient(f"sync directory {d.parent}: {e}") from e
    return True


def _expires(expiry: int | None, now: int) -> int:
    """The instant a write of `expiry` days at `now` expires at, 0 for none."""
    return 0 if expiry is None else now + expiry * DAY_MS


def _version(n: int) -> str:
    """The name of version `n` in its key's directory, never a key component."""
    return f".{n:020}"


def _version_of(name: str) -> int | None:
    digits = name[1:]
    if name[:1] == "." and len(digits) == 20 and digits.isascii() and digits.isdigit():
        return int(digits)
    return None


def _etag(inc: bytes, n: int) -> str:
    """The ETag of version `n` of the incarnation `inc`."""
    return f"{inc.hex()}.{n}"


def _parse_etag(etag: str) -> tuple[bytes, int] | None:
    inc, sep, n = etag.partition(".")
    if not sep or len(inc) != 32 or not (n.isascii() and n.isdigit()):
        return None
    try:
        return bytes.fromhex(inc), int(n)
    except ValueError:
        return None


def _versions(directory: Path) -> list[int]:
    """The versions the directory of a key lists, lowest first."""
    try:
        names = os.listdir(directory)
    except (FileNotFoundError, NotADirectoryError):
        return []
    except OSError as e:
        raise Transient(f"list {directory}: {e}") from e
    return sorted(n for n in map(_version_of, names) if n is not None)


@dataclass(frozen=True)
class _Version:
    data: bytes
    # The expiry instant, 0 for none, `TOMBSTONE` for a tombstone.
    at: int
    inc: bytes


def _read(path: Path) -> _Version | None:
    """A version, `None` when it is missing."""
    # § qf98sbvzj@gf7gq
    try:
        file = path.read_bytes()
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as e:
        raise Transient(f"read {path}: {e}") from e
    if len(file) < HEADER:
        raise Permanent(f"{path} is not a file of this store: shorter than its header")
    return _Version(file[HEADER:], int.from_bytes(file[:8], "big", signed=True), file[8:HEADER])


def _exists(path: Path) -> bool:
    try:
        os.lstat(path)
    except (FileNotFoundError, NotADirectoryError):
        return False
    except OSError as e:
        raise Transient(f"stat {path}: {e}") from e
    return True


def _gone(at: int, now: int) -> bool:
    """Whether a version of header `at` reads as missing at `now`: a tombstone, or past its
    expiry."""
    return at == TOMBSTONE or (at > 0 and now >= at)


def _latest(directory: Path) -> tuple[int, _Version] | None:
    """The latest version of a key and its number, read so that no later one existed when it
    answers.

    A version goes only once its successor is `SUPERSEDED_KEPT_S` old, the lowest first, so the
    latest one and every one written during the listing stay: the listing shows the latest at its
    start, and the next ones are found by name. A version gone when read was superseded, and is
    listed again."""
    # § j4b9rkhg8.c@nk93h
    while True:
        started = time.monotonic()
        listed = _versions(directory)
        if not listed:
            return None
        n = listed[-1]
        while True:
            found = _read(directory / _version(n))
            if found is None:
                break
            if _exists(directory / _version(n + 1)):
                n += 1
                continue
            if time.monotonic() - started >= SUPERSEDED_KEPT_S:
                break
            return n, found


def _write_temporary(root: Path, data: bytes, at: int, inc: bytes) -> Path:
    """The value as a version file holds it: its header first, so all of it lands together."""
    tmp = root / TMP / secrets.token_hex(16)
    try:
        with open(tmp, "xb") as f:
            f.write(at.to_bytes(8, "big", signed=True))
            f.write(inc)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
    except OSError as e:
        tmp.unlink(missing_ok=True)
        raise Transient(f"write {tmp}: {e}") from e
    return tmp


# What linking a version found: linked, the version taken, or the key's directory gone.
Linked = Literal["linked", "taken", "no directory"]


def _link(tmp: Path, directory: Path, n: int) -> Linked:
    """Writes version `n` of the key directory by linking `tmp` to it, which fails if the version
    exists."""
    # § j4b9rkhg8@186tx
    path = directory / _version(n)
    try:
        os.link(tmp, path)
    except FileExistsError:
        return "taken"
    except NotADirectoryError as e:
        raise Permanent(f"{directory} lies under another key: {e}") from e
    except OSError as e:
        # macOS answers EINVAL in a directory being removed.
        if not directory.is_dir():
            return "no directory"
        # On NFS a link that succeeded may still report an error.
        raise Ambiguous(f"link {path}: {e}") from e
    try:
        # § one kkad6aqxp.c@r0n98
        _sync_dir(directory)
    except OSError as e:
        raise Ambiguous(f"sync directory {directory}: {e}") from e
    return "linked"


def _linked_at(path: Path) -> int | None:
    """When the file at `path` was linked, by the store's clock: its ctime, which a link sets."""
    try:
        return os.lstat(path).st_ctime_ns // 1_000_000
    except FileNotFoundError:
        return None
    except OSError as e:
        raise Transient(f"stat {path}: {e}") from e


def _clean(directory: Path, now: int) -> None:
    """Removes the versions below the latest whose successor is `SUPERSEDED_KEPT_S` old at `now`,
    the store's clock, the lowest first, so a version is gone only once every version below it
    is."""
    # § j4b9rkhg8.b@rs6ny
    listed = _versions(directory)
    kept = int(SUPERSEDED_KEPT_S * 1000)
    for low, high in itertools.pairwise(listed):
        linked = _linked_at(directory / _version(high))
        if high != low + 1 or linked is None or linked > now - kept:
            return
        try:
            (directory / _version(low)).unlink()
        except FileNotFoundError:
            pass
        except OSError as e:
            raise Transient(f"delete {directory}: {e}") from e


def _write_after_latest(
    root: Path,
    directory: Path,
    tmp: Path,
    now: int,
    decide: Callable[[_Version | None], bool],
) -> int | None:
    """Links `tmp` as the version after the latest of `directory`, given the latest when it reads
    as present, or as version 0 of a directory made again when a sweep removed it; `None` when
    `decide` refuses."""
    while True:
        if not _ensure_dirs(root, directory):
            continue
        started = time.monotonic()
        latest = _latest(directory)
        live = None if latest is None or _gone(latest[1].at, now) else latest[1]
        if not decide(live):
            return None
        target = 0 if latest is None else latest[0] + 1
        if time.monotonic() - started >= SUPERSEDED_KEPT_S:
            continue
        if _link(tmp, directory, target) == "linked":
            return target


def _create(root: Path, key: str, data: bytes, at: int, now: int) -> Create:
    # § c7pz5rk26@mkf47
    check_key(key)
    directory = root / key
    # § one j4b9rkhg8.d@bg4h0
    inc = secrets.token_bytes(16)
    tmp = _write_temporary(root, data, at, inc)
    try:
        created = _write_after_latest(root, directory, tmp, now, lambda live: live is None)
    finally:
        tmp.unlink(missing_ok=True)
    if created is None:
        return "exists"
    _clean(directory, now)
    return Created(_etag(inc, created))


def _cas(root: Path, key: str, data: bytes, etag: str, at: int, now: int) -> Cas:
    # § j4b9rkhg8@5cfxr
    check_key(key)
    directory = root / key
    parsed = _parse_etag(etag)
    if parsed is None:
        return "mismatch"
    inc, n = parsed
    tmp = _write_temporary(root, data, at, inc)
    try:
        started = time.monotonic()
        # The successor absent, then the version present: no later one was written by then.
        if _exists(directory / _version(n + 1)):
            return "mismatch"
        found = _read(directory / _version(n))
        # § one kkad6aqxp.a@rt197
        if found is None or found.inc != inc or _gone(found.at, now):
            return "mismatch"
        if time.monotonic() - started >= SUPERSEDED_KEPT_S:
            raise Transient(f"cas {key}: read too long ago")
        if _link(tmp, directory, n + 1) != "linked":
            return "mismatch"
    finally:
        tmp.unlink(missing_ok=True)
    _clean(directory, now)
    return Written(_etag(inc, n + 1))


def _get(root: Path, key: str, now: int) -> Object | None:
    # § dfd6vt90h.a@dpsh4
    check_key(key)
    latest = _latest(root / key)
    if latest is None or _gone(latest[1].at, now):
        return None
    n, found = latest
    return Object(found.data, _etag(found.inc, n))


def _bury(root: Path, directory: Path, now: int, *, only_expired: bool) -> bool:
    """Writes a tombstone after the latest version of `directory` when that one reads as present
    at `now`, or, with `only_expired`, as past its expiry: a write naming an earlier version then
    finds its successor taken."""
    # § j4b9rkhg8.a@b5sph
    tmp: Path | None = None
    try:
        while True:
            started = time.monotonic()
            latest = _latest(directory)
            if latest is None:
                return False
            n, found = latest
            wanted = found.at > 0 and now >= found.at if only_expired else not _gone(found.at, now)
            if not wanted:
                return False
            if tmp is None:
                tmp = _write_temporary(root, b"", TOMBSTONE, found.inc)
            if time.monotonic() - started >= SUPERSEDED_KEPT_S:
                continue
            linked = _link(tmp, directory, n + 1)
            if linked == "no directory":
                return False
            if linked == "linked":
                break
    finally:
        if tmp is not None:
            tmp.unlink(missing_ok=True)
    _clean(directory, now)
    return True


def _delete(root: Path, key: str, now: int) -> None:
    # § dfd6vt90h.d@s7mwk
    check_key(key)
    _bury(root, root / key, now, only_expired=False)


def _reclaim(root: Path, directory: Path, now: int) -> None:
    """Removes the directory of a key whose only version is a tombstone `SUPERSEDED_KEPT_S` old,
    then each parent left empty, up to the root: a key deleted leaves nothing behind.

    The tombstone goes first: a create that read it links the version after it, which stays. A
    directory goes only by `rmdir`, which fails while it holds anything, so it never takes a
    version linked into it; a link that finds its directory gone makes it again."""
    # § w8a4smsae@meftc
    listed = _versions(directory)
    if len(listed) == 1:
        path = directory / _version(listed[0])
        linked = _linked_at(path)
        try:
            with open(path, "rb") as f:
                buried = int.from_bytes(f.read(8), "big", signed=True) == TOMBSTONE
        except FileNotFoundError:
            buried = False
        except OSError as e:
            raise Transient(f"read {path}: {e}") from e
        old = linked is not None and linked <= now - int(SUPERSEDED_KEPT_S * 1000)
        if not (old and buried):
            return
        path.unlink(missing_ok=True)
    # § one w8a4smsae.a@pr6f7
    at = directory
    while at != root:
        try:
            at.rmdir()
        except OSError:
            # Not empty, as a key or a parent of keys, or removed by another sweep.
            return
        at = at.parent


def _key_dirs(directory: Path, top: bool, out: list[Path]) -> None:
    """The directories under `directory` that could hold a key, the store's own skipped at the
    root, each before those under it."""
    try:
        entries = list(os.scandir(directory))
    except (FileNotFoundError, NotADirectoryError):
        return
    except OSError as e:
        raise Transient(f"list {directory}: {e}") from e
    for entry in entries:
        if not _is_component(entry.name) or (top and entry.name == TMP):
            continue
        try:
            is_dir = entry.is_dir(follow_symlinks=False)
        except OSError:
            continue
        if is_dir:
            out.append(Path(entry.path))
            _key_dirs(Path(entry.path), False, out)


def _sweep(root: Path, now: int) -> int:
    """Buries every key past its expiry at `now`, removes the versions old enough, and the keys
    buried long enough with their empty parents; how many keys it buried. Every step is a link
    that may fail or a removal that may find nothing, so sweeps of several processes may
    overlap."""
    # § qf98sbvzj.a@mhf5e
    directories: list[Path] = []
    _key_dirs(root, True, directories)
    buried = 0
    # The deepest first, so a parent is tried once its children are gone.
    for directory in reversed(directories):
        if _bury(root, directory, now, only_expired=True):
            buried += 1
        else:
            _clean(directory, now)
            _reclaim(root, directory, now)
    return buried


def _listed_header(directory: Path) -> int | None:
    """The header of the latest version a key's directory lists, `None` without one: what a
    listing needs to leave out a key whose latest version is a tombstone."""
    while True:
        listed = _versions(directory)
        if not listed:
            return None
        path = directory / _version(listed[-1])
        try:
            with open(path, "rb") as f:
                header = f.read(8)
        except FileNotFoundError:
            # Superseded and removed: list again.
            continue
        except OSError as e:
            raise Transient(f"read {path}: {e}") from e
        return int.from_bytes(header, "big", signed=True)


def _list(root: Path, prefix: str, start_after: str | None, max_keys: int) -> Listing:
    """Walks the deepest directory holding `prefix`, keeps the keys in range, sorts by full key."""
    dir_key = prefix.rsplit("/", 1)[0] if "/" in prefix else ""
    if dir_key:
        check_key(dir_key)
    keys: list[str] = []
    _walk(root / dir_key if dir_key else root, f"{dir_key}/" if dir_key else "", prefix, keys)
    keys.sort()
    return Listing(keys=in_range(keys, prefix, start_after, max_keys))


def _walk(directory: Path, base: str, prefix: str, keys: list[str]) -> None:
    try:
        entries = list(os.scandir(directory))
    except (FileNotFoundError, NotADirectoryError):
        return
    except OSError as e:
        raise Transient(f"list {directory}: {e}") from e
    for entry in entries:
        name = entry.name
        if not _is_component(name) or (base == "" and name == TMP):
            continue
        try:
            is_dir = entry.is_dir(follow_symlinks=False)
        except OSError as e:
            raise Transient(f"stat {entry.path}: {e}") from e
        if not is_dir:
            continue
        key = f"{base}{name}"
        if key.startswith(prefix):
            header = _listed_header(Path(entry.path))
            if header is not None and header != TOMBSTONE:
                keys.append(key)
        sub = f"{key}/"
        if sub.startswith(prefix) or prefix.startswith(sub):
            _walk(Path(entry.path), sub, prefix, keys)


class FsStore:
    """`file:<path>`: the directory is the namespace, one directory of numbered versions per key.
    For development, CI and one machine.

    Keys never differ only by case (I-17), so a file system that folds case holds them as they
    are.
    """

    capabilities = Capabilities(wait=False)

    def __init__(self, path: str | os.PathLike[str], *, clock: Clock = wall_clock) -> None:
        if not isinstance(path, str | os.PathLike):
            raise ConfigError(f"a file store needs a path, not {path!r}")
        self._root = Path(path)
        self._clock = clock
        # When the last sweep began.
        self._swept: int | None = None
        try:
            (self._root / TMP).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise StoreError(f"cannot prepare {self._root}: {e}") from e
        self._sweep_when_due()

    def sweep(self) -> int:
        """Buries the expired keys now and removes the versions old enough; how many keys it
        buried."""
        return _sweep(self._root, self._clock())

    def _sweep_when_due(self) -> None:
        """Starts a sweep in a thread of its own at open and then daily."""
        # § qf98sbvzj.a@bnhaw
        now = self._clock()
        if self._swept is not None and now - self._swept < DAY_MS:
            return
        self._swept = now
        root = self._root

        def sweep() -> None:
            try:
                _sweep(root, now)
            except (OSError, Transient, Permanent) as e:
                log.warning("the sweep of the file store %s failed: %s", root, e)

        threading.Thread(target=sweep, daemon=True).start()

    async def _blocking[T](self, f: Callable[[int], T]) -> T:
        self._sweep_when_due()
        now = self._clock()
        return await asyncio.to_thread(f, now)

    async def create(self, key: str, data: bytes, *, expiry: int | None) -> Create:
        check_days(expiry)
        data = bytes(data)

        return await retry.create(
            STORE_RETRY,
            data,
            lambda: self._blocking(
                lambda now: _create(self._root, key, data, _expires(expiry, now), now)
            ),
            lambda: self._blocking(lambda now: _get(self._root, key, now)),
        )

    async def cas(self, key: str, data: bytes, etag: str, *, expiry: int | None) -> Cas:
        check_days(expiry)
        data = bytes(data)
        return await retry.cas(
            STORE_RETRY,
            data,
            etag,
            lambda: self._blocking(
                lambda now: _cas(self._root, key, data, etag, _expires(expiry, now), now)
            ),
            lambda: self._blocking(lambda now: _get(self._root, key, now)),
        )

    async def get(self, key: str) -> Object | None:
        return await retry.with_retry(
            STORE_RETRY, lambda: self._blocking(lambda now: _get(self._root, key, now))
        )

    async def list(
        self, prefix: str, *, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        refuse_wait(wait)
        return await retry.with_retry(
            STORE_RETRY,
            lambda: self._blocking(lambda _: _list(self._root, prefix, start_after, max_keys)),
        )

    async def delete(self, key: str) -> None:
        await retry.with_retry(
            STORE_RETRY, lambda: self._blocking(lambda now: _delete(self._root, key, now))
        )

    async def check_expiry(self, days: Sequence[int]) -> None:
        check_all_days(days)

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass
