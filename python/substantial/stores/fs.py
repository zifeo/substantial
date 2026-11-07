from __future__ import annotations

import asyncio
import os
import secrets
from collections.abc import Callable
from pathlib import Path

from substantial.errors import ConfigError, StoreError
from substantial.stores import retry
from substantial.stores.base import Capabilities, Create, Listing, ListWait, in_range, refuse_wait
from substantial.stores.retry import STORE_RETRY, Ambiguous, Permanent, Transient

TMP = ".tmp"


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


def _ensure_dirs(root: Path, directory: Path) -> None:
    """Creates the missing directories between `root` and `directory`, each fsynced in its
    parent."""
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
            raise Transient(f"create directory {d}: {e}") from e
        try:
            _sync_dir(d.parent)
        except OSError as e:
            raise Transient(f"sync directory {d.parent}: {e}") from e


def _create(root: Path, key: str, data: bytes) -> Create:
    check_key(key)
    path = root / key
    tmp = root / TMP / secrets.token_hex(16)
    try:
        with open(tmp, "xb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
    except OSError as e:
        tmp.unlink(missing_ok=True)
        raise Transient(f"write {tmp}: {e}") from e
    try:
        _ensure_dirs(root, path.parent)
        try:
            os.link(tmp, path)
        except FileExistsError:
            return "exists"
        except NotADirectoryError as e:
            raise Permanent(f"key {key!r} lies under another key: {e}") from e
        except OSError as e:
            # On NFS a link that succeeded may still report an error.
            raise Ambiguous(f"link {path}: {e}") from e
        try:
            _sync_dir(path.parent)
        except OSError as e:
            raise Ambiguous(f"sync directory {path.parent}: {e}") from e
        return "created"
    finally:
        tmp.unlink(missing_ok=True)


def _get(root: Path, key: str) -> bytes | None:
    check_key(key)
    path = root / key
    try:
        return path.read_bytes()
    except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
        return None
    except OSError as e:
        raise Transient(f"read {path}: {e}") from e


def _delete(root: Path, key: str) -> None:
    check_key(key)
    path = root / key
    try:
        path.unlink()
    except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
        pass
    except OSError as e:
        raise Transient(f"delete {path}: {e}") from e


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
        key = f"{base}{name}"
        try:
            is_dir = entry.is_dir(follow_symlinks=False)
        except OSError as e:
            raise Transient(f"stat {entry.path}: {e}") from e
        if is_dir:
            sub = f"{key}/"
            if sub.startswith(prefix) or prefix.startswith(sub):
                _walk(Path(entry.path), sub, prefix, keys)
        elif key.startswith(prefix):
            keys.append(key)


class FsStore:
    """`file:<path>`: the directory is the namespace, one file per key. For development, CI and
    one machine.

    Keys never differ only by case (I-17), so a file system that folds case holds them as they
    are.
    """

    capabilities = Capabilities(wait=False)

    def __init__(self, path: str | os.PathLike[str]) -> None:
        if not isinstance(path, str | os.PathLike):
            raise ConfigError(f"a file store needs a path, not {path!r}")
        self._root = Path(path)
        try:
            (self._root / TMP).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise StoreError(f"cannot prepare {self._root}: {e}") from e

    async def _blocking[T](self, f: Callable[[], T]) -> T:
        return await asyncio.to_thread(f)

    async def create(self, key: str, data: bytes) -> Create:
        data = bytes(data)
        return await retry.create(
            STORE_RETRY,
            data,
            lambda: self._blocking(lambda: _create(self._root, key, data)),
            lambda: self._blocking(lambda: _get(self._root, key)),
        )

    async def get(self, key: str) -> bytes | None:
        return await retry.with_retry(
            STORE_RETRY, lambda: self._blocking(lambda: _get(self._root, key))
        )

    async def list(
        self, prefix: str, *, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        refuse_wait(wait)
        return await retry.with_retry(
            STORE_RETRY,
            lambda: self._blocking(lambda: _list(self._root, prefix, start_after, max_keys)),
        )

    async def delete(self, key: str) -> None:
        await retry.with_retry(
            STORE_RETRY, lambda: self._blocking(lambda: _delete(self._root, key))
        )

    async def close(self) -> None:
        pass
