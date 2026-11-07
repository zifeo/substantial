"""The store contract of I-26, run against every adapter of this host; on S3 and Redis only when
their variables are set (see `conftest`)."""

import asyncio
from pathlib import Path

import pytest

from substantial import ConfigError, Credentials
from substantial.stores import ListWait, Store, StoreFailure, open_store
from substantial.stores.fs import FsStore
from substantial.stores.memory import MemoryStore


async def test_create_is_exclusive(store: Store) -> None:
    assert await store.create("runs/a/start", b"one") == "created"
    assert await store.create("runs/a/start", b"two") == "exists"
    assert await store.get("runs/a/start") == b"one"
    assert await store.get("runs/b/start") is None
    assert await store.get("runs/a") is None
    assert await store.create("runs/a/empty", b"") == "created"
    assert await store.get("runs/a/empty") == b""


async def test_delete_semantics(store: Store) -> None:
    await store.delete("claims/missing")
    await store.create("claims/x", b"1")
    await store.delete("claims/x")
    assert await store.get("claims/x") is None
    await store.delete("claims/x")
    assert await store.create("claims/x", b"2") == "created", "a hint may come back"
    assert await store.get("claims/x") == b"2"


async def test_list_in_byte_order(store: Store) -> None:
    # "q/a/x" sorts after "q/a.b" by full key though its directory "a" sorts before "a.b".
    keys = ["q/a-b", "q/a.b", "q/a/x", "q/a0", "q/9", "q/b/c/d", "q/é", "qq/a", "p/a", "q0"]
    for k in keys:
        await store.create(k, k.encode())

    async def ls(prefix: str, after: str | None = None, most: int = 100) -> list[str]:
        listing = await store.list(prefix, start_after=after, max_keys=most, wait=None)
        assert listing.etag is None
        return listing.keys

    every = ["q/9", "q/a-b", "q/a.b", "q/a/x", "q/a0", "q/b/c/d", "q/é"]
    assert await ls("q/") == every
    assert await ls("q/", None, 3) == every[:3]
    assert await ls("q/", "q/a.b", 3) == every[3:6]
    assert await ls("q/", "q/a/") == every[3:], "start_after need not be a key"
    assert await ls("q/", "a") == every, "start_after before the prefix"
    assert await ls("q/", "r") == []
    assert await ls("q/a") == every[1:5]
    assert await ls("q/a/") == ["q/a/x"]
    assert await ls("q") == [*every, "q0", "qq/a"]
    assert await ls("q/b/") == ["q/b/c/d"]
    assert await ls("none/") == []
    assert await ls("", "q0") == ["qq/a"]
    assert await ls("", None, 1) == ["p/a"]


async def test_list_refuses_wait(store: Store) -> None:
    if store.capabilities.wait:
        pytest.skip("this store's listings wait")
    with pytest.raises(StoreFailure):
        await store.list("q/", start_after=None, max_keys=10, wait=ListWait(seconds=1))


async def test_concurrent_creates_of_one_key(store: Store) -> None:
    outcomes = await asyncio.gather(
        *(store.create("runs/r/log/0", f"writer {i}".encode()) for i in range(32))
    )
    winners = [i for i, outcome in enumerate(outcomes) if outcome == "created"]
    assert len(winners) == 1, f"exactly one create wins: {winners}"
    assert await store.get("runs/r/log/0") == f"writer {winners[0]}".encode()


async def test_fs_checks_key_components(tmp_path: Path) -> None:
    store = FsStore(tmp_path / "ns")
    long = "x" * 256
    for key in [
        "a/./b",
        "a/../b",
        "a//b",
        "/a",
        "a/",
        ".tmp/x",
        "a/\0",
        f"a/{long}",
        "runs/Run/start",
    ]:
        for op in (store.create(key, b"x"), store.get(key), store.delete(key)):
            with pytest.raises(StoreFailure):
                await op
    with pytest.raises(StoreFailure):
        await store.list("../", start_after=None, max_keys=10, wait=None)
    assert await store.create(f"a/{'x' * 255}", b"x") == "created"
    assert await store.create("a/.tmp", b"x") == "created", ".tmp is reserved at the root only"


async def test_fs_keys_survive_reopen_and_hide_temporaries(tmp_path: Path) -> None:
    first = FsStore(tmp_path)
    await first.create("runs/a/start", b"one")
    (tmp_path / ".tmp" / "leftover").write_text("x")
    (tmp_path / ".DS_Store").write_text("x")
    second = FsStore(tmp_path)
    assert await second.get("runs/a/start") == b"one"
    listing = await second.list("", start_after=None, max_keys=10, wait=None)
    assert listing.keys == ["runs/a/start"]


async def test_memory_namespaces_are_shared_by_name_only() -> None:
    a, again, other = MemoryStore("shared-ns"), MemoryStore("shared-ns"), MemoryStore("other-ns")
    await a.create("k", b"v")
    assert await again.get("k") == b"v"
    assert await other.get("k") is None


async def test_urls_name_the_adapter(tmp_path: Path) -> None:
    open_store("memory://url-test", None)
    path = tmp_path / "ns"
    file = open_store(f"file:{path}", None)
    await file.create("k", b"v")
    again = open_store(f"file://{path}", None)
    assert await again.get("k") == b"v"
    for url in [
        "memory://",
        "memory:x",
        "memory://x?wait=true",
        "file:",
        "s3://h/bucket/ns",
        "s3://h/bucket/ns?region=r&endpoint=x",
        "redis://h:1/0",
        "nope",
    ]:
        with pytest.raises(ConfigError):
            open_store(url, None)
    with pytest.raises(ConfigError, match="a store is a URL"):
        open_store(tmp_path, None)  # type: ignore[arg-type]


def test_credentials_go_only_where_a_store_takes_them(tmp_path: Path, monkeypatch) -> None:
    explicit = Credentials(access_key_id="id", secret_access_key="secret")
    assert "secret" not in repr(explicit)
    for url, credentials in [("memory://c", explicit), (f"file:{tmp_path}", "env")]:
        with pytest.raises(ConfigError):
            open_store(url, credentials)
    s3 = "s3+http://127.0.0.1:1/b/ns?region=r"
    with pytest.raises(ConfigError):
        open_store(s3, None)
    with pytest.raises(ConfigError):
        open_store(s3, "environment")  # type: ignore[arg-type]
    assert not open_store(s3, explicit).capabilities.wait
    h3 = "h3://127.0.0.1:1/b/ns?region=r"
    assert open_store(h3, explicit).capabilities.wait
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "id")
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    with pytest.raises(ConfigError, match="AWS_SECRET_ACCESS_KEY"):
        open_store(s3, "env")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "")
    with pytest.raises(ConfigError, match="AWS_SECRET_ACCESS_KEY"):
        open_store(s3, "env")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    open_store(s3, "env")
