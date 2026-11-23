"""The store contract of I-26, run against every adapter of this host; on S3 and Redis only when
their variables are set (see `conftest`)."""

import asyncio
import json
import os
import threading
import time
from pathlib import Path

import pytest
from conftest import PLAIN_LISTINGS, S3_URL, S3_VERSIONED_URL, kind, require_s3, value

from substantial import ConfigError, Credentials
from substantial.stores import Created, ListWait, Store, StoreFailure, Written, open_store
from substantial.stores.base import DAY_MS
from substantial.stores.fs import FsStore
from substantial.stores.memory import MemoryStore
from substantial.stores.s3 import S3Location


async def test_create_is_exclusive(store: Store) -> None:
    assert kind(await store.create("runs/a/start", b"one", expiry=None)) == "created"
    assert await store.create("runs/a/start", b"two", expiry=None) == "exists"
    assert await value(store, "runs/a/start") == b"one"
    assert await value(store, "runs/b/start") is None
    assert await value(store, "runs/a") is None
    assert kind(await store.create("runs/a/empty", b"", expiry=None)) == "created"
    assert await value(store, "runs/a/empty") == b""


async def test_delete_semantics(store: Store) -> None:
    await store.delete("claims/missing")
    await store.create("claims/x", b"1", expiry=None)
    await store.delete("claims/x")
    assert await value(store, "claims/x") is None
    await store.delete("claims/x")
    assert kind(await store.create("claims/x", b"2", expiry=None)) == "created", (
        "a hint may come back"
    )
    assert await value(store, "claims/x") == b"2"


async def test_list_in_byte_order(store: Store) -> None:
    # "q/a/x" sorts after "q/a.b" by full key though its directory "a" sorts before "a.b".
    keys = ["q/a-b", "q/a.b", "q/a/x", "q/a0", "q/9", "q/b/c/d", "q/é", "qq/a", "p/a", "q0"]
    for k in keys:
        await store.create(k, k.encode(), expiry=None)

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


@pytest.mark.parametrize("adapter", PLAIN_LISTINGS)
async def test_list_refuses_wait(store: Store) -> None:
    assert not store.capabilities.wait
    with pytest.raises(StoreFailure):
        await store.list("q/", start_after=None, max_keys=10, wait=ListWait(seconds=1))


async def test_concurrent_creates_of_one_key(store: Store) -> None:
    outcomes = await asyncio.gather(
        *(store.create("runs/r/log/0", f"writer {i}".encode(), expiry=None) for i in range(32))
    )
    winners = [i for i, outcome in enumerate(outcomes) if kind(outcome) == "created"]
    assert len(winners) == 1, f"exactly one create wins: {winners}"
    assert await value(store, "runs/r/log/0") == f"writer {winners[0]}".encode()


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
        for op in (store.create(key, b"x", expiry=None), store.get(key), store.delete(key)):
            with pytest.raises(StoreFailure):
                await op
    with pytest.raises(StoreFailure):
        await store.list("../", start_after=None, max_keys=10, wait=None)
    assert kind(await store.create(f"a/{'x' * 255}", b"x", expiry=None)) == "created"
    assert kind(await store.create("a/.tmp", b"x", expiry=None)) == "created", (
        ".tmp is reserved at the root only"
    )


async def test_fs_keys_survive_reopen_and_hide_temporaries(tmp_path: Path) -> None:
    first = FsStore(tmp_path)
    await first.create("runs/a/start", b"one", expiry=None)
    (tmp_path / ".tmp" / "leftover").write_text("x")
    (tmp_path / ".DS_Store").write_text("x")
    second = FsStore(tmp_path)
    assert await value(second, "runs/a/start") == b"one"
    listing = await second.list("", start_after=None, max_keys=10, wait=None)
    assert listing.keys == ["runs/a/start"]


async def test_memory_namespaces_are_shared_by_name_only() -> None:
    a, again, other = MemoryStore("shared-ns"), MemoryStore("shared-ns"), MemoryStore("other-ns")
    await a.create("k", b"v", expiry=None)
    assert await value(again, "k") == b"v"
    assert await value(other, "k") is None


async def test_urls_name_the_adapter(tmp_path: Path) -> None:
    open_store("memory://url-test", None)
    path = tmp_path / "ns"
    file = open_store(f"file:{path}", None)
    await file.create("k", b"v", expiry=None)
    again = open_store(f"file://{path}", None)
    assert await value(again, "k") == b"v"
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


async def etag(store: Store, key: str) -> str:
    found = await store.get(key)
    assert found is not None, key
    return found.etag


async def test_compare_and_set(store: Store) -> None:
    # § kkad6aqxp@19rky
    await store.create("runs/c", b"one", expiry=None)
    first = await etag(store, "runs/c")
    # An ETag no store makes, which RustFS takes as well formed.
    wrong = "substantial-probe-mismatch"
    assert await store.cas("runs/c", b"two", wrong, expiry=None) == "mismatch"
    assert await value(store, "runs/c") == b"one"
    written = await store.cas("runs/c", b"two", first, expiry=None)
    assert isinstance(written, Written), "the ETag read names the value"
    # § one kkad6aqxp.b@h0x58
    assert written.etag != first, "the bytes changed, so did the ETag"
    assert await etag(store, "runs/c") == written.etag
    assert await value(store, "runs/c") == b"two"
    assert await store.cas("runs/c", b"three", first, expiry=None) == "mismatch"
    assert await value(store, "runs/c") == b"two"
    # § one kkad6aqxp.a@8zxyj
    assert await store.cas("runs/none", b"x", first, expiry=None) == "mismatch"
    assert await value(store, "runs/none") is None


async def test_compare_and_set_after_a_delete(store: Store) -> None:
    await store.create("runs/d", b"one", expiry=None)
    read = await etag(store, "runs/d")
    await store.delete("runs/d")
    # § one kkad6aqxp.a@d9kxc
    assert await store.cas("runs/d", b"two", read, expiry=None) == "mismatch"
    assert await value(store, "runs/d") is None


async def deleted_key_comes_back(store: Store) -> bool:
    """The probe's last step: a compare-and-set naming the ETag of a deleted key, which must
    answer mismatch; true when the key came back instead."""
    await store.create("probe/deleted", b"", expiry=None)
    read = await etag(store, "probe/deleted")
    await store.delete("probe/deleted")
    back = await store.cas("probe/deleted", b"", read, expiry=None)
    if back != "mismatch":
        await store.delete("probe/deleted")
    return back != "mismatch"


async def test_rustfs_versioned_fails_the_probe_of_a_deleted_key() -> None:
    """RustFS 1.0.1 on a versioned bucket compares `If-Match` with the newest version holding
    data, past the delete marker, so a compare-and-set brings a deleted key back, where AWS and
    Herald answer 404: it fails the probe, which refuses the store at open. The only use of the
    versioned bucket. This fails once RustFS meets the contract."""
    store = open_store(require_s3(variable=S3_VERSIONED_URL), "env")
    try:
        assert await deleted_key_comes_back(store), (
            "RustFS on a versioned bucket now refuses a compare-and-set naming a deleted key: it"
            " meets the contract, so the versioned bucket can run the S3 tests again"
        )
    finally:
        await store.close()


async def test_probe(store: Store) -> None:
    """What the probe of `create` and `open` requires of a store, which every store answers."""
    assert kind(await store.create("probe/exclusive", b"", expiry=None)) in ("created", "exists")
    assert await store.create("probe/exclusive", b"", expiry=None) == "exists"
    # The ETag the core's probe names, which no store makes.
    wrong = "substantial-probe-mismatch"
    assert await store.cas("probe/exclusive", b"", wrong, expiry=None) == "mismatch"
    assert await value(store, "probe/exclusive") == b""
    assert await store.cas("probe/missing", b"", wrong, expiry=None) == "mismatch"
    assert not await deleted_key_comes_back(store)


async def test_a_create_answers_the_etag_a_cas_names(store: Store) -> None:
    created = await store.create("runs/n", b"one", expiry=None)
    assert isinstance(created, Created)
    assert created.etag == await etag(store, "runs/n")
    assert isinstance(await store.cas("runs/n", b"two", created.etag, expiry=None), Written)


async def test_concurrent_compare_and_sets_of_one_key(store: Store) -> None:
    await store.create("runs/h", b"start", expiry=None)
    read = await etag(store, "runs/h")
    outcomes = await asyncio.gather(
        *(store.cas("runs/h", f"writer {i}".encode(), read, expiry=None) for i in range(16))
    )
    winners = [i for i, outcome in enumerate(outcomes) if isinstance(outcome, Written)]
    assert len(winners) == 1, f"exactly one writer of a read wins: {winners}"
    assert await value(store, "runs/h") == f"writer {winners[0]}".encode()


async def test_expiries_in_range(store: Store) -> None:
    # § r4rsq6spn.c@xkftt
    for days in (0, 36_501, True):
        with pytest.raises(StoreFailure):
            await store.create("runs/x", b"x", expiry=days)
        with pytest.raises(ConfigError):
            await store.check_expiry([days])
    await store.create("runs/x", b"x", expiry=1)
    assert await value(store, "runs/x") == b"x"
    read = await etag(store, "runs/x")
    with pytest.raises(StoreFailure):
        await store.cas("runs/x", b"y", read, expiry=0)
    assert isinstance(await store.cas("runs/x", b"y", read, expiry=36_500), Written)
    assert await value(store, "runs/x") == b"y"


class Clock:
    """A clock a test moves, in ms since the epoch."""

    def __init__(self, at: int) -> None:
        self.at = at

    def __call__(self) -> int:
        return self.at


T0 = 1_795_000_000_000


async def exact_expiry(store: Store, clock: Clock) -> None:
    """How the stores that expire keys themselves, by their own clock, keep them."""
    await store.create("runs/e", b"one", expiry=1)
    await store.create("runs/kept", b"kept", expiry=1)
    # § one r4rsq6spn.a@7zxce
    cleared = await store.cas("runs/kept", b"kept", await etag(store, "runs/kept"), expiry=None)
    assert isinstance(cleared, Written)
    clock.at = T0 + DAY_MS - 1
    assert await value(store, "runs/e") == b"one", "kept for the whole day"
    read = await etag(store, "runs/e")
    clock.at = T0 + DAY_MS
    # § one r4rsq6spn@70x86
    assert await value(store, "runs/e") is None, "removed once its day passed"
    assert await store.cas("runs/e", b"two", read, expiry=1) == "mismatch"
    # § one r4rsq6spn.b@1nbsj
    assert await value(store, "runs/e") is None
    assert await value(store, "runs/kept") == b"kept", "its expiry cleared"
    assert kind(await store.create("runs/e", b"new", expiry=None)) == "created"
    clock.at = T0 + 100 * DAY_MS
    assert await value(store, "runs/e") == b"new"


async def test_memory_expires_keys_exactly() -> None:
    clock = Clock(T0)
    await exact_expiry(MemoryStore("contract-expiry", clock=clock), clock)


def version_file(root: Path, key: str, n: int) -> Path:
    """The file of version `n` of `key` in the file store at `root`."""
    return root / key / f".{n:020}"


def header_of(path: Path) -> int:
    """The header's expiry of the version file at `path`."""
    return int.from_bytes(path.read_bytes()[:8], "big", signed=True)


def with_version(etag: str, n: int) -> str:
    """The ETag of version `n` of the incarnation `etag` names."""
    return f"{etag.partition('.')[0]}.{n}"


def left(root: Path) -> list[str]:
    """Every path under `root` but the temporary files, as the store leaves it."""
    return sorted(
        str(p.relative_to(root)) for p in root.rglob("*") if p.relative_to(root).parts[0] != ".tmp"
    )


async def test_fs_expires_keys_exactly_and_sweeps_them(tmp_path: Path) -> None:
    clock = Clock(T0)
    store = FsStore(tmp_path, clock=clock)
    await exact_expiry(store, clock)
    await store.create("runs/swept", b"x", expiry=2)
    # § qf98sbvzj@kx5jq
    file = version_file(tmp_path, "runs/swept", 0).read_bytes()
    at = T0 + 102 * DAY_MS
    assert file[:8] == at.to_bytes(8, "big"), "the expiry heads the version"
    assert file[24:] == b"x", "after the incarnation"
    assert header_of(version_file(tmp_path, "runs/kept", 1)) == 0, "no expiry"
    clock.at = at
    # § one qf98sbvzj.a@h178m
    assert store.sweep() == 1, "a tombstone over the expired key"
    assert header_of(version_file(tmp_path, "runs/swept", 1)) == -1
    assert await value(store, "runs/swept") is None
    # The fake clock is past the real time of every link: what they superseded is old enough,
    # and so is the tombstone, which the next sweep reclaims with its directory.
    assert not version_file(tmp_path, "runs/swept", 0).exists()
    assert store.sweep() == 0
    assert not (tmp_path / "runs/swept").exists(), "reclaimed"
    assert not version_file(tmp_path, "runs/kept", 0).exists()
    assert version_file(tmp_path, "runs/kept", 1).exists(), "the latest stays"


async def test_fs_keeps_a_key_as_numbered_versions(tmp_path: Path) -> None:
    store = FsStore(tmp_path)
    with pytest.raises(StoreFailure):
        await store.create("a/.00000000000000000001", b"x", expiry=None)
    # § j4b9rkhg8@46846
    first = await store.create("runs/a", b"x", expiry=None)
    assert isinstance(first, Created) and first.etag.endswith(".0") and len(first.etag) == 34
    assert await store.cas("runs/a", b"y", first.etag, expiry=None) == Written(
        with_version(first.etag, 1)
    )
    assert version_file(tmp_path, "runs/a", 1).read_bytes()[24:] == b"y"
    assert version_file(tmp_path, "runs/a", 0).exists(), "superseded just now"
    listing = await store.list("", start_after=None, max_keys=10, wait=None)
    assert listing.keys == ["runs/a"]
    # A key may hold keys under it, as layout 2's do.
    await store.create("runs/a/start", b"s", expiry=None)
    listing = await store.list("runs/", start_after=None, max_keys=10, wait=None)
    assert listing.keys == ["runs/a", "runs/a/start"]


async def test_fs_a_delete_takes_the_version_a_late_write_names(tmp_path: Path) -> None:
    store = FsStore(tmp_path)
    await store.create("k", b"one", expiry=None)
    read = await etag(store, "k")
    # § j4b9rkhg8.a@67sqa
    await store.delete("k")
    assert header_of(version_file(tmp_path, "k", 1)) == -1
    assert await store.cas("k", b"late", read, expiry=None) == "mismatch"
    assert await value(store, "k") is None
    assert (await store.list("", start_after=None, max_keys=10, wait=None)).keys == []
    # A write that won the version first stands, and the delete buries it next.
    again = await store.create("k", b"two", expiry=None)
    assert isinstance(again, Created) and again.etag.endswith(".2")
    assert again.etag != with_version(read, 2), "a new incarnation"
    won = await store.cas("k", b"three", again.etag, expiry=None)
    await store.delete("k")
    assert won == Written(with_version(again.etag, 3))
    assert await store.cas("k", b"late", won.etag, expiry=None) == "mismatch"
    assert await value(store, "k") is None
    # A write whose version a removal freed below the latest is refused.
    await store.create("g", b"0", expiry=None)
    for n in range(3):
        await store.cas("g", str(n + 1).encode(), await etag(store, "g"), expiry=None)
    current = await etag(store, "g")
    for n in range(3):
        version_file(tmp_path, "g", n).unlink()
    assert await store.cas("g", b"stale", with_version(current, 1), expiry=None) == "mismatch"
    assert not version_file(tmp_path, "g", 2).exists(), "no version written below the latest"
    assert await value(store, "g") == b"3"


async def test_fs_refuses_an_etag_of_a_removed_incarnation(tmp_path: Path) -> None:
    clock = Clock(T0)
    store = FsStore(tmp_path, clock=clock)
    await store.create("k", b"old", expiry=None)
    stale = await etag(store, "k")
    await store.delete("k")
    # The fake clock is past the real time of the tombstone's link: old enough to reclaim.
    clock.at = T0 + 100 * DAY_MS
    store.sweep()
    assert not (tmp_path / "k").exists(), "the key's directory reclaimed"
    # § j4b9rkhg8.d@c73cv
    fresh = await store.create("k", b"new", expiry=None)
    assert isinstance(fresh, Created) and fresh.etag.endswith(".0"), "numbered from 0 again"
    assert fresh.etag != stale, "a new incarnation"
    assert await store.cas("k", b"stale", stale, expiry=None) == "mismatch"
    assert await value(store, "k") == b"new"


async def test_fs_reclaims_every_deleted_key_and_its_empty_parents(tmp_path: Path) -> None:
    clock = Clock(T0)
    store = FsStore(tmp_path, clock=clock)
    await store.create("runs/a/start", b"kept", expiry=None)
    for cycle in range(3):
        for i in range(10):
            key = f"queues/w/due/{i:03}~{cycle}"
            await store.create(key, b"", expiry=1)
            await store.delete(key)
            await store.create("probe/deleted", b"", expiry=None)
            await store.delete("probe/deleted")
    clock.at = T0 + 100 * DAY_MS
    # § w8a4smsae@39ez3
    store.sweep()
    assert left(tmp_path) == [
        "runs",
        "runs/a",
        "runs/a/start",
        "runs/a/start/.00000000000000000000",
    ], "nothing left of the deleted keys, nor of their parents"


async def test_fs_a_sweep_racing_creates_loses_none(tmp_path: Path) -> None:
    # Every tombstone is old enough to reclaim at once, so sweeps remove directories while the
    # creates and deletes run.
    store = FsStore(tmp_path, clock=Clock(T0 + 100 * DAY_MS))
    stop = threading.Event()
    sweeps = 0

    def sweeper() -> None:
        nonlocal sweeps
        while not stop.is_set():
            store.sweep()
            sweeps += 1
            time.sleep(0.001)

    thread = threading.Thread(target=sweeper)
    thread.start()

    async def worker(w: int) -> None:
        key = f"q/{w}/k"
        for i in range(20):
            mine = f"{w}-{i}".encode()
            assert isinstance(await store.create(key, mine, expiry=None), Created), "free again"
            assert await value(store, key) == mine, "kept until deleted"
            await store.delete(key)
            assert await value(store, key) is None

    try:
        await asyncio.gather(*(worker(w) for w in range(8)))
    finally:
        stop.set()
        thread.join()
    assert sweeps > 0
    store.sweep()
    assert left(tmp_path) == []


async def test_fs_writers_racing_a_delete_never_bring_the_key_back(tmp_path: Path) -> None:
    store = FsStore(tmp_path)
    await store.create("r", b"0", expiry=None)

    async def writer(i: int) -> int:
        written = 0
        while (read := await store.get("r")) is not None:
            if (
                await store.cas("r", f"{i}-{written}".encode(), read.etag, expiry=None)
                != "mismatch"
            ):
                written += 1
        return written

    writers = [asyncio.create_task(writer(i)) for i in range(8)]
    await asyncio.sleep(0.05)
    await store.delete("r")
    assert sum(await asyncio.gather(*writers)) > 0, "the writers wrote before the delete"
    assert await value(store, "r") is None, "no write lands after the delete"
    latest = max(os.listdir(tmp_path / "r"))
    assert header_of(tmp_path / "r" / latest) == -1, "the tombstone is the latest"


async def test_only_s3_checks_its_expiries(tmp_path: Path) -> None:
    for store in (MemoryStore("contract-check"), FsStore(tmp_path)):
        await store.check_expiry([1, 7, 36_500])


def raw_s3(url: str):
    """A botocore client of the bucket `url` names, beside the adapter, and its location."""
    import botocore.config
    import botocore.session

    location = S3Location.parse(url)
    client = botocore.session.get_session().create_client(
        "s3",
        region_name=location.region,
        endpoint_url=location.endpoint,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        config=botocore.config.Config(s3={"addressing_style": "path"}),
    )
    return client, location


@pytest.mark.parametrize("variable", [S3_URL])
async def test_s3_writes_its_expiry_as_a_tag(variable: str) -> None:

    url = require_s3(variable=variable)
    store = open_store(url, "env")
    client, at = raw_s3(url)

    def tags(key: str) -> list[str]:
        answer = client.get_object_tagging(Bucket=at.bucket, Key=f"{at.namespace}/{key}")
        return [f"{t['Key']}={t['Value']}" for t in answer["TagSet"]]

    try:
        # § c4fyc3wyk@7adks
        await store.create("runs/t", b"one", expiry=1)
        assert tags("runs/t") == ["substantial-retention=1d"]
        await store.cas("runs/t", b"two", await etag(store, "runs/t"), expiry=30)
        assert tags("runs/t") == ["substantial-retention=30d"]
        await store.cas("runs/t", b"three", await etag(store, "runs/t"), expiry=None)
        assert tags("runs/t") == [], "a write without an expiry clears it"
        await store.create("runs/u", b"one", expiry=None)
        assert tags("runs/u") == []
    finally:
        await store.close()


@pytest.mark.parametrize("variable", [S3_URL])
async def test_s3_checks_a_lifecycle_rule_expires_each_retention(variable: str) -> None:
    url = require_s3(variable=variable)
    store = open_store(url, "env")
    elsewhere = open_store(url.replace("/tests/", "/elsewhere-"), "env")
    try:
        # § cfdckf61n@1tazd
        await store.check_expiry([1, 30, 1])
        with pytest.raises(ConfigError) as refused:
            await store.check_expiry([30, 7, 365])
        message = str(refused.value)
        ns = S3Location.parse(url).namespace
        assert "substantial-retention=7d, 365d" in message, message
        rule = {
            "Expiration": {"Days": 7},
            "Filter": {
                "And": {
                    "Prefix": f"{ns}/",
                    "Tags": [{"Key": "substantial-retention", "Value": "7d"}],
                }
            },
            "ID": "substantial-expire-7d",
            "Status": "Enabled",
        }
        assert json.dumps(rule, separators=(",", ":")) in message, message
        assert "30d" not in message, message
        with pytest.raises(ConfigError, match="substantial-retention=1d"):
            await elsewhere.check_expiry([1])
    finally:
        await store.close()
        await elsewhere.close()
