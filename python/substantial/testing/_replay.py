"""The replay tester: recorded runs copied from a store into an environment, replayed by new code
against their histories, for CI before a deploy."""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from google.protobuf.message import DecodeError

from substantial._proto import layout
from substantial.errors import (
    InvalidArgument,
    LayoutError,
    NonDeterminism,
    ProtocolError,
    RunNotFound,
    StoreError,
)
from substantial.stores import Store, StoreFailure, open_store
from substantial.stores.credentials import CredentialsArg
from substantial.testing._env import Env
from substantial.testing._world import ended
from substantial.workflow import Workflow

_MARKER = "substantial.json"


@dataclass(frozen=True)
class Replayed:
    """How a run replayed against its history: `matched` when the code made every call the
    history recorded, up to where the run waits; `diverged`, with the `NonDeterminism` a worker of
    this code would meet; `ended` when the run had ended, so no worker replays it again."""

    run: str
    outcome: Literal["matched", "diverged", "ended"]
    error: NonDeterminism | None


async def replay(
    source: str,
    workflows: Sequence[Workflow[..., Any]],
    *,
    runs: Sequence[str],
    credentials: CredentialsArg = None,
) -> list[Replayed]:
    """Replays the runs `runs` of the store at `source` with the code of `workflows`: each run is
    copied into an environment's namespace in memory, by get alone, and replayed there
    up to the end of its history, with no step run, and nothing is written to the source. `runs`
    are run ids: for a chain that continued, its run that is live (`RunInfo.run`)."""
    # § awhhtbr5n@1ek42
    if isinstance(runs, str) or not runs:
        raise InvalidArgument("a replay names at least one run")
    store = open_store(source, credentials)
    try:
        await _store(store.open())
        ms = time.time_ns() // 1_000_000
        now = datetime.fromtimestamp(ms / 1000, UTC)
        async with Env.open(now=now) as env:
            world = env._world
            world.checks = {}
            target = world.memory
            await _same_layout(store, target)
            return [await _replay_one(env, store, target, workflows, run, ms) for run in runs]
    finally:
        await store.close()


async def _replay_one(
    env: Env,
    source: Store,
    target: Store,
    workflows: Sequence[Workflow[..., Any]],
    run: str,
    ms: int,
) -> Replayed:
    # § awhhtbr5n.c@qvtxt
    head = await _head(source, run)
    if head is None:
        raise RunNotFound(run)
    start = head.start
    version = start.version if start.HasField("version") else None
    if not any(w.name == start.workflow and w.version == version for w in workflows):
        raise InvalidArgument(
            f"run {run} is of workflow {start.workflow!r} version {version!r}, which no "
            "workflow given serves"
        )
    # § end awhhtbr5n.c
    # A run its chain continued, retried or reset past, or one that ended.
    if head.run != run or ended(head):
        return Replayed(run, "ended", None)
    # § awhhtbr5n.d@k6w79
    # Without its lease, which would hold it for its worker, and its block, which would hold it
    # until its backoff.
    held = layout.Head()
    held.CopyFrom(head)
    held.ClearField("lease")
    held.ClearField("blocked")
    chain = start.chain
    await _store(
        target.create(f"runs/{chain}", layout.Run(head=held).SerializeToString(), expiry=None)
    )
    # § end awhhtbr5n.d
    at = datetime.fromtimestamp(ms / 1000, UTC)
    stamp = f"{at:%Y%m%dt%H%M%S}{ms % 1000:03d}z"
    due = f"queues/{start.workflow}/due/{stamp}~{chain}~s{len(held.segments):010d}"
    await _store(target.create(due, b"", expiry=None))
    world = env._world
    worker = env.worker(workflows, preset="script")
    worker._start()
    try:
        await env.settle()
    finally:
        worker.crash()
    # A replay that starts no step takes no claim, so the next run's worker would replay this
    # one again, its wakeup still due.
    await target.delete(due)
    assert world.checks is not None
    if run not in world.checks:
        died = "; ".join(f"{type(e).__name__}: {e}" for e in world.errors) or "no worker died"
        raise ProtocolError(f"run {run} was not replayed: {died}")
    diverged = world.checks[run]
    if diverged is None:
        return Replayed(run, "matched", None)
    return Replayed(run, "diverged", NonDeterminism(diverged))


async def _same_layout(source: Store, target: Store) -> None:
    """Refuses a source of another layout than the environment's."""
    # § awhhtbr5n.c@4dn01
    theirs = await _value(source, _MARKER)
    ours = await _value(target, _MARKER)
    if theirs is None:
        raise LayoutError(f"the source names no namespace: it has no {_MARKER}")
    if theirs != ours:
        raise LayoutError(
            f"the source's {_MARKER} is {theirs.decode(errors='replace')}, not this "
            f"version's {(ours or b'').decode(errors='replace')}"
        )


async def _head(store: Store, id: str) -> layout.Head | None:
    """The head `runs/<id>` holds, through the pointer of a later run to its chain's head."""
    found = await _decode(store, f"runs/{id}")
    if found is None:
        return None
    if found.WhichOneof("kind") == "pointer":
        key = f"runs/{found.pointer.chain}"
        found = await _decode(store, key)
        if found is None:
            return None
        if found.WhichOneof("kind") == "pointer":
            raise LayoutError(f"{key}: a pointer to a pointer")
    if found.WhichOneof("kind") != "head":
        raise LayoutError(f"runs/{id}: neither head nor pointer")
    return found.head


async def _decode(store: Store, key: str) -> layout.Run | None:
    data = await _value(store, key)
    if data is None:
        return None
    try:
        return layout.Run.FromString(data)
    except DecodeError as e:
        raise LayoutError(f"{key}: {e}") from None


async def _value(store: Store, key: str) -> bytes | None:
    found = await _store(store.get(key))
    return None if found is None else found.data


async def _store[T](op: Any) -> T:
    try:
        return await op
    except StoreFailure as e:
        raise StoreError(e.message) from None
