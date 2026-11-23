"""What a `Substantial` tells its observer (I-29): store operations, step attempts, replays,
scans, blocked runs, runs near a history limit, and runs left for a clock behind them."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from substantial._proto import layout
from substantial.cost import Amount, Prices
from substantial.errors import ConfigError, Failure
from substantial.stores.attempts import measured
from substantial.stores.base import Capabilities, Cas, Create, Listing, ListWait, Object, Store


@dataclass(frozen=True)
class StoreOp:
    """A store operation of a client or a worker, once it answered: `op` is `create`, `cas`,
    `get`, `list` or `delete`; `attempts` the requests the adapter sent, its retries included;
    `outcome` `created`, `exists`, `written`, `mismatch`, `found`, `missing`, `listed`, `deleted`
    or `failed`; `cost` `attempts` requests at the price of the operation's kind, `None` without
    prices."""

    op: str
    seconds: float
    attempts: int
    outcome: str
    cost: Amount | None


@dataclass(frozen=True)
class Attempt:
    """A step attempt a worker ran, once it ended: `outcome` is `ok`, `retry` (a retryable error
    with attempts left), `failed` (no attempt left, or a permanent error), `timeout` or
    `abandoned` (interrupted, its outcome not recorded)."""

    workflow: str
    step: str
    attempt: int
    seconds: float
    outcome: str


@dataclass(frozen=True)
class Replay:
    """A replay a worker ran, once it ended: `delay` is how late it started after the earliest due
    entry it serves, `None` when it serves none; `end` is `suspended`, `ended`, `idle` (nothing
    new) or `dropped` (a lost race, a stop, a shutdown)."""

    workflow: str
    delay: float | None
    seconds: float
    end: str


@dataclass(frozen=True)
class Scan:
    """A scan listed the due entries of a workflow, `due` of them due."""

    workflow: str
    due: int


@dataclass(frozen=True)
class Blocked:
    """A replay of `run` was blocked by `error`, an error of its code that is no end or a
    divergence from its history: nothing of it is recorded, and the run is tried again after a
    backoff."""

    workflow: str
    run: str
    error: Failure


@dataclass(frozen=True)
class NearLimit:
    """A commit took `run` past the warning share of its `limit`, `history_bytes`,
    `history_events`, `replay_inbox` or `seen`, using `used` of `max`."""

    workflow: str
    run: str
    limit: str
    used: int
    max: int


@dataclass(frozen=True)
class Skewed:
    """The worker left the chain `run` alone, `current` its current run: its clock, `now`, is
    behind the run's last segment, `segment_at`, by more than `claim_skew`."""

    run: str
    current: str
    segment_at: datetime
    now: datetime


type Observed = StoreOp | Attempt | Replay | Scan | Blocked | NearLimit | Skewed

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def time_of(t: layout.Time) -> datetime:
    return _EPOCH + timedelta(milliseconds=t.ms)


def rfc3339(t: datetime) -> str:
    return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")


type Observe = Callable[[Observed], object]


class ObserverRaised(BaseException):
    """The error of an observer, carried past the catch-alls of the store's path to where it stops
    the worker or fails the client call."""

    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


@dataclass(frozen=True)
class Metering:
    """The prices and the observer of a `Substantial`, which its workers take."""

    prices: Prices | None
    observe: Observe | None

    def __post_init__(self) -> None:
        if self.prices is not None and not isinstance(self.prices, Prices):
            raise ConfigError(f"prices is a Prices, not {self.prices!r}")
        if self.observe is not None and not callable(self.observe):
            raise ConfigError(f"observe is a function, not {self.observe!r}")

    def tell(self, event: Observed) -> None:
        if self.observe is not None:
            self.observe(event)

    def wrap(self, store: Store) -> Store:
        """`store`, its operations observed when there is an observer."""
        if self.observe is None:
            return store
        return ObservingStore(store, self.observe, self.prices)


class ObservingStore:
    """A store whose every operation is told to an observer once it answered."""

    def __init__(self, inner: Store, observe: Observe, prices: Prices | None) -> None:
        self._inner = inner
        self._observe = observe
        self._prices = prices

    @property
    def capabilities(self) -> Capabilities:
        return self._inner.capabilities

    async def _observed[T](
        self, op: str, call: Callable[[], Awaitable[T]], outcome: Callable[[T], str]
    ) -> T:
        # § yp7fv5sa7.a@xd50r
        started = time.monotonic()
        try:
            with measured() as sent:
                result = await call()
        except Exception:
            self._tell(op, started, sent.n, "failed")
            raise
        self._tell(op, started, sent.n, outcome(result))
        return result

    def _tell(self, op: str, started: float, attempts: int, outcome: str) -> None:
        event = StoreOp(
            op=op,
            seconds=time.monotonic() - started,
            attempts=attempts,
            outcome=outcome,
            # § one vg9yp9xsf@gkyd9
            cost=None if self._prices is None else self._prices._of(op, attempts),
        )
        # § one yp7fv5sa7.a@h5jpt
        try:
            self._observe(event)
        except Exception as e:
            # § yp7fv5sa7.b@pwsj8
            raise ObserverRaised(e) from e

    async def create(self, key: str, data: bytes, *, expiry: int | None) -> Create:
        def outcome(created: Create) -> str:
            return "exists" if created == "exists" else "created"

        return await self._observed(
            "create", lambda: self._inner.create(key, data, expiry=expiry), outcome
        )

    async def cas(self, key: str, data: bytes, etag: str, *, expiry: int | None) -> Cas:
        def outcome(written: Cas) -> str:
            return "mismatch" if written == "mismatch" else "written"

        return await self._observed(
            "cas", lambda: self._inner.cas(key, data, etag, expiry=expiry), outcome
        )

    async def check_expiry(self, days: Sequence[int]) -> None:
        await self._inner.check_expiry(days)

    async def get(self, key: str) -> Object | None:
        def found(data: Object | None) -> str:
            return "missing" if data is None else "found"

        return await self._observed("get", lambda: self._inner.get(key), found)

    async def list(
        self, prefix: str, *, start_after: str | None, max_keys: int, wait: ListWait | None
    ) -> Listing:
        def listed() -> Awaitable[Listing]:
            return self._inner.list(prefix, start_after=start_after, max_keys=max_keys, wait=wait)

        return await self._observed("list", listed, lambda _: "listed")

    async def delete(self, key: str) -> None:
        await self._observed("delete", lambda: self._inner.delete(key), lambda _: "deleted")

    async def open(self) -> None:
        await self._inner.open()

    async def close(self) -> None:
        await self._inner.close()
