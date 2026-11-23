"""Creating and opening a namespace (I-16), starting and following runs (I-17), and workers."""

from __future__ import annotations

import asyncio
import json
import logging
import weakref
from collections.abc import AsyncIterator, Coroutine, Generator, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Self

from substantial._proto import dumps, json_value, layout, plan
from substantial.child import effective_budget, input_object
from substantial.clock import REAL, Clock
from substantial.core import Core
from substantial.cost import Prices, budget_proto
from substantial.errors import (
    ConfigError,
    Failure,
    InvalidArgument,
    LayoutError,
    ProtocolError,
    RunEnded,
    RunExists,
    RunFailed,
    RunNotFound,
    StoreError,
    WaitTimeout,
    run_failed_cause,
)
from substantial.names import check_key, check_sent_name, check_str
from substantial.observe import Metering, Observe, ObserverRaised
from substantial.plan import outcome_of, plan_steps, run_plan, ulid
from substantial.schedule import Cron, check_config, config, message_id
from substantial.scheduler import decode_json
from substantial.settings import Settings
from substantial.stores import Store, StoreFailure, open_store
from substantial.stores.credentials import CredentialsArg
from substantial.stores.s3 import listing_changed
from substantial.worker import Worker
from substantial.workflow import Workflow, Workflows

log = logging.getLogger("substantial")

_ENDS = {
    layout.STATUS_COMPLETED: "completed",
    layout.STATUS_FAILED: "failed",
    layout.STATUS_CANCELLED: "cancelled",
}


class RunStatus(StrEnum):
    """The status of a chain's last run (I-17)."""

    PENDING = "pending"
    RUNNING = "running"
    SUSPENDED = "suspended"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    # Its code raised an error that is no end, or diverged from its history: tried again after a
    # backoff, until code that runs it is deployed.
    BLOCKED = "blocked"


@dataclass(frozen=True)
class RunInfo:
    """A live run of a workflow, as `Substantial.runs` lists it."""

    chain: str
    run: str
    version: int | None
    # Its earliest due time; `None` when it waits on messages alone.
    due_at: datetime | None
    # "start", "time" (a timer or a step's retry backoff), "inbox" (a message arrived, consumed
    # at its next wakeup), "message" (idle until one comes, no timer) or "blocked" (by an error
    # of its code, tried again at `due_at`).
    waiting_on: str
    # For a blocked run, the error that blocks it; else `None`.
    error: Failure | None = None


@dataclass(frozen=True)
class Message:
    """A message `start` sends to the chain once the run is created or found: a name, a JSON
    value, and the id it is delivered once per, a ULID when none is given."""

    name: str
    value: Any = None
    id: str | None = None


@dataclass(frozen=True)
class HistoryEvent:
    """An event of a run's history, with the segment that recorded it."""

    # The run of the segment: the chain's last run, or a failed run it resumes.
    run: str
    segment: int
    # When the segment was committed.
    at: datetime
    kind: str
    # The event's fields: times in RFC 3339, failures as {"type", "message", "retryable",
    # "details"}, a value of raw bytes as {"$bytes": <base64>}.
    data: dict[str, Any]


_WAITING = {
    plan.WAITING_START: "start",
    plan.WAITING_TIME: "time",
    plan.WAITING_INBOX: "inbox",
    plan.WAITING_MESSAGE: "message",
    plan.WAITING_BLOCKED: "blocked",
}


def _run_info(info: plan.RunInfo) -> RunInfo:
    waiting_on = _WAITING.get(info.waiting)
    if waiting_on is None:
        raise ProtocolError(f"run {info.run} waits on {info.waiting}")
    return RunInfo(
        chain=info.chain,
        run=info.run,
        version=info.version if info.HasField("version") else None,
        due_at=_datetime(info.due_at) if info.HasField("due_at") else None,
        waiting_on=waiting_on,
        error=Failure.from_proto(info.blocked.error) if info.HasField("blocked") else None,
    )


def _encode(value: Any, what: str) -> bytes:
    try:
        return dumps(value)
    except (TypeError, ValueError) as e:
        # § z6zyx22ss.c@tzc7x
        raise InvalidArgument(f"{what} is not JSON: {e}") from None


class Substantial:
    """An opened namespace: the store, and the workers of this process serving it."""

    def __init__(self, core: Core, store: Store, owned: bool, metering: Metering) -> None:
        self._core = core
        # Observed already when there is an observer.
        self._store = store
        self._owned = owned
        # Its prices and observer, which its workers take.
        self._metering = metering
        self._clock: Clock = REAL
        self._ulid = ulid
        self._workers: weakref.WeakSet[Worker] = weakref.WeakSet()
        # The futures of the `result` waits in progress, given the error of a worker that dies.
        self._waits: set[asyncio.Future[BaseException]] = set()

    async def close(self) -> None:
        """Closes the store, its connections included."""
        if self._owned:
            await self._store.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    @classmethod
    def create(
        cls,
        url: str,
        *,
        credentials: CredentialsArg = None,
        prices: Prices | None = None,
        observe: Observe | None = None,
    ) -> Opening:
        """Creates the namespace, or opens it if it exists. `await` it, or `async with` it to
        close it at the end. `prices` price the store's requests (I-27); `observe` is called with
        each store operation, step attempt, replay, scan, block, history limit warning and skewed
        clock (I-29)."""
        metering = Metering(prices, observe)
        return Opening(cls._opened(open_store(url, credentials), True, _create_plan(), metering))

    @classmethod
    def open(
        cls,
        url: str,
        *,
        credentials: CredentialsArg = None,
        prices: Prices | None = None,
        observe: Observe | None = None,
    ) -> Opening:
        """Opens the namespace; one that does not exist is a `LayoutError`. `await` it, or
        `async with` it to close it at the end. `prices` and `observe` as for `create`."""
        metering = Metering(prices, observe)
        return Opening(cls._opened(open_store(url, credentials), True, _open_plan(), metering))

    @classmethod
    async def _create(
        cls, store: Store, *, prices: Prices | None = None, observe: Observe | None = None
    ) -> Substantial:
        """`create` on a store object, which stays its caller's."""
        return await cls._opened(store, False, _create_plan(), Metering(prices, observe))

    @classmethod
    async def _open(
        cls, store: Store, *, prices: Prices | None = None, observe: Observe | None = None
    ) -> Substantial:
        """`open` on a store object, which stays its caller's."""
        return await cls._opened(store, False, _open_plan(), Metering(prices, observe))

    @classmethod
    async def _opened(
        cls, store: Store, owned: bool, data: plan.PlanInput, metering: Metering
    ) -> Substantial:
        store = metering.wrap(store)
        try:
            try:
                await store.open()
            except StoreFailure as e:
                raise StoreError(e.message) from None
            core = Core.new()
            case, value = outcome_of("open", *await run_plan(core, store, data), "opened")
            if case == "layout_error":
                raise LayoutError(value)
            if case == "config_error":
                raise ConfigError(value)
            # § one zemxe46k3.b@c3rfq
            if case == "failed":
                raise StoreError(value.message)
        except BaseException:
            if owned:
                await store.close()
            raise
        return cls(core, store, owned, metering)

    def worker(
        self, workflows: Iterable[Workflow[..., Any]], *, preset: str | None = None, **settings: Any
    ) -> Worker:
        """A worker serving `workflows`, with the settings of `preset` and these overrides;
        `async with` it, or `serve()` it. The preset is required: `None` only stands for a
        missing argument, refused with `ConfigError`."""
        if preset is None:
            raise ConfigError("a worker needs a preset, script or service")
        resolved = Settings.preset(preset, waiting_store=self._store.capabilities.wait)
        return self._worker(workflows, resolved.override(**settings))

    def _worker(self, workflows: Iterable[Workflow[..., Any]], settings: Settings) -> Worker:
        """A worker with every setting given, public or not."""
        worker = Worker(
            self._store,
            Workflows(workflows),
            settings,
            self._worker_died,
            self._metering,
        )
        self._workers.add(worker)
        return worker

    def _worker_died(self, error: BaseException) -> None:
        for wait in self._waits:
            if not wait.done():
                wait.set_result(error)

    async def start[R](
        self,
        workflow: Workflow[..., R],
        input: dict[str, Any] | None = None,
        *,
        id: str | None = None,
        budget: Mapping[str, int] | None = None,
        message: Message | None = None,
    ) -> Run[R]:
        """Starts a run of `workflow`, idempotently under `id`, a ULID when none is given, with
        `budget`, else its workflow's (I-28): the same id with another budget is `RunExists`.
        With `message`, sends it to the chain once the run is created or found, as `run.send`
        does, and raises as `send` does when it is refused."""
        return await self._start(workflow, input, id, budget, message, upgrade=False)

    async def _start[R](
        self,
        workflow: Workflow[..., R],
        input: dict[str, Any] | None,
        id: str | None,
        budget: Mapping[str, int] | None,
        message: Message | None,
        *,
        upgrade: bool,
    ) -> Run[R]:
        if not isinstance(workflow, Workflow):
            # § bxb0ygj7m@z5fwa
            raise InvalidArgument(f"start takes a workflow, not {workflow!r}")
        # § h8wg39cct.a@dn8v9
        data_input = input_object(input)
        # § end h8wg39cct.a
        check_key("workflow name", workflow.name)
        at = self._clock.now()
        # § cq034wpjp@y88y1
        chain = self._ulid(at.ms) if id is None else check_key("run id", id)
        # § end cq034wpjp
        data = plan.StartPlan(
            prefix="",
            workflow=workflow.name,
            version=workflow.version,
            input=json_value(data_input),
            id=chain,
            now=at,
            budget=effective_budget(budget, workflow),
            upgrade=upgrade,
        )
        if message is not None:
            data.message.CopyFrom(_start_message(message, self._ulid(at.ms)))
        which, started = await self._plan(plan.PlanInput(start=data))
        case, value = outcome_of("start", which, started, "started")
        if case == "run_exists":
            raise RunExists(chain)
        if case == "failed":
            raise StoreError(value.message)
        if case == "versioned":
            # § sfzkmddwj@ar6az
            was = value.version if value.HasField("version") else None
            log.info(
                "schedule %s runs version %s: it moves to version %s at its next boundary that "
                "upgrades",
                chain,
                was,
                workflow.version,
            )
            # § end sfzkmddwj
        self._wake_workers(workflow.name)
        handle = self._handle(workflow, chain)
        if started.HasField("sent"):
            sent = started.sent
            handle._sent_case(sent.WhichOneof("sent"), getattr(sent, sent.WhichOneof("sent")))
        elif message is not None:
            raise ProtocolError(f"start of {chain} with a message answered no send")
        return handle

    async def runs(self, workflow: Workflow[..., Any]) -> AsyncIterator[RunInfo]:
        """The live runs of `workflow`, all its versions: pending, waiting on a timer or a retry
        backoff, overdue, or waiting on messages. Ended runs are not listed."""
        if not isinstance(workflow, Workflow):
            raise InvalidArgument(f"runs takes a workflow, not {workflow!r}")
        check_key("workflow name", workflow.name)
        data = plan.RunsPlan(prefix="", workflow=workflow.name)
        steps = plan_steps(self._core, self._store, plan.PlanInput(runs=data))
        try:
            async for step in steps:
                case, value = outcome_of("runs", *step, "runs")
                if case == "end":
                    return
                if case == "failed":
                    raise StoreError(value.message)
                for info in value.runs:
                    yield _run_info(info)
        finally:
            await steps.aclose()

    async def schedule[R](
        self,
        workflow: Workflow[..., R],
        id: str,
        cron: Cron | None,
        *,
        revision: int,
        backfill: int = 5,
    ) -> Run[R]:
        """Configures the schedule of the run `id` of `workflow`, whose code loops on
        `c.schedule()`: starts it if needed and sends it this config, applied only if `revision`
        is higher than its own. `None` disables it. `backfill` is how many fires missed while no
        worker ran fire when one does, the most recent ones, 5 unless given. A revision already
        applied with the same config does nothing; a higher one applied, or the same with another
        config, raises `ConfigError`."""
        check_config(revision, cron, backfill)
        wanted = config(revision, cron, backfill)
        published = await self.run(workflow, id).read("schedule")
        # § ey8zhz0kq.b@a1b3w
        if published is not None:
            applied = {k: published.get(k) for k in ("revision", "cron", "tz", "backfill")}
            if applied["revision"] > revision:
                raise ConfigError(
                    f"schedule {id}: revision {applied['revision']} is applied already, "
                    f"higher than {revision}"
                )
            if applied["revision"] == revision:
                if applied == wanted:
                    return self.run(workflow, id)
                raise ConfigError(
                    f"schedule {id}: revision {revision} is applied with another config, "
                    f"{applied}, not {wanted}"
                )
        # § end ey8zhz0kq.b
        # § ey8zhz0kq.a@gwavm
        run = await self._start(workflow, None, id, None, None, upgrade=True)
        await run.send("schedule", wanted, id=message_id(wanted))
        # § end ey8zhz0kq.a
        return run

    def run[R](self, workflow: Workflow[..., R], id: str) -> Run[R]:
        """A handle on an existing chain."""
        return self._handle(workflow, id)

    def _handle[R](self, workflow: Workflow[..., R], chain: str) -> Run[R]:
        return Run(self, workflow, chain)

    async def _plan(self, data: plan.PlanInput) -> tuple[str, object]:
        return await run_plan(self._core, self._store, data)

    def _wake_workers(self, workflow: str) -> None:
        """Wakes the workers of this process serving `workflow`, which then list its due
        entries at once instead of after `max_idle`."""
        # § c46hgkfse@sxwbb
        for worker in list(self._workers):
            worker._wake_workflow(workflow)


class Opening:
    """A `Substantial` being opened: `await` it, or `async with` it to close it at the end."""

    _sub: Substantial

    def __init__(self, opened: Coroutine[Any, Any, Substantial]) -> None:
        self._opened = opened

    def __await__(self) -> Generator[Any, None, Substantial]:
        return self._opened.__await__()

    async def __aenter__(self) -> Substantial:
        self._sub = await self._opened
        return self._sub

    async def __aexit__(self, *_: object) -> None:
        await self._sub.close()


def _check_timeout(timeout: timedelta | None) -> None:
    if timeout is not None and not isinstance(timeout, timedelta):
        raise InvalidArgument(f"timeout is a timedelta, not {timeout!r}")
    if timeout is not None and timeout < timedelta(0):
        raise InvalidArgument(f"timeout is not negative: {timeout}")


def _max_interval(max_interval: timedelta | None) -> float:
    """The longest wait between two polls of `result`, in seconds."""
    if max_interval is None:
        # § y025f8qen@fbg2r
        return 30.0
    if not isinstance(max_interval, timedelta) or max_interval <= timedelta(0):
        raise InvalidArgument(f"max_interval is a positive timedelta, not {max_interval!r}")
    return max_interval.total_seconds()


def _datetime(at: layout.Time) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=at.ms)


def _start_message(message: Message, ord: str) -> plan.StartMessage:
    """The message of a start, its id and order made as `run.send` makes them."""
    if not isinstance(message, Message):
        raise InvalidArgument(f"message is an s.Message, not {message!r}")
    check_sent_name(message.name)
    if message.id is not None:
        check_key("message id", message.id)
    return plan.StartMessage(
        name=message.name,
        value=json_value(_encode(message.value, "the message")),
        id=ord if message.id is None else message.id,
        ord=ord,
    )


def _probe() -> str:
    """An id of this opener's own, so that two processes opening one namespace at once never share
    a probe key."""
    # § mgfgmd25p.e@zyn67
    return ulid(REAL.now().ms)


def _create_plan() -> plan.PlanInput:
    return plan.PlanInput(create=plan.CreatePlan(prefix="", probe=_probe()))


def _open_plan() -> plan.PlanInput:
    return plan.PlanInput(open=plan.OpenPlan(prefix="", probe=_probe()))


class Run[R]:
    """A handle on a chain: `send`, `result`, `status`, `cancel`, `history` and `retry` act on its
    last run."""

    def __init__(self, sub: Substantial, workflow: Workflow[..., R], chain: str) -> None:
        self._sub = sub
        self.workflow = workflow
        self.chain = chain

    def __repr__(self) -> str:
        return f"<run {self.chain} of {self.workflow.name}>"

    def _not_found(self) -> RunNotFound:
        return RunNotFound(self.chain)

    async def send(self, name: str, value: Any = None, *, id: str | None = None) -> None:
        """Sends a message, delivered once per `id`, a ULID when none is given."""
        check_key("run id", self.chain)
        check_sent_name(name)
        if id is not None:
            check_key("message id", id)
        at = self._sub._clock.now()
        # § wtwx1e769@xvnm9
        ord = self._sub._ulid(at.ms)
        data = plan.SendPlan(
            prefix="",
            chain=self.chain,
            name=name,
            # § one h8wg39cct.a@nzvre
            value=json_value(_encode(value, "the message")),
            # § one wtwx1e769.a@51554
            id=ord if id is None else id,
            now=at,
            ord=ord,
        )
        # § end wtwx1e769
        await self._sent(plan.PlanInput(send=data))

    async def cancel(self, reason: str) -> None:
        """Cancels the run: it raises `Cancelled` at every call it is blocked on."""
        check_key("run id", self.chain)
        check_str("a reason", reason)
        at = self._sub._clock.now()
        # § wtwx1e769.c@zpm39
        ord = self._sub._ulid(at.ms)
        data = plan.CancelPlan(
            prefix="",
            chain=self.chain,
            reason=reason,
            id=ord,
            now=at,
            ord=ord,
        )
        # § end wtwx1e769.c
        await self._sent(plan.PlanInput(cancel=data))

    async def _sent(self, data: plan.PlanInput) -> None:
        self._sent_case(*outcome_of("send", *await self._sub._plan(data), "sent"))

    def _sent_case(self, case: str | None, value: Any) -> None:
        if case == "accepted":
            self._sub._wake_workers(value.workflow)
        elif case == "refused":
            how = _ENDS.get(value.status)
            if how is None:
                raise ProtocolError(f"send refused by a run {value.status}")
            raise RunEnded(f"run {value.run} has ended, {how}")
        elif case == "no_chain":
            raise self._not_found()
        else:
            raise StoreError(value.message)

    async def status(self) -> RunStatus:
        check_key("run id", self.chain)
        data = plan.StatusPlan(prefix="", chain=self.chain, now=self._sub._clock.now())
        case, value = outcome_of(
            "status", *await self._sub._plan(plan.PlanInput(status=data)), "status"
        )
        if case == "no_chain":
            raise self._not_found()
        if case == "failed":
            raise StoreError(value.message)
        name = plan.RunStatus.Name(value.status).removeprefix("RUN_STATUS_").lower()
        if name == "unspecified":
            raise ProtocolError("unspecified run status")
        return RunStatus(name)

    async def history(self) -> list[HistoryEvent]:
        """The events of the chain's last run as its replay reads them, a retried run's including
        those of the failed run it resumes."""
        check_key("run id", self.chain)
        data = plan.HistoryPlan(prefix="", chain=self.chain)
        case, value = outcome_of(
            "history", *await self._sub._plan(plan.PlanInput(history=data)), "history"
        )
        if case == "no_chain":
            raise self._not_found()
        if case == "failed":
            raise StoreError(value.message)
        return [
            HistoryEvent(
                run=e.run,
                segment=e.segment,
                at=_datetime(e.at),
                kind=e.kind,
                data=json.loads(e.data),
            )
            for e in value.events
        ]

    async def retry(
        self, *, budget: Mapping[str, int] | None = None, keep: int | None = None
    ) -> None:
        """Retries the chain's last run, which failed, keeping its recorded steps: the failed step
        runs again with a fresh retry budget, the others return their recorded values. The retry
        keeps the failed run's budget and its cost so far, unless `budget` gives another; a
        second retry of the same failed run with another budget is `InvalidArgument`.

        With `keep`, resets the chain's last run, ended completed, failed or cancelled: the new
        run replays its first `keep` events of `history()`, and the messages consumed after them
        are consumed again."""
        check_key("run id", self.chain)
        if keep is not None and (not isinstance(keep, int) or isinstance(keep, bool) or keep < 0):
            raise InvalidArgument(f"keep is an int, not negative, not {keep!r}")
        data = plan.RetryPlan(
            prefix="",
            chain=self.chain,
            now=self._sub._clock.now(),
            budget=None if budget is None else budget_proto(budget),
            keep=keep,
        )
        case, value = outcome_of(
            "retry", *await self._sub._plan(plan.PlanInput(retry=data)), "retried"
        )
        if case in ("created", "existing"):
            self._sub._wake_workers(value.workflow)
        elif case == "not_retryable":
            raise InvalidArgument(value.message)
        elif case == "no_chain":
            raise self._not_found()
        else:
            raise StoreError(value.message)

    async def read(self, key: str) -> Any:
        """The last value published under `key` by any run of the chain, or `None`."""
        check_key("run id", self.chain)
        check_key("published key", key)
        data = plan.ReadPlan(prefix="", chain=self.chain, key=key)
        case, value = outcome_of("read", *await self._sub._plan(plan.PlanInput(read=data)), "read")
        if case == "none":
            return None
        if case == "failed":
            raise StoreError(value.message)
        return decode_json(value)

    async def result(
        self, *, timeout: timedelta | None = None, max_interval: timedelta | None = None
    ) -> R:
        """The value the chain ended with, waited for through its continuations; raises
        `RunFailed` if it failed or was cancelled, `WaitTimeout` when `timeout` passes first.
        Polls double their interval up to `max_interval`, 30 s unless given."""
        _check_timeout(timeout)
        cap = _max_interval(max_interval)
        died: asyncio.Future[BaseException] = asyncio.get_running_loop().create_future()
        self._sub._waits.add(died)
        try:
            async with asyncio.timeout(None if timeout is None else timeout.total_seconds()):
                # § y025f8qen@89qx5
                delay, last, etag = min(0.01, cap), None, None
                # § end y025f8qen
                while True:
                    # § y025f8qen.b@09eka
                    # A chain removed meanwhile, its retention passed, ends the wait with
                    # `RunNotFound`: the head is the chain's, so every poll reads it.
                    ended, value = await self._ended(last)
                    if ended:
                        return value
                    last = value.run
                    # § one y025f8qen.e@k8prm
                    if self._sub._store.capabilities.wait:
                        # The first listing waits for nothing: it takes the etag the next one
                        # waits on, and the read after it sees any commit that landed before it.
                        etag = await self._listing_changed(value.result_key, etag, died)
                    else:
                        await asyncio.wait({died}, timeout=delay)
                        # § y025f8qen@dd8sj
                        delay = min(delay * 2, cap)
                        # § end y025f8qen
                    if died.done():
                        raise died.result()
        except TimeoutError:
            raise WaitTimeout(f"run {self.chain} did not end within {timeout}") from None
        finally:
            self._sub._waits.discard(died)

    async def _listing_changed(
        self, key: str, etag: str | None, died: asyncio.Future[BaseException]
    ) -> str | None:
        """The etag of a listing of the head `key`, waited on until it differs from `etag`;
        `None` when a worker of this namespace died first."""
        waiting = asyncio.ensure_future(listing_changed(self._sub._store, key, etag))
        try:
            await asyncio.wait({died, waiting}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            waiting.cancel()
        if not waiting.done() or waiting.cancelled():
            return None
        try:
            return waiting.result()
        except StoreFailure as e:
            raise StoreError(e.message) from e
        except ObserverRaised as e:
            raise e.error from None

    async def _ended(self, last: str | None) -> tuple[bool, Any]:
        """The chain's end, or its last run while it has none: `last` is the run the poll before
        answered, whose `result` alone the core then gets."""
        check_key("run id", self.chain)
        data = plan.ResultPlan(prefix="", chain=self.chain, run=last)
        case, value = outcome_of(
            "result", *await self._sub._plan(plan.PlanInput(result=data)), "result"
        )
        if case == "not_ended":
            return False, value
        if case == "no_chain":
            raise self._not_found()
        if case == "failed":
            raise StoreError(value.message)
        run_id, result = value.run, value.result
        if result.status == layout.STATUS_COMPLETED:
            return True, decode_json(result.value if result.HasField("value") else None)
        if result.status == layout.STATUS_FAILED:
            error = Failure.from_proto(result.error)
            cause = run_failed_cause(error.type, error.details)
            raise RunFailed(run_id, RunStatus.FAILED, error, cause)
        if result.status == layout.STATUS_CANCELLED:
            raise RunFailed(run_id, RunStatus.CANCELLED, Failure("Cancelled", result.reason), None)
        raise ProtocolError(f"result of {run_id} with status {result.status}")
