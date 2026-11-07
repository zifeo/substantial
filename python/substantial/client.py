"""Creating and opening a namespace (I-16), starting and following runs (I-17), and workers."""

from __future__ import annotations

import asyncio
import json
import weakref
from collections.abc import Coroutine, Generator, Iterable
from datetime import timedelta
from enum import StrEnum
from typing import Any, Self

from substantial._proto import json_value, layout, plan
from substantial.core import Core
from substantial.durations import now
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
)
from substantial.names import check_key, check_name, check_str
from substantial.plan import outcome_of, run_plan, ulid
from substantial.scheduler import decode_json
from substantial.settings import Settings
from substantial.stores import Store, StoreFailure, open_store
from substantial.stores.credentials import CredentialsArg
from substantial.stores.s3 import result_created
from substantial.worker import Worker
from substantial.workflow import Workflow, Workflows


class RunStatus(StrEnum):
    """The status of a chain's last run (I-17)."""

    PENDING = "pending"
    RUNNING = "running"
    SUSPENDED = "suspended"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


def _encode(value: Any, what: str) -> bytes:
    try:
        return json.dumps(value, allow_nan=False).encode()
    except (TypeError, ValueError) as e:
        raise InvalidArgument(f"{what} is not JSON: {e}") from None


class Substantial:
    """An opened namespace: the store, its partitions, and the workers of this process serving
    it."""

    def __init__(self, core: Core, store: Store, partitions: int, owned: bool) -> None:
        self._core = core
        self._store = store
        self._owned = owned
        self.partitions = partitions
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
        cls, url: str, *, partitions: int | None = None, credentials: CredentialsArg = None
    ) -> Opening:
        """Creates the namespace, or opens it if it exists with the same `partitions`, which
        are required: `None` only stands for a missing argument, refused with `ConfigError`.
        `await` it, or `async with` it to close it at the end."""
        data = _create_plan(partitions)
        return Opening(cls._opened(open_store(url, credentials), True, data))

    @classmethod
    def open(cls, url: str, *, credentials: CredentialsArg = None) -> Opening:
        """Opens the namespace; one that does not exist is a `LayoutError`. `await` it, or
        `async with` it to close it at the end."""
        return Opening(cls._opened(open_store(url, credentials), True, _open_plan()))

    @classmethod
    async def _create(cls, store: Store, *, partitions: int | None) -> Substantial:
        """`create` on a store object, which stays its caller's."""
        return await cls._opened(store, False, _create_plan(partitions))

    @classmethod
    async def _open(cls, store: Store) -> Substantial:
        """`open` on a store object, which stays its caller's."""
        return await cls._opened(store, False, _open_plan())

    @classmethod
    async def _opened(cls, store: Store, owned: bool, data: plan.PlanInput) -> Substantial:
        try:
            core = Core.new()
            case, value = outcome_of("open", *await run_plan(core, store, data), "opened")
            if case == "layout_error":
                raise LayoutError(value)
            if case == "failed":
                raise StoreError(value.message)
        except BaseException:
            if owned:
                await store.close()
            raise
        return cls(core, store, value, owned)

    def worker(
        self, workflows: Iterable[Workflow[..., Any]], *, preset: str | None = None, **settings: Any
    ) -> Worker:
        """A worker serving `workflows` on every partition, with the settings of `preset` and
        these overrides; `async with` it, or `serve()` it. The preset is required: `None` only
        stands for a missing argument, refused with `ConfigError`."""
        if preset is None:
            raise ConfigError("a worker needs a preset, script or service")
        resolved = Settings.preset(preset, waiting_store=self._store.capabilities.wait)
        return self._worker(workflows, resolved.override(**settings))

    def _worker(self, workflows: Iterable[Workflow[..., Any]], settings: Settings) -> Worker:
        """A worker with every setting given, public or not."""
        worker = Worker(
            self._store, Workflows(workflows), self.partitions, settings, self._worker_died
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
    ) -> Run[R]:
        """Starts a run of `workflow`, idempotently under `id`, a ULID when none is given."""
        if not isinstance(workflow, Workflow):
            raise InvalidArgument(f"start takes a workflow, not {workflow!r}")
        if input is None:
            input = {}
        if not isinstance(input, dict):
            raise InvalidArgument(f"an input is a JSON object, not {input!r}")
        check_key("workflow name", workflow.name)
        at = now()
        chain = ulid(at.ms) if id is None else check_key("run id", id)
        data = plan.StartPlan(
            prefix="",
            partitions=self.partitions,
            workflow=workflow.name,
            version=workflow.version,
            input=json_value(_encode(input, "the input")),
            id=chain,
            now=at,
        )
        case, value = outcome_of("start", *await self._plan(plan.PlanInput(start=data)), "started")
        if case == "run_exists":
            raise RunExists(f"id {chain} is taken by another workflow, version or input")
        if case == "failed":
            raise StoreError(value.message)
        self._wake_workers()
        return Run(self, workflow, chain)

    def run[R](self, workflow: Workflow[..., R], id: str) -> Run[R]:
        """A handle on an existing chain."""
        return Run(self, workflow, id)

    async def _plan(self, data: plan.PlanInput) -> tuple[str, object]:
        return await run_plan(self._core, self._store, data)

    def _wake_workers(self) -> None:
        """Wakes the workers of this process, which then list at once instead of after
        `max_idle`."""
        for worker in list(self._workers):
            worker._wake()


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


def _create_plan(partitions: int | None) -> plan.PlanInput:
    if partitions is None:
        raise ConfigError("create needs partitions, a positive whole number")
    if isinstance(partitions, bool) or not isinstance(partitions, int) or partitions <= 0:
        raise ConfigError(f"partitions must be a positive whole number, not {partitions!r}")
    data = plan.CreatePlan(prefix="", partitions=partitions)
    return plan.PlanInput(create=data)


def _open_plan() -> plan.PlanInput:
    return plan.PlanInput(open=plan.OpenPlan(prefix=""))


class Run[R]:
    """A handle on a chain: `send`, `result`, `status` and `cancel` act on its last run."""

    def __init__(self, sub: Substantial, workflow: Workflow[..., R], chain: str) -> None:
        self._sub = sub
        self.workflow = workflow
        self.chain = chain

    def __repr__(self) -> str:
        return f"<run {self.chain} of {self.workflow.name}>"

    def _not_found(self) -> RunNotFound:
        return RunNotFound(f"no chain {self.chain}")

    async def send(self, name: str, value: Any = None, *, id: str | None = None) -> None:
        """Sends a message, delivered once per `id`, a ULID when none is given."""
        check_key("run id", self.chain)
        check_name("message name", name)
        if id is not None:
            check_key("message id", id)
        at = now()
        data = plan.SendPlan(
            prefix="",
            partitions=self._sub.partitions,
            chain=self.chain,
            name=name,
            value=json_value(_encode(value, "the message")),
            id=ulid(at.ms) if id is None else id,
            now=at,
        )
        await self._sent(plan.PlanInput(send=data))

    async def cancel(self, reason: str) -> None:
        """Cancels the run: it raises `Cancelled` at every call it is blocked on."""
        check_key("run id", self.chain)
        check_str("a reason", reason)
        at = now()
        data = plan.CancelPlan(
            prefix="",
            partitions=self._sub.partitions,
            chain=self.chain,
            reason=reason,
            id=ulid(at.ms),
            now=at,
        )
        await self._sent(plan.PlanInput(cancel=data))

    async def _sent(self, data: plan.PlanInput) -> None:
        case, value = outcome_of("send", *await self._sub._plan(data), "sent")
        if case == "accepted":
            self._sub._wake_workers()
        elif case == "refused":
            raise RunEnded(f"run {value.run} has ended")
        elif case == "no_chain":
            raise self._not_found()
        else:
            raise StoreError(value.message)

    async def status(self) -> RunStatus:
        check_key("run id", self.chain)
        data = plan.StatusPlan(
            prefix="", partitions=self._sub.partitions, chain=self.chain, now=now()
        )
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

    async def read(self, key: str) -> Any:
        """The last value published under `key` by any run of the chain, or `None`."""
        check_key("run id", self.chain)
        check_key("published key", key)
        data = plan.ReadPlan(prefix="", partitions=self._sub.partitions, chain=self.chain, key=key)
        case, value = outcome_of("read", *await self._sub._plan(plan.PlanInput(read=data)), "read")
        if case == "none":
            return None
        if case == "failed":
            raise StoreError(value.message)
        return decode_json(value)

    async def result(self, *, timeout: timedelta | None = None) -> R:
        """The value the chain ended with, waited for through its continuations; raises
        `RunFailed` if it failed or was cancelled, `WaitTimeout` when `timeout` passes first."""
        if timeout is not None and not isinstance(timeout, timedelta):
            raise InvalidArgument(f"timeout is a timedelta, not {timeout!r}")
        died: asyncio.Future[BaseException] = asyncio.get_running_loop().create_future()
        self._sub._waits.add(died)
        try:
            async with asyncio.timeout(None if timeout is None else timeout.total_seconds()):
                delay, last = 0.01, None
                while True:
                    ended, value = await self._ended(last)
                    if ended:
                        return value
                    last = value.run
                    if self._sub._store.capabilities.wait:
                        await self._result_created(value.result_key, died)
                    else:
                        await asyncio.wait({died}, timeout=delay)
                        delay = min(delay * 2, 30.0)
                    if died.done():
                        raise died.result()
        except TimeoutError:
            raise WaitTimeout(f"run {self.chain} did not end within {timeout}") from None
        finally:
            self._sub._waits.discard(died)

    async def _result_created(self, key: str, died: asyncio.Future[BaseException]) -> None:
        waiting = asyncio.ensure_future(result_created(self._sub._store, key))
        try:
            await asyncio.wait({died, waiting}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            waiting.cancel()
        if waiting.done() and not waiting.cancelled():
            try:
                waiting.result()
            except StoreFailure as e:
                raise StoreError(e.message) from e

    async def _ended(self, last: str | None) -> tuple[bool, Any]:
        """The chain's end, or its last run while it has none: `last` is the run the poll before
        answered, whose `result` alone the core then gets."""
        check_key("run id", self.chain)
        partitions = self._sub.partitions
        data = plan.ResultPlan(prefix="", partitions=partitions, chain=self.chain, run=last)
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
            raise RunFailed(run_id, RunStatus.FAILED, Failure.from_proto(result.error))
        if result.status == layout.STATUS_CANCELLED:
            raise RunFailed(run_id, RunStatus.CANCELLED, Failure("Cancelled", result.reason))
        raise ProtocolError(f"result of {run_id} with status {result.status}")
