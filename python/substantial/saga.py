"""Rollbacks and sagas (I-10): the core says which steps to undo, the host runs their undos."""

from __future__ import annotations

import asyncio
from types import TracebackType

from substantial._proto import boundary, run
from substantial.errors import ContinuedAsNew, RolledBack, StepFailed, UndoFailed
from substantial.scheduler import Shared, StepSpec, decode_json
from substantial.step import SAGA


async def run_undos(shared: Shared, undos: run.Called.Undos) -> None:
    """Runs each undo as the step the core names, with the retry and timeout of the step it
    undoes."""
    for u in undos.undos:
        found = shared.undos.get(u.call)
        if found is None:
            raise shared.protocol(f"undo of call {u.call} without an undo")
        make, step_options = found
        options = run.StepOptions()
        options.CopyFrom(step_options)
        options.occurrence = u.occurrence
        call = run.Call(step=run.Call.Step(name=u.name, options=options))
        value = decode_json(u.value if u.HasField("value") else None)
        try:
            await shared.future(call, decode_json, StepSpec(make(value)))
        except StepFailed as e:
            raise UndoFailed(u.name, e.failure) from None


async def rollback(shared: Shared, reason: str) -> None:
    undos = shared.expect(run.Call(rollback=boundary.Empty()), "undos")
    await run_undos(shared, undos)
    raise RolledBack(reason)


class Saga:
    """`async with c.saga():` on an error inside, the undos of its steps, then the error again."""

    def __init__(self, shared: Shared) -> None:
        self._shared = shared
        self._id: int | None = None
        self._token = None

    async def __aenter__(self) -> None:
        if self._id is not None:
            raise RuntimeError("a saga is entered once")
        self._id = self._shared.expect(run.Call(saga_begin=boundary.Empty()), "saga")
        self._token = SAGA.set(self._id)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        SAGA.reset(self._token)  # type: ignore[arg-type]
        if isinstance(exc, asyncio.CancelledError):
            # The replay is closing its code: nothing more is recorded.
            return False
        # Continuing as new is no failure: nothing is undone.
        failed = exc is not None and not isinstance(exc, ContinuedAsNew)
        # An inner saga ending without error hands its steps to the enclosing one.
        parent = SAGA.get()
        fields = {"saga": self._id, "failed": failed}
        if parent is not None:
            fields["parent"] = parent
        end = run.Call(saga_end=run.Call.SagaEnd(**fields))
        if failed:
            await run_undos(self._shared, self._shared.expect(end, "undos"))
        else:
            self._shared.expect(end, "done")
        return False
