# from substantial import workflow, DurableBackend
# compact_on
# compensate

import asyncio
from typing import Any, Callable, Iterator, Optional, Union
from uuid import uuid4

from substantial.types import AppError, Interrupt, Log, LogKind, Activity, Empty, RetryStrategy


class WorkflowRun:
    # sleep
    # uuid4 / time / random now
    # continue_as_new
    # wait_condition
    # child_workflow

    def __init__(
        self,
        workflow: "Workflow",
        run_id,
        args,
        kwargs,
    ):
        self.run_id = run_id
        self.workflow = workflow
        self.args = args
        self.kwargs = kwargs

    @property
    def handle(self):
        return f"{self.workflow.id}-{self.run_id}"

    async def replay(self, backend):
        print("-----------------replay----------")
        logs = backend.get_logs(self.handle)
        durable_logs = (e for e in logs if e.kind != LogKind.Meta)

        ctx = Context(self.handle, backend.log, durable_logs)
        ctx.source(LogKind.Meta, f"replaying with {len(logs)}...")
        try:
            ret = await self.workflow.f(ctx, *self.args, **self.kwargs)
        except Interrupt:
            ctx.source(LogKind.Meta, "waiting for condition...")
            raise
        except Exception as e:
            ctx.source(LogKind.Meta, f"error: {e}")
            raise
        finally:
            ctx.source(LogKind.Meta, "replayed")

        return ret


class Context:
    def __init__(self, handle, log_event, logs: Iterator[Log]):
        self.handle = handle
        self.log_event = log_event
        self.logs = logs
        self.events = {}

    def source(self, kind, data):
        self.log_event(Log(self.handle, kind, data))

    def unroll(self, kind: str):
        while True:
            event = next(self.logs, Empty)
            if event is Empty:
                return Empty

            if event.kind == kind:
                return event

    async def save(
        self,
        callable: Callable,
        *,
        timeout: Union[int, None] = None,
        retry_strategy: Union[RetryStrategy, None] = None
    ) -> Any:
        """ Force idempotency on `callable` and add `Activity` like behavior """
        val = self.unroll(LogKind.Save)
        if val is Empty:
            activity = Activity(callable, timeout, retry_strategy)
            val = await activity.exec()
            self.source(LogKind.Save, val)
            # return val
            raise Interrupt("Must replay")
        else:
            self.source(LogKind.Meta, f"reused {val.data}")
            return val.data
    
    async def sleep(duration_sec: int) -> Any:
        if duration_sec <= 0:
            raise AppError(f"Invalid timeout value: {duration_sec}")
        await asyncio.sleep(duration_sec)

    def register(self, event_name: str, callback: Any):
        self.source(LogKind.Meta, f"registering... {event_name}")
        self.events[event_name] = callback

    async def wait(self, condition: Callable[[], bool]):
        """ Wait for `condition()` to be True """
        self.source(LogKind.Meta, "waiting...")
        event = self.unroll(LogKind.EventIn)
        if event is not Empty:
            callback = self.events.get(event.data[0])
            if callback is not None:
                ret = callback(*event.data[1])
                res = self.unroll(LogKind.EventOut)
                if res is not Empty:
                    self.source(LogKind.Meta, f"reused {event.data[1]}")
                else:
                    self.source(LogKind.EventOut, (event.data[0], ret))

        if not condition():
            raise Interrupt("wait => not condition")

    async def event(self, event_name: str):
        """ Register a new event that can be triggered from outside the workflow """
        proxy = {}
        self.register(event_name, lambda x: proxy.update(val=x))
        await self.wait(lambda: "val" in proxy)
        return proxy["val"]


class Workflow:
    def __init__(
        self,
        f: Callable[..., Any],
        workflow_version: int,
        workflow_name: Optional[str] = None,
        # multiple queues
        # timeout
        # user-defined migration
        # retry track and backoff
    ):
        if workflow_name is None:
            workflow_name = f.__name__

        self.id = f"{workflow_name}-{workflow_version}"
        self.f = f

    def __call__(self, *args, **kwargs):
        run_id = uuid4()
        return WorkflowRun(self, run_id, args, kwargs)


def workflow(*args, **kwargs):
    def wrapper(f):
        return Workflow(f, *args, **kwargs)

    return wrapper
