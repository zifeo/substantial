# from substantial import workflow, DurableBackend
# compact_on
# compensate

import asyncio
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union
from uuid import uuid4
from datetime import timedelta

if TYPE_CHECKING:
    from substantial.conductor import Backend
from substantial.types import AppError, CancelWorkflow, EventData, Interrupt, Log, LogKind, ValueEval, Empty, RetryStrategy


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
    ):
        self.run_id = run_id
        self.workflow = workflow
        self.replayed = False

    @property
    def handle(self) -> str:
        return f"{self.workflow.id}-{self.run_id}"

    async def replay(self, backend: 'Backend'):
        print("----------------- replay -----------------")
        # if not self.replayed:
        #     backend.load_file("logs/example", self.handle)
        #     self.replayed = True

        run_logs = backend.get_run_logs(self.handle)
        events_logs = backend.get_event_logs(self.handle)

        ctx = Context(self.handle, backend.log, run_logs, events_logs)
        ctx.source(LogKind.Meta, f"replaying ...")
        try:
            ret = await self.workflow.f(ctx, self.workflow.id)
            self.replayed = True
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
    def __init__(
        self,
        handle: str,
        backend_event_logger, # TODO: memory for now, should be typed with an interface later
        run_logs: List[Log],
        event_logs: List[Log],
    ):
        self.handle = handle
        self.backend_event_logger = backend_event_logger
        self.run_logs = iter(run_logs)
        self.event_logs = iter(event_logs)
        self.events = {}

    def source(self, kind: LogKind, data: any):
        self.backend_event_logger(Log(self.handle, kind, data))

    def __unqueue_up_to(self, kind: LogKind):
        """ unshift, popfront: discard old events till kind is found """
        event = Empty
        logs = self.run_logs
        if kind == LogKind.EventIn or kind == LogKind.EventOut:
            logs = self.event_logs
        while True:
            event = next(logs, Empty)
            if event is Empty or event.kind == kind:
                break
        return event

    async def save(
        self,
        callable: Callable,
        *,
        timeout: Union[timedelta, None] = None,
        retry_strategy: Union[RetryStrategy, None] = None
    ) -> Any:
        """ Force idempotency on `callable` and add `ValueEval` like behavior """
        val = self.__unqueue_up_to(LogKind.Save)
        if val is Empty:
            timeout_secs = timeout.total_seconds() if timeout is not None else None
            value_eval = ValueEval(callable, timeout_secs, retry_strategy)
            val = await value_eval.exec()
            self.source(LogKind.Save, val)
            return val
        else:
            self.source(LogKind.Meta, f"reused {val.data}")
            return val.data
    
    async def sleep(self, duration: timedelta) -> Any:
        seconds = duration.total_seconds()
        if seconds <= 0:
            raise AppError(f"Invalid timeout value: {seconds}")
        val = self.__unqueue_up_to(LogKind.Sleep)
        if val is Empty:
            await asyncio.sleep(seconds)
            self.source(LogKind.Sleep, None)
        else:
            self.source(LogKind.Meta, f"{seconds}s sleep already executed")

    def register(self, event_name: str, callback: Any):
        self.source(LogKind.Meta, f"registering... {event_name}")
        self.events[event_name] = callback

    async def wait_on(self, condition: Callable[[], bool]):
        """ Wait for `condition()` to be True """
        self.source(LogKind.Meta, "waiting...")
        event = self.__unqueue_up_to(LogKind.EventIn)
        if event is not Empty:
            data: EventData = event.data
            callback = self.events.get(data.event_name)
            if callback is not None:
                ret = callback(*tuple(data.args))
                res = self.__unqueue_up_to(LogKind.EventOut)
                if res is not Empty:
                    self.source(LogKind.Meta, f"reused {data.args}")
                else:
                    self.source(LogKind.EventOut, EventData(data.event_name, ret))

        result = condition()
        if not result:
            raise Interrupt("wait => not condition")
        return result

    async def event(self, event_name: str):
        """ Register a new event that can be triggered from outside the workflow """
        proxy = {}
        self.register(event_name, lambda x: proxy.update(val=x))
        await self.wait_on(lambda: "val" in proxy)
        return proxy["val"]

    def cancel_run(self):
        """ Interrupt after the call """
        raise CancelWorkflow(self.handle)


class Workflow:
    def __init__(
        self,
        f: Callable[..., Any],
        workflow_name: Optional[str] = None,
        # multiple queues
        # timeout
        # user-defined migration
        # retry track and backoff
    ):
        if workflow_name is None:
            workflow_name = f.__name__

        self.id = workflow_name
        self.f = f

    def __call__(self):
        run_id = uuid4()
        return WorkflowRun(self, run_id)


def workflow(name: Union[str, None] = None):
    def wrapper(f):
        return Workflow(f, name or f.__name__)

    return wrapper
