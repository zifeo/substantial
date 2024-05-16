# from substantial import workflow, DurableBackend
# compact_on
# compensate


from datetime import datetime
from enum import Enum
from typing import Any, Callable, Iterator, Optional
import asyncio
from uuid import uuid4
import uvloop

from dataclasses import dataclass


async def step_1():
    return 1


async def step_2(n):
    return n + 2


async def step_3(n):
    return n * 2


async def step_4(n, d):
    return n / d


class Interrupt(BaseException):
    pass


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


class LogKind(str, Enum):
    Save = "save"
    EventIn = "event_in"
    EventOut = "event_out"
    Meta = "meta"


@dataclass
class Log:
    handle: str
    kind: LogKind
    data: Any
    at: datetime = datetime.now()


Empty: Any = object()


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

    async def save(self, callable: Callable) -> Any:
        val = self.unroll(LogKind.Save)
        if val is Empty:
            val = await callable()
            self.source(LogKind.Save, val)
            return val
        else:
            self.source(LogKind.Meta, f"reused {val.data}")
            return val.data

    def register(self, event_name: str, callback: Any):
        self.source(LogKind.Meta, f"registering... {event_name}")
        self.events[event_name] = callback

    async def wait(self, condition: Callable[[], bool]):
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
            raise Interrupt()

    async def event(self, event_name: str):
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


@dataclass
class State:
    is_cancelled: bool

    def cancel(self):
        self.is_cancelled = True


@workflow(1)
async def deploy(c: Context, name, n):
    r1 = await c.save(lambda: step_1())

    r2 = await c.save(lambda: step_2(r1))

    r3 = await c.save(lambda: step_3(r2))

    n = await c.event("n")

    s = State(is_cancelled=False)

    c.register("cancel", s.cancel)

    await c.wait(lambda: s.is_cancelled)

    if s.is_cancelled:
        r4 = await c.save(lambda: step_4(r3, n))

    return r4


@dataclass
class Event:
    handle: str
    name: str
    data: Any
    future: asyncio.Future
    at: datetime = datetime.now()


class SubstantialMemoryConductor:
    def __init__(self):
        # num workers
        self.known_workflows = {}
        self.events = asyncio.Queue()
        self.workflows = asyncio.Queue()
        self.runs = dict()

    def register(self, workflow):
        self.known_workflows[workflow.id] = workflow

    async def start(self, workflow_run):
        await self.workflows.put(workflow_run)
        return workflow_run.handle

    async def send(self, handle, event_name, *args):
        ret = asyncio.Future()
        if handle not in self.runs:
            self.runs[handle] = []
        self.runs[handle].append(Log(handle, LogKind.EventIn, (event_name, args)))
        await self.events.put(Event(handle, event_name, args, ret))
        return ret

    def get_logs(self, handle):
        if handle not in self.runs:
            self.runs[handle] = []
        return self.runs[handle]

    def log(self, log: Log):
        dt = log.at.strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"{dt} [{log.handle}] {log.kind} {log.data}")
        self.runs[log.handle].append(log)

    async def schedule_later(self, queue, task, secs):
        await asyncio.sleep(secs)
        await queue.put(task)

    async def run(self):
        e = asyncio.create_task(self.run_events())
        w = asyncio.create_task(self.run_workflows())
        return await asyncio.gather(e, w)

    async def run_events(self):
        while True:
            event = await self.events.get()
            logs = self.get_logs(event.handle)

            res = Empty
            for log in logs[::-1]:
                if log.kind == LogKind.EventOut and log.data[0] == event.name:
                    res = log.data[1]
                    break

            if res is Empty:
                asyncio.create_task(self.schedule_later(self.events, event, 3))
            else:
                event.future.set_result(event.data)
                event.future.done()

            self.events.task_done()

    async def run_workflows(self):
        while True:
            workflow_run = await self.workflows.get()
            try:
                await workflow_run.replay(self)
            except Interrupt:
                print("Interrupted")
                asyncio.create_task(
                    self.schedule_later(self.workflows, workflow_run, 3)
                )
            except Exception as e:
                print(f"Workflow error: {e}")
                # retry
            self.workflows.task_done()


async def main():
    substantial = SubstantialMemoryConductor()
    substantial.register(deploy)

    execution = asyncio.create_task(substantial.run())

    workflow_run = deploy("test", 1)

    handle = await substantial.start(workflow_run)
    print(handle)

    await asyncio.sleep(1)

    print(await substantial.send(handle, "n", 3))

    await asyncio.sleep(6)

    print(await substantial.send(handle, "cancel"))

    await execution


with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
    runner.run(main())
