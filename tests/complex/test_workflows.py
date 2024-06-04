

import asyncio
import logging
from time import sleep
import pytest
from dataclasses import dataclass
from substantial.task_queue import MultiTaskQueue
from tests.complex.utils import LogFilter, StepError, TimeStep, WorkflowTest, make_sync

from substantial.workflow import workflow, Context

# Further configuration details:
# https://pytest-asyncio.readthedocs.io/en/latest/how-to-guides/multiple_loops.html

# However, each test are still run sequentially

@pytest.mark.asyncio(scope="module")
async def test_simple():
    @workflow("simple")
    async def simple_workflow(c: Context, name, n):
        r1 = await c.save(lambda: "A")
        r2 = await c.save(lambda: f"B {r1}")
        r3 = await c.save(lambda: f"C {r2}")
        return r3

    t = WorkflowTest()
    s = await (
        t
        .step()
        .timeout(3)
        .exec_workflow(simple_workflow)
    )
    (
        s
        .logs_data_equal(LogFilter.runs, ['A', 'B A', 'C B A'])
        .logs_data_equal(LogFilter.event, [])
    )

@pytest.mark.asyncio(scope="module")
async def test_events():
    @dataclass
    class State:
        is_cancelled: bool
        def update(self, *_):
            self.is_cancelled = True

    @workflow(1, "events")
    async def event_workflow(c: Context, name, n):
        r1 = await c.save(lambda: "A")
        payload = await c.event("sayHello")

        s = State(False)
        c.register("cancel", s.update)
        await c.wait(lambda: s.is_cancelled)

        if s.is_cancelled:
            r3 = await c.save(lambda: f"{payload} B {r1}")
        return r3

    t = WorkflowTest()
    s = t.step().timeout(10)
    s = await (
        s
        .timeline(TimeStep("sayHello", 1, "Hello from outside!"))
        .timeline(TimeStep("cancel", 5))
        .exec_workflow(event_workflow)
    )
    s.logs_data_equal(LogFilter.runs, ['A', 'Hello from outside! B A'])


# FIXME: still blocking
# @pytest.mark.asyncio(scope="module")
# async def test_multiple_workflows_parallel():
#     @workflow(1, "first")
#     async def first(c: Context, name, n):
#         v = await c.save(lambda: "first")
#         return v

#     @workflow(1, "second")
#     async def second(c: Context, name, n):
#         v = await c.save(lambda: "second")
#         return v

#     def exec(wf):
#         # curryfy is necessary as dill will freeze
#         # the arg to latest seen if we iter through `arg in [first, second]` for example
#         async def test():
#             t = WorkflowTest()
#             s = t.step().timeout(10)
#             s = await s.exec_workflow(wf)
#             assert len(s.recorder.logs) == 1
#         return test

#     todos = [exec(first), exec(second)]
#     # todos = [exec(first)]
#     async with MultiTaskQueue(2) as send:
#         _rets = await asyncio.gather(*[send(make_sync(todo)) for todo in todos])
