

import asyncio
import time
import pytest
from dataclasses import dataclass
from substantial.task_queue import MultithreadedQueue
from tests.utils import LogFilter, TimeStep, WorkflowTest, make_sync

from substantial.workflow import workflow, Context

# Further configuration details:
# https://pytest-asyncio.readthedocs.io/en/latest/how-to-guides/multiple_loops.html

# However, each test are still run sequentially

@pytest.mark.asyncio(scope="function")
async def test_simple():
    @workflow("simple-test")
    async def simple_workflow(c: Context, name):
        async def async_op(v):
            return f"C {v}"
        r1 = await c.save(lambda: "A")
        r2 = await c.save(lambda: (lambda: f"B {r1}")())
        r3 = await c.save(lambda: async_op(r2))
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

@pytest.mark.asyncio(scope="function")
async def test_events():
    @dataclass
    class State:
        is_cancelled: bool
        def update(self, *_):
            self.is_cancelled = True

    @workflow()
    async def event_workflow(c: Context, name):
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


@pytest.mark.asyncio(scope="function")
async def test_multiple_workflows_parallel():
    @workflow()
    async def first(c: Context, name):
        v = await c.save(lambda: "first")
        return v

    @workflow()
    async def second(c: Context, name):
        v = await c.save(lambda: "second 1")
        v = await c.save(lambda: f"{v} 2")
        v = await c.save(lambda: f"{v} 3")
        return v

    @workflow()
    async def third(c: Context, name):
        v = await c.save(lambda: "third")
        return v

    t = WorkflowTest()
    def exec(wf):
        # curryfy is necessary as dill will freeze
        # the arg to latest seen if we iter through `arg in [first, second]` for example
        async def test():
            s = t.step().timeout(3)
            s = await s.exec_workflow(wf)
            return len(s.get_logs(LogFilter.runs))
        return test

    todos = [make_sync(exec(wf)) for wf in [first, second, third]]
    start_time = time.time()
    async with MultithreadedQueue(2) as send:
        log_sizes = await asyncio.gather(*[send(todo) for todo in todos])
    end_time = time.time()

    duration = end_time - start_time
    assert log_sizes == [1, 3, 1]
    # 0s      3s       6s
    # 1i --- 1f,3i --- 3f --->
    # 2i --- 2f --------->
    assert duration < 6.2
 
