

import asyncio
from dataclasses import dataclass

import pytest

from tests.complex.utils import LogFilter, StepError, TimeStep, WorkflowTest

from substantial.workflow import workflow, Context

@pytest.mark.asyncio(scope="function") # ensure one main loop per function
class TestInOneEventLoopPerClass(WorkflowTest):
    async def test_async(t):
        await asyncio.sleep(1)
        assert 1 + 1 is 2

    async def test_test(t):
        with pytest.raises(StepError) as info:
            t.step("A").logs_data_equal(LogFilter.runs, [])
        assert info.value.args[0] == "'A': No workflow has been run prior the call"

    async def test_simple(t):
        @workflow("simple")
        async def simple_workflow(c: Context, name, n):
            r1 = await c.save(lambda: "A")
            r2 = await c.save(lambda: f"B {r1}")
            r3 = await c.save(lambda: f"C {r2}")
            return r3

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

    async def test_events(t):
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

        s = t.step().timeout(10)
        s = await (
            s
            .timeline(TimeStep("sayHello", 1, "Hello from outside!"))
            .timeline(TimeStep("cancel", 5))
            .exec_workflow(event_workflow)
        )
        s.logs_data_equal(LogFilter.runs, ['A', 'Hello from outside! B A'])

