

import asyncio

import pytest

from tests.complex.utils import LogFilter, StepError, TimeStep, WorkflowTest
from tests.complex.workflows.events import event_workflow
from tests.complex.workflows.simple import simple_workflow

@pytest.mark.asyncio(scope="class") # ensure one main loop per class
class TestInOneEventLoopPerClass(WorkflowTest):
    async def test_async(t):
        await asyncio.sleep(1)
        assert 1 + 1 is 2

    async def test_test(t):
        with pytest.raises(StepError) as info:
            t.step("A").logs_data_equal(LogFilter.runs, [])
        assert info.value.args[0] == "'A': No workflow has been run prior the call"

    async def test_simple(t):
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
        s = t.step().timeout(10)
        s = await (
            s
            .timeline(TimeStep("sayHello", 1, "Hello from outside!"))
            .timeline(TimeStep("cancel", 5))
            .exec_workflow(event_workflow)
        )
        s.logs_data_equal(LogFilter.runs, ['A', 'Hello from outside! B A'])
