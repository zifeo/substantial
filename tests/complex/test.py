

import asyncio

import pytest

from tests.complex.utils import LogFilter, WorkflowTest
from tests.complex.workflows.simple import simple_workflow

@pytest.mark.asyncio(scope="class") # ensure one main loop per class
class TestInOneEventLoopPerClass(WorkflowTest):
    async def test_test(t):
        await asyncio.sleep(0.2)
        assert True

    async def test_simple(t):
        s = t.step().timeout(3)
        s = await s.exec_workflow(simple_workflow, "simple", 1)
        (
            s
            .logs_data_equal(LogFilter.runs, ['A', 'B A', 'C B A'])
            .logs_data_equal(LogFilter.event, [])
        )
