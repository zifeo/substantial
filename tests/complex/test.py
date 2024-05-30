

import asyncio

import pytest
import uvloop

from tests.complex.utils import execute_workflow
from tests.complex.workflows.simple import simple_workflow

from unittest import TestCase

@pytest.mark.asyncio(scope="class")
class TestInOneEventLoopPerClass():
    async def test_test(self):
        await asyncio.sleep(0.2)
        assert True

    async def test_simple(self):
        timeout = 3
        handle, recorder = await execute_workflow(simple_workflow, timeout)
        # TODO: wrap this in an utility
        # TODO: measure delta time (useful for sleeps and replayed sleeps)
        # runs
        assert [log.data for log in recorder.logs[handle]] == ['A', 'B A', 'C B A']
        # events
        assert len(recorder.events[handle]) == 0
