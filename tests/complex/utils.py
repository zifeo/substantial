# Equiv. server + worker
import asyncio
from enum import Enum
from typing import List

from substantial.conductor import Recorder, SubstantialMemoryConductor
from substantial.types import Log

class LogFilter(str, Enum):
    event = "event"
    runs = "runs"

class StepError(Exception):
    def __init__(self, step: str, message) -> None:
        super().__init__(f"At step '{step}': {message}")

class WorkflowTest:
    timeout_secs = 120
    name: str
    curr_handle: str | None = None
    curr_record: Recorder | None = None

    def error(self, message: str):
        return StepError(self.name, message)

    def step(self, name: str | None = None):
        runner = WorkflowTest()
        runner.name = name or "<unnamed>"
        return runner

    def timeout(self, timeout_secs: int):
        self.timeout_secs = timeout_secs
        return self

    def logs_data_equal(
        self,
        filter: LogFilter,
        other: List[Log] | List[any],
    ):
        res = []
        if self.curr_record is None or self.curr_handle is None:
            raise self.error("No workflow has been run prior the call")
        
        if filter.name == LogFilter.runs:
            res = self.curr_record.get_recorded_runs(self.curr_handle)
        else:
            res = self.curr_record.get_recorded_events(self.curr_handle)
        if len(res) == 0 and len(other) == 0:
            return self
        if isinstance(res[0], Log):
            res = [log.data for log in res]
        if isinstance(other[0], Log):
            other = [log.data for log in other]
        assert res == other

        return self

    async def logs_run_within(
        self,
        logs: List[Log],
        delta_time_secs: List[int] | int
    ):
        """ Compare elapsed time in between two rows """
        pass

    async def exec_workflow(
        self,
        workflow,
        name: str,
        version: int,
    ):
        substantial = SubstantialMemoryConductor()
        substantial.register(workflow)
        backend_exec = asyncio.create_task(substantial.run())

        workflow_run = workflow(name, version)
        handle = workflow_run.handle

        async def go():
            await substantial.start(workflow_run),
            await backend_exec
        try:
            await asyncio.wait_for(go(), self.timeout_secs)
        except TimeoutError:
            pass
        except:
            raise
        
        self.curr_record = substantial.runs
        self.curr_handle = handle 

        return self
