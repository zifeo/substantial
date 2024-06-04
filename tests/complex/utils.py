import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import List, Union

import uvloop

from substantial.conductor import Recorder, SubstantialMemoryConductor
from substantial.types import Log
from substantial.workflow import Workflow

class LogFilter(str, Enum):
    event = "event"
    runs = "runs"

@dataclass
class TimeStep:
    event_name: str
    delta_time: int
    payload: Union[any, None] = None

class StepError(Exception):
    def __init__(self, step: str, message) -> None:
        super().__init__(f"'{step}': {message}")

class WorkflowTest:
    timeout_secs = 120
    name: str
    handle: str | None = None
    recorder: Recorder | None = None
    event_timeline: List[TimeStep] = []

    def error(self, message: str):
        return StepError(self.name, message)

    def step(self, name: str | None = None):
        """ Prepare a new test scope """
        runner = WorkflowTest()
        runner.name = name or "<unnamed>"
        return runner

    def timeout(self, timeout_secs: int):
        self.timeout_secs = timeout_secs
        return self
    
    def timeline(self, time_step: TimeStep):
        self.event_timeline.append(time_step)
        return self

    def logs_data_equal(
        self,
        filter: LogFilter,
        other: List[Log] | List[any],
    ):
        res = []
        if self.recorder is None or self.handle is None:
            raise self.error("No workflow has been run prior the call")
        
        if filter.name == LogFilter.runs:
            res = self.recorder.get_recorded_runs(self.handle)
        else:
            res = self.recorder.get_recorded_events(self.handle)
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
        # idea: run a workflow multiple times, keep the recorded runs
        # then compare
        raise Exception("TODO")

    async def exec_workflow(self, workflow: Workflow):
        substantial = SubstantialMemoryConductor()
        substantial.register(workflow)
        backend_exec = asyncio.create_task(substantial.run())

        workflow_run = workflow(0, "<default>")
        handle = workflow_run.handle

        async def go():
            await substantial.start(workflow_run)
            for ev in self.event_timeline:
                await asyncio.sleep(ev.delta_time)
                await substantial.send(handle, ev.event_name, ev.payload)
            await backend_exec

        try:
            await asyncio.wait_for(go(), self.timeout_secs)
        except TimeoutError:
            pass
        except:
            raise

        self.recorder = substantial.runs
        self.handle = handle 

        return self

    # async def replay(self, count: int):
    #     pass
    # Not sure about the feasability since 
    # replay is hidden inside the backend's workflow queue and is is not reachable from outside
    # though it can be simulated with events


def make_sync(fn: any) -> any:
    # Naive impl may run it from a running event loop
    # return asyncio.run(fn())
    def syncified():
        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            ret = runner.run(fn())
        return ret
    return syncified
