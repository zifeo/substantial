import asyncio
from dataclasses import dataclass
from enum import Enum
import time
from typing import Dict, List, Union

import pytest
import uvloop

from substantial.conductor import HandleSignaler, Recorder, SubstantialMemoryConductor
from substantial.types import Log
from substantial.workflow import Workflow

class LogFilter(str, Enum):
    Events = "events"
    Runs = "runs"

@dataclass
class EventSend:
    event_name: str
    payload: Union[any, None] = None

class StepError(Exception):
    def __init__(self, step: str, message) -> None:
        super().__init__(f"'{step}': {message}")

class WorkflowTest:
    timeout_secs: int
    name: str
    event_timeline: Dict[float, EventSend]
    handle: Union[str, None] = None

    def __init__(self) -> None:
        self.timeout_secs = []
        self.event_timeline = dict()

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
    
    def events(self, event_timeline: Dict[float, EventSend]):
        self.event_timeline = event_timeline
        return self

    def get_logs(self, filter: LogFilter):
        if filter == LogFilter.Runs:
            return Recorder.get_recorded_runs(self.handle)
        elif filter == LogFilter.Events:
            return Recorder.get_recorded_events(self.handle)
        else:
            raise Exception(f"Unsupported filter {filter}")

    def logs_data_equal(
        self,
        filter: LogFilter,
        other: List[Log] | List[any],
    ):
        res = []
        if self.handle is None:
            raise self.error("No workflow has been run prior the call")

        res = self.get_logs(filter)

        if len(res) == 0 and len(other) == 0:
            return self
        if len(other) == 0:
            raise Exception(f"Cannot compare empty logs to logs of size {len(res)}")
        if isinstance(res[0], Log):
            res = [log.data.payload for log in res]
        if isinstance(other[0], Log):
            other = [log.data.payload for log in other]
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

        workflow_run = workflow()
        handle = workflow_run.handle
        signaler = HandleSignaler(handle, substantial)

        async def go():
            await substantial.start(workflow_run)
            time_prev = 0
            # 0 ======== t1 ===== t2 ======== t3 ======== .. ===>
            #   <========>
            #     t1 - 0
            #              <=======>
            #                t2 -t1 
            # TODO: this should use Recorder directly and write into the file!
            for (t, event) in self.event_timeline.items():
                delta_time = t - time_prev
                time_prev = t
                await asyncio.sleep(delta_time)
                # TODO: reimplement with continuous `poll` and rely on `class Recorder(LogSource)` directly for communication
                await signaler.send(event.event_name, event.payload)
            await backend_exec

        try:
            await asyncio.wait_for(go(), self.timeout_secs)
        except TimeoutError:
            pass
        except:
            raise

        self.handle = handle 

        return self

    # async def replay(self, count: int):
    #     pass
    # Not sure about the feasability since 
    # replay is hidden inside the backend's workflow queue and is is not reachable from outside
    # though it can be simulated with events


def make_sync(fn: any) -> any:
    # Naive impl will run it in the ongoing event loop
    # return asyncio.run(fn())
    def syncified():
        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            ret = runner.run(fn())
        return ret
    return syncified


asyncio_fun = pytest.mark.asyncio(scope="function")
