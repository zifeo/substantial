import asyncio
from dataclasses import dataclass

from substantial.log_recorder import Recorder
from substantial.types import (
    CancelWorkflow,
    EventData,
    Interrupt,
    Log,
    LogKind,
    RetryMode,
)
from substantial.workflow import WorkflowRun, Workflow


class SubstantialConductor:
    def __init__(self):
        self.known_workflows = {}
        # mulithreaded safe queues
        self.workflows = asyncio.Queue()

    def register(self, workflow: Workflow):
        self.known_workflows[workflow.id] = workflow

    async def start(self, workflow_run: WorkflowRun):
        """
        Put `workflow_run` into the workflow queue and return the `handle`
        """
        await self.workflows.put(workflow_run)
        return workflow_run.handle

    def get_run_logs(self, handle: str):
        return Recorder.get_recorded_runs(handle)

    def get_event_logs(self, handle: str):
        return Recorder.get_recorded_events(handle)

    def log(self, log: Log):
        dt = log.at.strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"{dt} [{log._handle}] {log.kind} {log.data}")
        Recorder.record(log._handle, log)

    async def schedule_later(self, queue, task, secs):
        # FIXME
        await asyncio.sleep(secs)
        await queue.put(task)

    async def run(self):
        w = asyncio.create_task(self.run_workflows())
        # return w # FIXME: not working?
        return (await asyncio.gather(w))[0]

    async def run_workflows(self):
        while True:
            workflow_run: WorkflowRun = await self.workflows.get()
            try:
                ret = await workflow_run.replay(self)
                return ret
            except Interrupt as interrupt:
                print(f"Interrupted: {interrupt.hint}")
                await asyncio.create_task(
                    self.schedule_later(self.workflows, workflow_run, 3)
                )
            except RetryMode:
                print("Retry")
                await asyncio.create_task(
                    self.schedule_later(self.workflows, workflow_run, 1)
                )
            except CancelWorkflow as cancel:
                print(f"Cancelled workflow: {cancel.hint}")
            except Exception as e:
                print(f"Workflow error: {e}")
                # raise e
                # retry
            self.workflows.task_done()


@dataclass
class EventEmitter:
    handle: str

    async def send(self, event_name, *args):
        event_data = EventData(event_name, list(args))
        Recorder.record(self.handle, Log(self.handle, LogKind.EventIn, event_data))
