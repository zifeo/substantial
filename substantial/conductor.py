import asyncio
from dataclasses import dataclass
from typing import List

from substantial.log_recorder import Recorder
from substantial.types import CancelWorkflow, Empty, Event, EventData, Interrupt, Log, LogKind, RetryMode
from substantial.workflow import WorkflowRun, Workflow

class Backend:
    def get_run_logs(self, handle: str) -> List[Log]:
        raise Exception("Not implemented")

    def get_event_logs(self, handle: str) -> List[Log]:
        raise Exception("Not implemented")
    
    def send(self, handle: str, event_name: str, *args):
        raise Exception("Not implemented")

@dataclass
class HandleSignaler:
    """ Simple wrapper for backend.send(handle, event, *args) """
    handle: str
    backend: Backend
    async def send(self, event_name, *args):
        return await self.backend.send(self.handle, event_name, *args)


class SubstantialMemoryConductor(Backend):
    def __init__(self):
        # num workers
        self.known_workflows = {}
        # mulithreaded safe queues
        self.events = asyncio.Queue()
        self.workflows = asyncio.Queue()

    def register(self, workflow: Workflow):
        self.known_workflows[workflow.id] = workflow

    async def start(self, workflow_run: WorkflowRun):
        """ Put `workflow_run` into the workflow queue and return the `handle` """
        await self.workflows.put(workflow_run)
        return workflow_run.handle

    async def send(self, handle: str, event_name: str, *args):
        ret = asyncio.Future()
        event_data = EventData(event_name, list(args))
        Recorder.record(handle, Log(handle, LogKind.EventIn, event_data))
        await self.events.put(Event(handle, event_name, event_data, ret))
        return ret

    def get_run_logs(self, handle: str):
        return Recorder.get_recorded_runs(handle)

    def get_event_logs(self, handle: str):
        return Recorder.get_recorded_events(handle)

    def log(self, log: Log):
        dt = log.at.strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"{dt} [{log._handle}] {log.kind} {log.data}")
        Recorder.record(log._handle, log)

    async def schedule_later(self, queue, task, secs):
        await asyncio.sleep(secs)
        await queue.put(task)

    async def run(self):
        # TODO: use task_queue?
        e = asyncio.create_task(self.run_events())
        w = asyncio.create_task(self.run_workflows())
        return await asyncio.gather(e, w)

    # async def run_as_task(self):
        # FIXME: x = backend.run_as_task() has the same effect as x = self.run()
        # vs a direct call at the place where it is used
        # return asyncio.create_task(self.run())

    async def run_events(self):
        """ Event loop """
        while True:
            # Note: if empty, this will block and wait otherwise process the latest
            event: Event = await self.events.get()
            event_logs = self.get_event_logs(event.handle)

            res = Empty
            for log in event_logs[::-1]:
                if log.kind == LogKind.EventOut:
                    data: EventData = log.data
                    if data.event_name == event.name:
                        res = data.args
                        break

            if res is Empty:
                asyncio.create_task(self.schedule_later(self.events, event, 2))
            else:
                event.future.set_result(event.data)
                event.future.done()

            self.events.task_done()

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
            except RetryMode as retry:
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