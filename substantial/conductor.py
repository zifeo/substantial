import asyncio
import os
from typing import Dict, List

from substantial.types import Empty, Event, Interrupt, Log, LogKind
from substantial.workflow import WorkflowRun, Workflow

class Recorder:
    logs: Dict[str, List[Log]] = dict()
    events: Dict[str, List[Log]] = dict()

    def record(self, handle: str, log: Log):
        action_kinds = [LogKind.Save]
        event_kinds = [LogKind.EventIn, LogKind.EventIn]
        
        if log.kind in action_kinds:
            if handle not in self.logs:
                self.logs[handle] = []
            self.logs[handle].append(log)
        elif log.kind in event_kinds:
            if handle not in self.events:
                self.events[handle] = []
            self.events[handle].append(log)

        if log.kind in (action_kinds + event_kinds):
            self.persist(handle, log)

        print(f"{log.kind} received but not persisted")

    def get_recorded_runs(self, handle: str) -> List[Log]:
        if handle not in self.logs:
            self.logs[handle] = []
        return self.logs[handle]

    def get_recorded_events(self, handle: str) -> List[Log]:
        if handle not in self.events:
            self.events[handle] = []
        return self.events[handle]

    def persist(self, handle: str, log: Log):
        with open(f"logs/{handle}", "a") as file:
            file.write(f"{log.to_json()}\n")

    def recover_from_file(self, filename: str, handle: str):
        if os.path.exists(filename):
            if len(self.logs) > 0:
                raise Exception("Invalid state: cannot recover from non empty logs")

            force_override = True
            self.logs = dict()

            with open(filename, "r") as file:
                count = 0
                print(f"[!] Loading logs from {filename} for {handle}")
                while line := file.readline():
                    log = Log.from_json(line.rstrip())
                    if not force_override and log.handle != handle:
                        raise Exception(f"Workflow id is not the same as the one from '{filename}':\n\t'{log.handle}' != '{handle}'")
                    else:
                        log.handle = handle
                    self.record(handle, log)
                    count += 1
                print(f"Read {count} lines")


class SubstantialMemoryConductor:
    def __init__(self):
        # num workers
        self.known_workflows = {}
        # mulithreaded safe queues
        self.events = asyncio.Queue()
        self.workflows = asyncio.Queue()
        self.runs = Recorder()

    def register(self, workflow: Workflow):
        self.known_workflows[workflow.id] = workflow

    async def start(self, workflow_run: WorkflowRun):
        """ Put `workflow_run` into the workflow queue and return the `handle` """
        await self.workflows.put(workflow_run)
        return workflow_run.handle

    async def send(self, handle: str, event_name: str, *args):
        ret = asyncio.Future()
        self.runs.record(handle, Log(handle, LogKind.EventIn, (event_name, args)))
        await self.events.put(Event(handle, event_name, args, ret))
        return ret

    def get_run_logs(self, handle: str):
        return self.runs.get_recorded_runs(handle)

    def get_event_logs(self, handle: str):
        return self.runs.get_recorded_events(handle)

    def log(self, log: Log):
        dt = log.at.strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"{dt} [{log.handle}] {log.kind} {log.data}")
        self.runs.record(log.handle, log)

    async def schedule_later(self, queue, task, secs):
        await asyncio.sleep(secs)
        await queue.put(task)

    async def run(self):
        # TODO: use task_queue?
        e = asyncio.create_task(self.run_events())
        w = asyncio.create_task(self.run_workflows())
        return await asyncio.gather(e, w)

    async def run_events(self):
        while True:
            event = await self.events.get()
            logs = self.get_event_logs(event.handle)

            res = Empty
            for log in logs[::-1]:
                if log.kind == LogKind.EventOut and log.data[0] == event.name:
                    res = log.data[1]
                    break

            if res is Empty:
                asyncio.create_task(self.schedule_later(self.events, event, 2))
            else:
                event.future.set_result(event.data)
                event.future.done()

            self.events.task_done()

    async def run_workflows(self):
        while True:
            workflow_run = await self.workflows.get()
            try:
                await workflow_run.replay(self)
            except Interrupt as interrupt:
                print(f"Interrupted {interrupt.hint}")
                asyncio.create_task(
                    self.schedule_later(self.workflows, workflow_run, 3)
                )
            except Exception as e:
                print(f"Workflow error: {e}")
                # retry
            self.workflows.task_done()