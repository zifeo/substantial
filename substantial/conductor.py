import asyncio

from substantial.types import Empty, Event, Interrupt, Log, LogKind


class SubstantialMemoryConductor:
    def __init__(self):
        # num workers
        self.known_workflows = {}
        # mulithreaded safe queues
        self.events = asyncio.Queue()
        self.workflows = asyncio.Queue()
        self.runs = dict()

    def register(self, workflow):
        self.known_workflows[workflow.id] = workflow

    async def start(self, workflow_run):
        """ Put `workflow_run` into the workflow queue and return the `handle` """
        await self.workflows.put(workflow_run)
        return workflow_run.handle

    async def send(self, handle, event_name, *args):
        ret = asyncio.Future()
        if handle not in self.runs:
            self.runs[handle] = []
        self.runs[handle].append(Log(handle, LogKind.EventIn, (event_name, args)))
        await self.events.put(Event(handle, event_name, args, ret))
        return ret

    def get_logs(self, handle):
        if handle not in self.runs:
            self.runs[handle] = []
        return self.runs[handle]

    def log(self, log: Log):
        dt = log.at.strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"{dt} [{log.handle}] {log.kind} {log.data}")
        self.runs[log.handle].append(log)

    async def schedule_later(self, queue, task, secs):
        await asyncio.sleep(secs)
        await queue.put(task)

    async def run(self):
        e = asyncio.create_task(self.run_events())
        w = asyncio.create_task(self.run_workflows())
        return await asyncio.gather(e, w)

    async def run_events(self):
        while True:
            event = await self.events.get()
            logs = self.get_logs(event.handle)

            res = Empty
            for log in logs[::-1]:
                if log.kind == LogKind.EventOut and log.data[0] == event.name:
                    res = log.data[1]
                    break

            if res is Empty:
                asyncio.create_task(self.schedule_later(self.events, event, 3))
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
