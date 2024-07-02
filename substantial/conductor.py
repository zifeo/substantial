import asyncio
from uuid import uuid4

from substantial.agent import Agent
from substantial.backends.backend import Backend
from substantial.workflows import Store
from substantial.workflows.run import Run
from substantial.workflows.workflow import Workflow
import aioprocessing


class Conductor:
    def __init__(self, backend: Backend):
        self.backend = backend

    def register(self, workflow: Workflow):
        Store.register(workflow)

    async def start(
        self,
        workflow: Workflow,
        kwargs=None,
        queue="default",
    ):
        if kwargs is None:
            kwargs = {}

        run_id = f"{workflow.id}-{uuid4()}"
        run = Run(run_id, queue, self.backend)
        await run.start(kwargs)
        return run

    def run_for(self, run_id, queue="default"):
        return Run(run_id, queue, self.backend)

    async def run(self, queue="default"):
        return asyncio.create_task(self.run_as_process(queue))

    async def run_as_process(self, queue):
        agent = Agent(self.backend, queue)
        p = aioprocessing.AioProcess(target=agent.run_sync)
        try:
            p.start()
            await p.coro_join()
            return p
        except asyncio.CancelledError:
            print("Cancelled")
        except Exception:
            raise
