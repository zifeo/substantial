# Equiv. server + worker
import asyncio
from typing import Tuple, Union
from substantial.conductor import Recorder, SubstantialMemoryConductor
import uvloop

from substantial.workflow import Workflow
from tests.basic.workflows.event.workflow import example_workflow

async def execute_workflow(
    workflow: Workflow,
    timeout: int = 120
) -> Tuple[str, Recorder]:
    substantial = SubstantialMemoryConductor()
    substantial.register(workflow)
    backend_exec = asyncio.create_task(substantial.run())

    workflow_run = workflow("test", 1)
    handle = workflow_run.handle

    async def go(): 
        await substantial.start(workflow_run),
        await backend_exec
    try:
        await asyncio.wait_for(go(), timeout)
    except TimeoutError:
        # await backend_exec.cancel("Terminated backend execution")
        return handle, substantial.runs
    except:
        raise
    return handle, substantial.runs

