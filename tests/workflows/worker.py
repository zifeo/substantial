# Equiv. server + worker
import asyncio
from substantial.conductor import SubstantialMemoryConductor
import uvloop

from tests.workflows.simple import simple_workflow

async def example_worker():
    substantial = SubstantialMemoryConductor()
    substantial.register(simple_workflow)

    execution = asyncio.create_task(substantial.run())

    workflow_run = simple_workflow("test", 1)
    handle = await substantial.start(workflow_run)
    print(f"Ran {handle}")

    await execution

with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
    sec = 3
    try:
        runner.run(asyncio.wait_for(example_worker(), sec))
    except TimeoutError:
        pass
    except Exception:
        raise
