# Equiv. server + worker
import asyncio
from substantial.conductor import SubstantialMemoryConductor
import uvloop

from tests.sync.workflows.event.workflow import example_workflow

async def example_worker():
    substantial = SubstantialMemoryConductor()
    substantial.register(example_workflow)

    execution = asyncio.create_task(substantial.run())

    workflow_run = example_workflow("test", 1)

    handle = await substantial.start(workflow_run)

    print("Sending...")
    print(await substantial.send(handle, "say_hello", "Hello World"))

    await asyncio.sleep(3)
    print("Updating...")
    print(await substantial.send(handle, "UpDaTE"))

    await execution


with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
    sec = 6
    try:
        runner.run(asyncio.wait_for(example_worker(), sec))
    except TimeoutError:
        pass
    except Exception:
        raise

