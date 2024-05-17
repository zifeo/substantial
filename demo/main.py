# Equiv. server + worker
import asyncio
from substantial.conductor import SubstantialMemoryConductor
import uvloop

from workflows import example_workflow

async def example_worker():
    substantial = SubstantialMemoryConductor()
    substantial.register(example_workflow)

    execution = asyncio.create_task(substantial.run())

    workflow_run = example_workflow("test", 1)

    handle = await substantial.start(workflow_run)

    await asyncio.sleep(1)

    print(await substantial.send(handle, "do_print", "'sent from app'"))

    await asyncio.sleep(6)

    print(await substantial.send(handle, "cancel"))

    await execution


with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
    runner.run(example_worker())
