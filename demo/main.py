# Equiv. server + worker
import asyncio
from substantial.conductor import SubstantialMemoryConductor

from workflows import example_workflow

async def same_thread_example():
    substantial = SubstantialMemoryConductor()
    substantial.register(example_workflow)

    execution = asyncio.create_task(substantial.run())

    workflow_run = example_workflow()

    handle = await substantial.start(workflow_run)

    await asyncio.sleep(3) # just pick a big enough delay (we have sleep(1) on the example workflow)

    print("Sending...")
    print(await substantial.send(handle, "do_print", "'sent from app'"))

    await asyncio.sleep(5)
    print("Cancelling...")
    print(await substantial.send(handle, "cancel"))

    await execution


asyncio.run(same_thread_example())
