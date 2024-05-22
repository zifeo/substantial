# Equiv. server + worker
import asyncio
import logging
import random
import time
from substantial.conductor import SubstantialMemoryConductor
import uvloop

from substantial.task_queue import MultiTaskQueue
from workflows import example_workflow

async def example_worker():
    substantial = SubstantialMemoryConductor()
    substantial.register(example_workflow)

    execution = asyncio.create_task(substantial.run())

    workflow_run = example_workflow("test", 1)

    handle = await substantial.start(workflow_run)

    await asyncio.sleep(3) # just pick a big enough delay (we have sleep(1) on the example workflow)

    print(await substantial.send(handle, "do_print", "'sent from app'"))

    await asyncio.sleep(5)
    print(await substantial.send(handle, "cancel"))

    await execution


with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
    # async def parallel():
    #     async with MultiTaskQueue(1) as send:
    #         await asyncio.gather(*[
    #             send(example_worker()),
    #             send(example_worker())
    #         ])

    # runner.run(parallel())
    runner.run(example_worker())
