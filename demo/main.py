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

    await asyncio.sleep(1)

    print(await substantial.send(handle, "do_print", "'sent from app'"))

    await asyncio.sleep(6)

    print(await substantial.send(handle, "cancel"))

    await execution


with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
    def f():
        print(time.ctime())
        time.sleep(random.random())
        return "Hello"
    async def run():
        async with MultiTaskQueue(2) as send:
            logging.info("Sending")

            await asyncio.gather(*[send(f) for _ in range(20)])

            logging.info("Sent")
    runner.run(run())
    # runner.run(example_worker())
