# Equiv. server + worker
import asyncio
from substantial.conductor import SubstantialConductor, HandleSignaler

from workflows import example_retry

w = example_retry

async def same_thread_example():
    substantial = SubstantialConductor()
    substantial.register(w)

    # Note: `create_task` makes sure substantial.run executes async
    # all await calls that happens after will run in parallel to this task
    execution = asyncio.create_task(substantial.run())

    workflow_run = w()

    handle = await substantial.start(workflow_run)

    await asyncio.sleep(3) # just pick a big enough delay (we have sleep(1) on the example workflow)

    signaler = HandleSignaler(handle, substantial)

    # print("Sending...")
    # print(await signaler.send("do_print", "'sent from app'"))

    # await asyncio.sleep(5)
    # print("Cancelling...")
    # print(await signaler.send("cancel"))

    await execution


asyncio.run(same_thread_example())
