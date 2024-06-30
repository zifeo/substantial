# Equiv. server + worker
import asyncio
from substantial.conductor import SubstantialConductor, EventEmitter

from .workflows import example_simple

w = example_simple


async def same_thread_example():
    substantial = SubstantialConductor()
    substantial.register(w)

    workflow_run = w()

    handle = await substantial.start(workflow_run)

    workflow_output, _ = await asyncio.gather(
        substantial.run(), event_timeline(EventEmitter(handle))
    )

    print("Final output", workflow_output)


async def event_timeline(emitter: EventEmitter):
    await asyncio.sleep(
        3
    )  # just pick a big enough delay (we have sleep(1) on the example workflow)
    print("Sending...")
    print(await emitter.send("do_print", "'sent from app'"))

    await asyncio.sleep(5)
    print("Cancelling...")
    print(await emitter.send("cancel"))


asyncio.run(same_thread_example())
