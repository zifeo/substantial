import asyncio
from substantial import SubstantialConductor, Handle

from .workflows import example_simple


async def same_thread_example():
    substantial = SubstantialConductor()
    substantial.register(example_simple)

    workflow_run = example_simple()

    handle = await substantial.start(workflow_run)

    workflow_output, _ = await asyncio.gather(
        substantial.run(),
        # FIXME
        event_timeline(handle),
    )

    print("Final output", workflow_output)


async def event_timeline(handle: Handle):
    # just pick a big enough delay (we have sleep(1) on the example workflow)
    await asyncio.sleep(3)
    print("Sending...")
    print(await handle.send("do_print", "'sent from app'"))

    await asyncio.sleep(5)
    print("Cancelling...")
    print(await handle.send("cancel"))


asyncio.run(same_thread_example())
