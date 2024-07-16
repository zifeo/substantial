import asyncio
from substantial import Conductor
from substantial.backends.fs import FSBackend

from workflows import example_simple


async def example():
    backend = FSBackend("./logs")
    substantial = Conductor(backend)
    substantial.register(example_simple)

    agent = await substantial.run()

    w = await substantial.start(
        example_simple,
    )

    await asyncio.sleep(3)
    print("Sending...")
    print(await w.send("do_print", "'sent from app'"))

    await asyncio.sleep(5)
    print("Cancelling...")
    print(await w.send("cancel"))

    output = await w.result()
    print("Final output", output)

    # should be put on schedule, but ignored (don't trigger `Stop` flagged run)
    await asyncio.sleep(0.5)
    print(await w.send("after stop", "one"))

    agent.cancel()
    await agent


asyncio.run(example())
