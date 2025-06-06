import asyncio
from substantial import Conductor

from substantial.backends.redis import RedisBackend

# from substantial.backends.fs import FSBackend
from demo.main_ws import example_simple
from substantial.filters import Err


async def example():
    # backend = FSBackend("./logs")
    backend = RedisBackend(host="localhost", port=6380, password="password")
    substantial = Conductor(backend)
    substantial.register(example_simple)

    agent = substantial.run()

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

    # filter runs feature overview
    results = [
        s.result
        for s in await substantial.filters.search(
            example_simple,
            {
                "or": [
                    {"in": Err("fatal")},
                    {"and": [{"contains": "B A"}, {"not": {"not": {"contains": "D"}}}]},
                ]
            },
        )
    ]

    print(len(results), results)


asyncio.run(example())
