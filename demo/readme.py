import asyncio
from substantial import Conductor
from substantial.backends.fs import FSBackend
from demo.readme_ws import example


async def main():
    backend = FSBackend("./demo/logs/readme")
    substantial = Conductor(backend)
    substantial.register(example)

    # run the agent in the background
    agent = substantial.run()

    # start the workflow
    w = await substantial.start(example)

    await asyncio.sleep(3)
    print(await w.send("by", 2))

    await asyncio.sleep(5)
    print(await w.send("cancel"))

    output = await w.result()
    print("Final output", output)  # 100

    # stop the agent
    agent.cancel()
    await agent


if __name__ == "__main__":
    asyncio.run(main())
