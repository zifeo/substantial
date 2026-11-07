"""The workflow of I-3 on a file store, to try durability by hand:

    uv run python/examples/first_workflow.py /tmp/substantial-demo-py

Stop it with Ctrl-C during the sleep and run it again: the run resumes, and the steps it had
recorded do not print again.
"""

import asyncio
import random
import sys

import substantial as s


@s.step(retry=s.Retry(max_attempts=6, initial=s.seconds(1), max=s.minutes(1), multiplier=2))
def roulette() -> int:
    print("step roulette")
    if random.random() < 1 / 6:
        raise Exception("shot!")
    return 100


@s.step
def multiply(a: float, b: float) -> float:
    print(f"step multiply({a}, {b})")
    return a * b


@s.workflow
async def example(c: s.Context) -> float:
    res = await roulette()
    print("at the sleep (code outside steps runs again on every replay)")
    await c.sleep(s.seconds(10))
    res = await multiply(res, 0.5)

    n = await c.receive("by")
    res = await multiply(res, n)

    stopped: list[None] = []
    c.handle("stop", stopped.append)
    await c.wait_until(lambda: bool(stopped))
    return res


async def main(directory: str) -> None:
    async with (
        s.Substantial.create(f"file:{directory}", partitions=1) as sub,
        sub.worker([example], preset="script"),
    ):
        # The id makes the start idempotent: running the script again finds the same run. A send
        # to a run that ended raises RunEnded, so the messages go only to a run still going.
        run = await sub.start(example, id="example-1")
        if await run.status() not in (
            s.RunStatus.COMPLETED,
            s.RunStatus.FAILED,
            s.RunStatus.CANCELLED,
        ):
            await run.send("by", 2, id="by-1")
            await run.send("stop", id="stop-1")
        print("Final output", await run.result(timeout=s.minutes(2)))  # 100
        print("status", await run.status())


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: first_workflow.py <directory>")
    asyncio.run(main(sys.argv[1]))
