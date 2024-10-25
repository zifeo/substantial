from dataclasses import dataclass
from datetime import timedelta
import random
from substantial.types import RetryStrategy
from substantial import workflow, Context


@workflow()
async def example_simple(c: Context):
    retry_strategy = RetryStrategy(
        max_retries=3, initial_backoff_interval=1, max_backoff_interval=10
    )

    r1 = await c.save(step_1)

    r2 = await c.save(
        lambda: step_2(r1), timeout=timedelta(seconds=1), retry_strategy=retry_strategy
    )

    await c.sleep(timedelta(seconds=10))

    r3 = await c.save(lambda: step_3(r2))

    n = await c.receive("do_print")
    # print("Outside sent", n)

    s = State(is_cancelled=False)
    c.handle("cancel", lambda _: s.update())

    if await c.ensure(lambda: s.is_cancelled):
        r4 = await c.save(lambda: step_4(r3, n))
        print("s.is_cancelled updated, should stop now..")

    return r4


@workflow()
async def example_retry(c: Context):
    a = await c.save(
        lambda: "A",
        retry_strategy=RetryStrategy(
            max_retries=10, initial_backoff_interval=1, max_backoff_interval=10
        ),
    )

    await c.sleep(timedelta(seconds=10))
    await c.save(lambda: a + " B")

    r1 = await c.save(
        step_failing,
        retry_strategy=RetryStrategy(
            max_retries=10, initial_backoff_interval=1, max_backoff_interval=4
        ),
    )
    return r1


@dataclass
class State:
    is_cancelled: bool

    def update(self):
        print("update..")
        self.is_cancelled = True


async def step_1():
    return "A"


async def step_2(b):
    return f"B {b}"


async def step_3(b):
    return f"C {b}"


async def step_4(b, a):
    return f"{a} D {b}"


async def step_failing():
    if random.random() > 0.5:
        raise Exception("random failure")
    return "RESOLVED => SHOULD STOP"
