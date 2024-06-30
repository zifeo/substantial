from dataclasses import dataclass
from datetime import timedelta
import random
from substantial.types import RetryStrategy
from substantial import workflow, Context


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


@workflow()
async def example_simple(c: Context):
    retry_strategy = RetryStrategy(
        max_retries=3, initial_backoff_interval=1, max_backoff_interval=10
    )

    r1 = await c.save(step_1)
    r2 = await c.save(
        lambda: step_2(r1), timeout=timedelta(seconds=1), retry_strategy=retry_strategy
    )

    await c.sleep(timedelta(seconds=2))

    r3 = await c.save(lambda: step_3(r2))

    n = await c.event("do_print")
    s = State(is_cancelled=False)

    c.register("cancel", s.update)

    if await c.wait_on(lambda: s.is_cancelled):
        r4 = await c.save(lambda: step_4(r3, n))

    return r4


@dataclass
class State:
    is_cancelled: bool

    def update(self):
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
    if random.random() > 0.2:
        raise Exception("random failure")
    return "RESOLVED => SHOULD STOP"
