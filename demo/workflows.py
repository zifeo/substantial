import asyncio
from dataclasses import dataclass
import random
from substantial.types import RetryStrategy
from substantial.workflow import workflow, Context

retry_strategy = RetryStrategy(
    max_retries=3,
    initial_backoff_interval=1,
    max_backoff_interval=10
)

# Workflow: orchestrate activities deterministically
@workflow(1, "simple")
async def example_workflow(c: Context, name, n):
    r1 = await c.save(lambda: step_1())
    print(r1)

    r2 = await c.save(
        lambda: step_2(r1),
        timeout=1,
        retry_strategy=retry_strategy
    )
    print(r2)

    await c.sleep(1)

    r3 = await c.save(lambda: step_3(r2))
    print(r3)

    n = await c.event("do_print")

    s = State(is_cancelled=False)

    c.register("cancel", s.update)

    await c.wait(lambda: s.is_cancelled)

    if s.is_cancelled:
        r4 = await c.save(lambda: step_4(r3, n))

    return r4



@dataclass
class State:
    is_cancelled: bool

    def update(self):
        self.is_cancelled = True

# Activities: fn + c.save(..), should be idempotent
async def step_1():
    return "A"


async def step_2(b):
    # if random.random() > (1 / 3):
    #     await asyncio.sleep(1 + random.random() * 4)
    return f"B {b}"


async def step_3(b):
    return f"C {b}"

async def step_4(b, a):
    return f"{a} D {b}"
