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
    await c.sleep(1)

    n = await c.event("say_hello")

    # Register the callback under the name 'UpDaTE' to be called once say_hello event occurs
    s = State(is_cancelled=False)
    c.register("UpDaTE", s.update) 

    await c.wait(lambda: s.is_cancelled)

    if s.is_cancelled:
        r2 = await c.save(lambda: step_2(r1, n))

    return r2


@dataclass
class State:
    is_cancelled: bool
    def update(self):
        self.is_cancelled = True


async def step_1():
    return "A"

async def step_2(b, a):
    return f"{a} B {b}"
