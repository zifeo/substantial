from dataclasses import dataclass
from datetime import timedelta
import random
from substantial.types import RetryStrategy
from substantial.workflow import workflow, Context

# @workflow(name="continue", restore_using="example_retry-40ef491e-2c08-4f99-9fb5-bedb82043747")
@workflow()
async def example_retry(c: Context, name):
    a = await c.save(
        lambda: "A",
        retry_strategy=RetryStrategy(
            max_retries=10,
            initial_backoff_interval=1,
            max_backoff_interval=10
        )
    )

    await c.sleep(timedelta(seconds=10))
    b = await c.save(lambda: a + " B") 

    r1 = await c.save(
        lambda: failing_op(),
        retry_strategy=RetryStrategy(
            max_retries=10,
            initial_backoff_interval=1,
            max_backoff_interval=4
        )
    )
    return r1

async def failing_op():
    if random.random() > 0.2:
        raise Exception("random failure")
    return "RESOLVED => SHOULD STOP"

retry_strategy = RetryStrategy(
    max_retries=3,
    initial_backoff_interval=1,
    max_backoff_interval=10
)

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
