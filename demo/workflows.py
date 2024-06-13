from dataclasses import dataclass
from datetime import timedelta
import random
from substantial.types import RetryStrategy
from substantial.workflow import workflow, Context

retry_strategy = RetryStrategy(
    max_retries=3,
    initial_backoff_interval=1,
    max_backoff_interval=10
)

# @workflow(name="continue-example", restore_using="example")
@workflow()
async def example_simple(c: Context, name):
    r1 = await c.save(lambda: step_1())
    print(r1)

    r2 = await c.save(
        lambda: step_2(r1),
        timeout=timedelta(seconds=1),
        retry_strategy=retry_strategy
    )
    print(r2)

    await c.sleep(timedelta(seconds=2))

    r3 = await c.save(lambda: step_3(r2))
    print(r3)

    n = await c.event("do_print")
    s = State(is_cancelled=False)

    c.register("cancel", s.update)

    if await c.wait_on(lambda: s.is_cancelled):
        r4 = await c.save(lambda: step_4(r3, n))

    return r4

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

    b = await c.save(lambda: "B") 

    r1 = await c.save(
        lambda: failing_op(), # 70% chance not failling
        retry_strategy=RetryStrategy(
            max_retries=10,
            initial_backoff_interval=1,
            max_backoff_interval=4
        )
    )
    return r1


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

def failing_op():
    if random.random() > 0.7:
        raise Exception("random failure")
    return "RESOLVED => SHOULD STOP"
