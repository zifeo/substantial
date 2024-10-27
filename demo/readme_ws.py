from dataclasses import dataclass
from datetime import timedelta
import random
from substantial import workflow, Context


def roulette():
    if random.random() < 1 / 6:
        raise Exception("shot!")
    return 100


def multiply(a, b):
    return a * b


@dataclass
class State:
    cancelled: bool = False

    def update(self):
        self.cancelled = True


@workflow()
async def example(c: Context):
    res = await c.save(roulette)

    await c.sleep(timedelta(seconds=10))
    res = await c.save(lambda: multiply(res, 0.5))

    n = await c.receive("by")
    res = await c.save(lambda: multiply(res, n))

    s = State(cancelled=False)
    c.handle("cancel", lambda _: s.update())

    await c.ensure(lambda: s.cancelled)
    return res
