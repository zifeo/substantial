from substantial.types import State
from substantial.workflow import workflow, Context

# Workflow: orchestrate activities deterministically
@workflow(1)
async def example_workflow(c: Context, name, n):
    r1 = await c.save(lambda: step_1())
    print(r1)

    r2 = await c.save(lambda: step_2(r1))
    print(r2)

    r3 = await c.save(lambda: step_3(r2))
    print(r3)

    n = await c.event("n")

    s = State(is_cancelled=False)

    c.register("cancel", s.cancel)

    await c.wait(lambda: s.is_cancelled)

    if s.is_cancelled:
        r4 = await c.save(lambda: step_4(r3, n))

    return r4


# Activities: fn + c.save(..), should be indenpotent
async def step_1():
    return 1


async def step_2(n):
    return n + 2


async def step_3(n):
    return n * 2


async def step_4(n, d):
    return n / d
