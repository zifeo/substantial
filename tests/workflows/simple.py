from substantial.workflow import workflow, Context

@workflow(1, "test-simple")
async def simple_workflow(c: Context, name, n):
    r1 = await c.save(lambda: step_1("A"))
    r2 = await c.save(lambda: step_2(r1))
    r3 = await c.save(lambda: step_3(r2))
    return r3

async def step_1(v):
    return v

async def step_2(v):
    return f"B {v}"


async def step_3(v):
    return f"C {v}"
