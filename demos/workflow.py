from substantial import Context

async def example(c: Context):
    a = await c.save(lambda: plus_one(1))
    b = c.func("pow", {"a":1, "b": 2})
    c = c.receive("thing")
    return f"{a} :: {b} :: {c}"

async def plus_one(x):
    import asyncio
    await asyncio.sleep(1)
    return x + 1
