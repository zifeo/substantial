async def main(c):
    a = await c.save(lambda: 1)
    b = await c.save(lambda: plus_one(a))
    return a + b

def plus_one(x):
    return x + 1

