from dataclasses import dataclass
from substantial.workflow import workflow, Context

@workflow(1, "events")
async def event_workflow(c: Context, name, n):
    r1 = await c.save(lambda: step_1("A"))

    payload = await c.event("sayHello")

    s = State(False)
    c.register("cancel", s.update)

    await c.wait(lambda: s.is_cancelled)
    if s.is_cancelled:
        r3 = await c.save(lambda: step_2(r1, payload))

    return r3


@dataclass
class State:
    is_cancelled: bool
    def update(self, *args):
        self.is_cancelled = True

async def step_1(v):
    return v

async def step_2(v, p):
    return f"{p} B {v}"
