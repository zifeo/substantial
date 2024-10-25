# Substantial

[![PyPI version](https://badge.fury.io/py/substantial.svg)](https://badge.fury.io/py/substantial)

Brokerless durable execution for Python.

> Substantial is part of the
> [Metatype ecosystem](https://github.com/metatypedev/metatype). Consider
> checking out how this component integrates with the whole ecosystem and browse
> the
> [documentation](https://metatype.dev?utm_source=github&utm_medium=readme&utm_campaign=substantial)
> to see more examples.

## What is durable execution?

Durable execution is a programming model where the state of a function is preserved across failures, restarts, or other (un)voluntary disruptions. This ensures that applications can continue execution from their last stable state without losing context or causing additional side effects. It is particularly well-suited for long-running workflows as it enables the management of complex sequences of steps, handling breaks, retries and recovery gracefully.

Substantial is designed around a replay mechanism that reconstructs the function state by re-executing historical events from stored logs. All the logic is embedded in a protocol and there is no centralized broker, allowing it to work with any backend (local files, cloud storage and databases). It aims to be an alternative for use cases that do not require the scale and complexity of [Temporal](https://github.com/temporalio/temporal) or [Durable Task](https://github.com/Azure/durabletask).

## Getting started

```
# pypi
pip install substantial
poetry add substantial

# remote master
pip install --upgrade git+https://github.com/zifeo/substantial.git
poetry add git+https://github.com/zifeo/substantial.git

# local repo/dev
poetry install
pre-commit install
protoc -I . --python_betterproto_out=. protocol/*
```

### Workflow

```py
def roulette():
    if random.random() < 1/6:
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

    s = State(is_cancelled=False)
    c.handle("cancel", lambda: s.update())

    await c.ensure(lambda: s.cancelled):
    return res
```

### Worker

Brokerless means that there is no active broker and all the scheduling is solely organized around files/key values. Substantial currently supports Redis, local files and s3-compatible object storages.

```py
backend = FSBackend("./logs")
substantial = Conductor(backend)
substantial.register(example)

# run the agent in the background
agent = substantial.run()

# start the workflow
w = await substantial.start(example)

await asyncio.sleep(3)
print(await w.send("by", 2))

await asyncio.sleep(5)
print(await w.send("cancel"))

output = await w.result()
print("Final output", output) # 100

# stop the agent
agent.cancel()
await agent
```

## API

_Note_: not all features are implemented/completed yet.

### Primitives

`save(f: Callable, compensate_with: Optional[Callable]): Any` - memoize the result of a function to avoid re-execution on replay. Functions shall be idempotent as they may be called more than once in case of failure just before the value is persisted. The function can be compensated by providing its inverse effect and trigger later in the workflow with `revert`.

`handle(event_name: str, cb: Callable): None` - register a callback to be executed when a specific event is received. The callbacks are executed in the order they were received and whenever a primitive being called.

`ensure(f: Callable): True` - wait for the function to evaluate to true and schedule a new run when false.

### Higher-level

`sleep` - schedule a replay after a certain amount of time.

`receive` - wait for the value of an event to be received.

`log` - TODO

`datetime.now` - TODO

`random` - TODO

`uuid4` - TODO

### Advanced

`revert()` - execute the compensations and stop the workflow.

`continue_using(workflow, *args, **kwargs)` - stop the current workflow and pass the context to a new one.

`compact()` - a key can be defined on all the primitives to avoid the infinitely growing log issue. This function will keep only the last occurrence of each of the keys.
