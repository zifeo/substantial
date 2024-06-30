# Substantial

Brokerless durable execution for Typescript and Python.

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
TODO
```

### Workflow

```
TODO
```

### Worker

```
TODO
```

## API

### Primitives

`save(f: Callable, compensate_with: Optional[Callable]): Any` - memoize the result of a function to avoid re-execution on replay. Functions shall be idempotent as they may be called more than once in case of failure before the value is persisted. The function can be compensated by providing its inverse effect and trigger later in the workflow with `revert`.

`handle(event_name: str, cb: Callable): None` - register a callback to be executed when a specific event is received. The callbacks are executed in the order they were received and whenever a primitive being called.

`ensure(f: Callable): True` - wait for the function to evaluate to true and schedule a new run when false.

### Higher-level

`log`

`now`

`random`

`uuid4`

`receive`

`sleep`

###

`revert()` - execute the compensations and stop the workflow.

`continue_using(workflow, *args, **kwargs)` - stop the current workflow and pass the context to a new one.

`compact()` - a key can be defined on all the primitives to avoid the infinitely growing log issue. This function will keep only the last occurrence of each of the keys.

## Roadmap

- leases
- higher-level abstractions
- ghjk
- protobuf log serialization
- s3 and redis backend
- child workflow
