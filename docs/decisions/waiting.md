# Waiting

A run waits without holding a process: on a sleep, a message, or a predicate over its state (I-8). A wait is resumed by a later replay, maybe on another worker with another clock, so the instants a wait depends on are fixed the first time its call runs: "a sleep's end is fixed when it first runs, and its firing is recorded, so a replay never sleeps again and a worker with a slow clock replays it the same" (I-8). The core fixes those instants and resolves a wait whose time passed; the host evaluates the predicates, builds `receive` from them, and checks durations.

## Sleeping and predicates

A `wait_until`'s timeout is fixed when the call first runs, as a sleep's end, so "a loop that waits again after each message gets a new timeout from each call, which makes a debounce" (I-8; [Integration](../integration.md#a-durable-debounce)). Passed first, it makes the call return false (I-8). When the host evaluates the predicates is stated in [k3zma9hjs](hosts.md#k3zma9hjs).

`wait_until` returns true once its predicate holds, and the host evaluates the predicates after each message, child end and timer ([k3zma9hjs.a](hosts.md#k3zma9hjs)).

## Receiving

A `receive` takes one message out of the stream of a name, where a handler sees all of them (I-9). A message's value may itself be `None`, "so no value stands for none": a timeout raises (I-8). A timed and an untimed `receive` of one name pending at once are refused, as "which one a message goes to would depend on the order of the code's branches" (I-8).

**§ <a id="p6tzbmjaq">p6tzbmjaq</a> `receive(name)` returns the next message of its name that no `receive` took yet, and with a timeout raises `WaitTimeout` once the timeout passed with none.**

- (a) A `receive` with a timeout and one without, of the same name, pending at the same time in one run raise `InvalidArgument`.

A `receive` with a timeout is a `wait_until` on the messages of its name, so its timeout is fixed when it first runs, and once `WaitTimeout` is caught the code may wait again (I-8).

## Durations

A deadline is passed as `deadline - await c.now()`, once checked not negative (I-8). Where each host refuses a negative duration is in the reference (I-8).
