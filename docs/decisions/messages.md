# Messages and handlers

A message is a name and a JSON value sent to a chain, by a client or by a workflow ([Interfaces](../interfaces.md#i-9-messages), "A message is a name and a JSON value"). How a message reaches a run is the core's: a resend with the same id is delivered once within the dedup window ([r1nrgn5h8](core/messages.md#r1nrgn5h8)), a workflow's send survives its run's crash without doubling ([b7g8bsqaq](core/messages.md#b7g8bsqaq)), its id derived from the call when it gives none ([b7g8bsqaq.c](core/messages.md#b7g8bsqaq)), a replay consumes every message handed to it in order and records it ([dnxwdv0ac](core/messages.md#dnxwdv0ac)), and a run consumes them in the order of their senders' ULIDs ([a1qvxtaj6](core/messages.md#a1qvxtaj6)). The window is a fixed value of every worker ([ry26kkc7v](workers.md#ry26kkc7v)), and a client draws the ids and the order of its own sends ([Clients](clients.md)).

What each host decides is how code binds to messages and which names it refuses.

## Handlers

`receive` takes the next message of its name that no `receive` took yet, while "handlers see every message" ([Interfaces](../interfaces.md#i-9-messages)): an approval list or an actor's operations are a handler over the run's whole inbox, past and future. Names bound together get their past messages in arrival order across names ([n4zyj94mr](core/messages.md#n4zyj94mr)).

**§ <a id="aeby9bgqd">aeby9bgqd</a> `c.handle(name, cb)` first delivers every message of its name the run consumed so far, in order, then each new one as it is consumed.**

- (a) ~~A message a handler cannot decode fails the run with `MessageDecode`, and so does a handler that raises, in Rust that panics.~~

- (b) A handler is a plain function: in Python and TypeScript an async one raises `InvalidArgument` when it is bound.

- (c) ~~In Python and TypeScript a handler that returns an awaitable, a promise in TypeScript, fails the run with `MessageDecode`.~~

- (d) A message a handler cannot decode blocks the run with `MessageDecode`, and so does a handler that raises, in Rust that panics.

- (e) In Python and TypeScript a handler that returns an awaitable, a promise in TypeScript, blocks the run with `MessageDecode`.

## Reserved names

The ends a run awaits with `c.result` travel as messages whose names start with `substantial.`, "which no handler sees" ([Interfaces](../interfaces.md#i-9-messages)), and so does a cancel, the message `substantial.cancel` ([vmk2ydwta](core/lifecycle.md#vmk2ydwta)). Those names are reserved: sending one raises `InvalidArgument` "from a workflow as from a client" (I-9).

**§ <a id="j69rwh8x9">j69rwh8x9</a> A send of a message whose name starts with `substantial.`, a workflow's or a client's, raises `InvalidArgument`.**

The same prefix is refused in ids, workflow names and published keys ([px8dv58y7.b](clients.md#px8dv58y7)), and in step names ([px8dv58y7.c](clients.md#px8dv58y7)).
