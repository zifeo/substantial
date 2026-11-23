# Messages

A message is a name and a JSON value sent to a chain, by a client or by a workflow (I-9). Messages carry webhooks, approvals, replies to an actor, the ends of awaited chains and a child's cancellation. Users build idempotent routes and actors on three properties: a resend is delivered once, a workflow's send survives its run's crash without doubling, and messages from one sender arrive in the order sent ([Integration](../../integration.md#what-start-and-send-guarantee)).

A sender retries after a lost answer, so a message is deduplicated by an id the sender picks ([Integration](../../integration.md#what-start-and-send-guarantee)), and a run replays its code, so a workflow's send is recorded and never sent again at a replay (I-9). There is no broker ([Architecture](../../architecture.md#how-it-works)): messages are ordered by the sender's clock, strictly within one process by the host's ULIDs (I-17).

## Delivery

**§ <a id="vcgwgaf4n">vcgwgaf4n</a> ~~A message is an inbox key of the chain's last run, `runs/<run>/inbox/<ord>-<id>`, and a replay consumes every message waiting there, in key order, whether the code asks for it yet or not, and records it, so on every replay the code sees the same messages at the same place.~~**

A message sent while its run ends may be accepted and left unconsumed ([Architecture](../../architecture.md#when-something-goes-wrong), "a client send races the run's end").

- (a) ~~A message that reaches a run as it continues as new is forwarded to the next run of the chain, none lost, and may be consumed there after a message sent later.~~

A message is its own wakeup: its entry sits in the queue of the chain's workflow, holding its body, so a send is one create, and a worker that lists the queue finds it. Its key is the sender's time first, so a chain's messages sort in send order, and its entry is due at the epoch, so no sender's clock delays it. A message a run was handed and did not consume moves into the chain's head, so an entry wakes its chain once.

**§ <a id="dnxwdv0ac">dnxwdv0ac</a> A message is an entry `<ord>-<id>` of its chain in the queue of its current run's workflow, holding its body, and a replay consumes every message handed to it, in key order, whether the code asks for it yet or not, and records it, so every replay shows the code the same messages at the same place.**

- (a) A message a run did not consume is kept in its chain's head and goes on to the chain's next run, none lost, and may be consumed there after a message sent later.

A message sent to a chain its retention removed raises `RunNotFound`, and while the chain is kept its id starts no new run, so no message meant for a run reaches another under the same id (I-16).

**§ <a id="dva6cx6nb">dva6cx6nb</a> A `send` or a `cancel` to a chain whose last run ended raises `RunEnded`, and to a chain that does not exist `RunNotFound`, from a client as from a workflow.**

- (a) ~~A client's `send` or `cancel` returns once the message's inbox key and the run's due entry exist in the store.~~

- (c) A client's `send` or `cancel` returns once the message's entry, which is also its chain's wakeup, exists in the store.

- (b) A workflow's send the core refused leaves the store as it was.

**§ <a id="r1nrgn5h8">r1nrgn5h8</a> A chain consumes a message id once within the dedup window from its consumption, across continuations, so a resend with the same id is delivered once.**

- (a) A run's history keeps every id it consumed, and the next run of a continued chain carries only the ids within the window, so a resend past the window is delivered again only once the run has continued as new since.

**§ <a id="b7g8bsqaq">b7g8bsqaq</a> A workflow's send is delivered once, its run crashing after it or not, while workers serve due runs within the dedup window, and its outcome is recorded, so every replay gives the code the same one.**

- (a) Before its commit the replay reads the chain up to its last run, with no write, and records `RunNotFound`, `RunEnded` or the run the message goes to.

- (b) A child the run started is accepted before its start exists.

- (c) A workflow's send without an id gets an id the core derives from the call, the same on every replay.

## Handlers and names

**§ <a id="n4zyj94mr">n4zyj94mr</a> A bind of several names returns their past messages in the order they arrived, across names.**

**§ <a id="ezxksxayf">ezxksxayf</a> Binding or receiving a message name that starts with `substantial.` raises `InvalidArgument`.**

The ends a run awaits with `c.result` come as such messages, which no handler sees (I-9).

## Order

**§ <a id="a1qvxtaj6">a1qvxtaj6</a> A run consumes its messages in the order of their `ord`, the sender's ULID, so in the order of the sender's clock, then in the order one sender drew them.**

- (a) A run's own sends draw their order in call order.

Across processes within one millisecond the order is arbitrary, and a message forwarded at a continuation may be consumed after a later one (I-9).
