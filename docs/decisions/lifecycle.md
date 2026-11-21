# A run's life

This note holds no law. It follows one run from its start to its collection, the order of [Architecture](../architecture.md#how-it-works) and of the reference's run machine ([Interfaces](../interfaces.md#i-23-the-core)), and names at each step the law that decides it: a host law, or a law of the core, in [core](core/).

## Start

1. A client or a parent names the run: the caller's `id`, else a ULID the host draws ([cq034wpjp](clients.md#cq034wpjp)), and a child's derived from its parent's ([yb61qtt3s](core/children.md#yb61qtt3s)). The name must pass the naming rules ([px8dv58y7](clients.md#px8dv58y7)).
2. The start creates `start`, then the due entry ([ct862686r](core/lifecycle.md#ct862686r)), from a declared workflow ([bxb0ygj7m](promises.md#bxb0ygj7m)).
3. A start through a `Substantial` wakes its own workers at once ([c46hgkfse](workers.md#c46hgkfse)); others find the run at their next scan ([e3rcyme0p](core/lifecycle.md#e3rcyme0p), [qsw5w98x3](core/lifecycle.md#qsw5w98x3)).

## Replay

4. A worker serving the run's name and version ([e3rcyme0p.d](core/lifecycle.md#e3rcyme0p), [pjrbw31h4](promises.md#pjrbw31h4)) claims it ([t452yredf](core/lifecycle.md#t452yredf)), within its `concurrency` ([rb377j7db](core/lifecycle.md#rb377j7db)) and the settings of its preset ([yxy5pzfkp](workers.md#yxy5pzfkp)).
5. It reads the history ([aqrdmj10z](core/cost.md#aqrdmj10z)) and calls the workflow function from its first line ([g4kv2kj51](promises.md#g4kv2kj51)). Recorded calls return their outcomes ([twravbm6m](core/history.md#twravbm6m)); the host runs the code one `Next` at a time ([k3zma9hjs](hosts.md#k3zma9hjs)).
6. A call the history does not answer fails the run with `NonDeterminism` ([twravbm6m](core/history.md#twravbm6m)), and the replay is dropped there ([v4n6ja42e](steps.md#v4n6ja42e)).
7. New steps run at least once ([ftq7bbgff](core/steps.md#ftq7bbgff)), with their retries ([a03w2yqkz](core/steps.md#a03w2yqkz)), and the host runs them as [Steps](steps.md) says.

## Commit and suspension

8. The replay commits what it recorded by an exclusive create of the next segment, and of two replays the first wins ([czkkbyhe0](core/commit.md#czkkbyhe0)), then creates its effects ([wymbfvjjr](core/commit.md#wymbfvjjr)), as often as `commit_every` says ([wkryj60b0](core/lifecycle.md#wkryj60b0)).
9. A run that waits on time or messages suspends: the host drops its code ([d9818h4m4](workflows.md#d9818h4m4)), and the run sends nothing until its wakeup ([gnhavbn5m](core/cost.md#gnhavbn5m)). A message lands in its inbox ([vcgwgaf4n](core/messages.md#vcgwgaf4n)) and a sleep's end is fixed when it first runs ([eqpe8dchq](core/steps.md#eqpe8dchq)).
10. The runs of a worker that crashed are taken over once its claims expire ([t452yredf.b](core/lifecycle.md#t452yredf)); one that leaves lets its steps end within `shutdown_grace` ([dh7y75pvn](workers.md#dh7y75pvn), [bv0addfga](core/lifecycle.md#bv0addfga)).

## End

11. What leaves the workflow function ends the run ([g039jvy6k](workflows.md#g039jvy6k)): its end creates `result` and the collection's due entry and tells the runs awaiting it ([kqh75fg4a](core/lifecycle.md#kqh75fg4a)).
12. A cancel ends it cancelled ([vmk2ydwta](core/lifecycle.md#vmk2ydwta)), with its children still running ([t873v7eg5](core/lifecycle.md#t873v7eg5)).
13. A continuation starts the chain's next run ([deycjjrap](core/lifecycle.md#deycjjrap), [hy3f1z5jj](chains.md#hy3f1z5jj), [gjnefe4w6](chains.md#gjnefe4w6)), and a `retry` a new run of a failed chain ([vb6krfmej](core/lifecycle.md#vb6krfmej)).
14. Clients read the end by `status` and `result` ([gt35c42xy](core/lifecycle.md#gt35c42xy), [e9a24k5b9](core/lifecycle.md#e9a24k5b9), [y025f8qen](clients.md#y025f8qen)).

## Collection

15. Once its retention has passed, a worker collects the run ([yx9pxz6mr](core/lifecycle.md#yx9pxz6mr), [pn928919v](workers.md#pn928919v)), by operations a second collector would repeat the same ([cv078fzet](core/lifecycle.md#cv078fzet)), and a chain's first run leaves a tombstone ([ja67c3az7](core/lifecycle.md#ja67c3az7)).
