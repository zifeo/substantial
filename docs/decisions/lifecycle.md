# A run's life

This note holds no law. It follows one run from its start to its expiry, the order of [Architecture](../architecture.md#how-it-works) and of the reference's run machine ([Interfaces](../interfaces.md#i-23-the-core)), and names at each step the law that decides it: a host law, or a law of the core, in [core](core/).

## Start

1. A client or a parent names the run: the caller's `id`, else a ULID the host draws ([cq034wpjp](clients.md#cq034wpjp)), and a child's derived from its parent's ([yb61qtt3s](core/children.md#yb61qtt3s)). The name must pass the naming rules ([px8dv58y7](clients.md#px8dv58y7)).
2. The start creates the chain's head, then the due entry ([jbkv4v0we](core/lifecycle.md#jbkv4v0we)), from a declared workflow ([bxb0ygj7m](promises.md#bxb0ygj7m)).
3. A start through a `Substantial` wakes its own workers at once ([c46hgkfse](workers.md#c46hgkfse)); others find the run at their next scan ([d9vmk6y6y](core/lifecycle/queue.md#d9vmk6y6y), [qsw5w98x3](core/lifecycle.md#qsw5w98x3)).

## Replay

4. A worker serving the run's name and version ([d9vmk6y6y.d](core/lifecycle/queue.md#d9vmk6y6y), [pjrbw31h4](promises.md#pjrbw31h4)) replays it, leasing it before its first step attempt ([j1nq6rqpf](core/lifecycle/leases.md#j1nq6rqpf)), within its `concurrency` ([rb377j7db](core/lifecycle.md#rb377j7db)) and the settings of its preset ([yxy5pzfkp](workers.md#yxy5pzfkp)).
5. It reads the chain's head ([hdzwp5tnf](core/cost.md#hdzwp5tnf)) and calls the workflow function from its first line ([g4kv2kj51](promises.md#g4kv2kj51)). Recorded calls return their outcomes ([rbwstdetg](core/history.md#rbwstdetg)); the host runs the code one `Next` at a time ([k3zma9hjs](hosts.md#k3zma9hjs)).
6. A call the history does not answer blocks the run with `NonDeterminism` ([rbwstdetg](core/history.md#rbwstdetg)), and the replay is dropped there ([v4n6ja42e](steps.md#v4n6ja42e)).
7. New steps run at least once ([ftq7bbgff](core/steps.md#ftq7bbgff)), with their retries ([a03w2yqkz](core/steps.md#a03w2yqkz)), and the host runs them as [Steps](steps.md) says.

## Commit and suspension

8. The replay commits what it recorded by a compare-and-set of the chain's head, and of two replays the first wins ([rnx5b50bn](core/commit.md#rnx5b50bn)), then creates its effects ([kkcyc97cz](core/commit.md#kkcyc97cz)), as often as `commit_every` says ([wkryj60b0](core/lifecycle.md#wkryj60b0)).
9. A run that waits on time or messages suspends: the host drops its code ([d9818h4m4](workflows.md#d9818h4m4)), and the run sends nothing until its wakeup ([gnhavbn5m](core/cost.md#gnhavbn5m)). A message is an entry in its workflow's queue ([dnxwdv0ac](core/messages.md#dnxwdv0ac)) and a sleep's end is fixed when it first runs ([eqpe8dchq](core/steps.md#eqpe8dchq)).
10. The runs of a worker that crashed are taken over once its leases expire ([j1nq6rqpf.b](core/lifecycle/leases.md#j1nq6rqpf)); one that leaves lets its steps end within `shutdown_grace` ([dh7y75pvn](workers.md#dh7y75pvn), [ywes67t3h](core/lifecycle.md#ywes67t3h)).

## End

11. What leaves the workflow function ends the run ([dxgjekysz](workflows.md#dxgjekysz)): its end writes the chain's head with its expiry and tells the runs awaiting it ([e93tpdr4p](core/lifecycle/chains.md#e93tpdr4p)).
12. A cancel ends it cancelled ([vmk2ydwta](core/lifecycle.md#vmk2ydwta)), and every end cancels its children still running but those started with `abandon` ([fca4h4ker](core/lifecycle.md#fca4h4ker)).
13. A continuation starts the chain's next run and archives the run before ([db0mrbxkk](core/lifecycle/chains.md#db0mrbxkk), [hy3f1z5jj](chains.md#hy3f1z5jj), [gjnefe4w6](chains.md#gjnefe4w6)), and a `retry` a new run of a failed chain ([vb6krfmej](core/lifecycle.md#vb6krfmej)).
14. Clients read the end by `status` and `result` ([f91jrjyjy](core/lifecycle.md#f91jrjyjy), [hnjtggsx7](core/lifecycle.md#hnjtggsx7), [y025f8qen](clients.md#y025f8qen)), and every run by its id ([fw4aw2ath](core/lifecycle/chains.md#fw4aw2ath)).

## Expiry

15. The run is kept, readable by its id and its id refused to a new start, for its retention, counted from its own end whatever its children do ([a1ys7at7p](core/lifecycle/expiry.md#a1ys7at7p), [pnctq6tx8](workers.md#pnctq6tx8)); then the store removes it, a child's end or a cancel that reaches it after is ignored ([n0ge6zsws](core/lifecycle/expiry.md#n0ge6zsws)), and every call that names it raises `RunNotFound` ([qkh68yrf1](core/lifecycle/expiry.md#qkh68yrf1)).
