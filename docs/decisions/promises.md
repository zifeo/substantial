# What Substantial promises

A workflow is a plain async function, and after a crash, a restart or a deploy it picks up where it was, "without running finished steps again" ([README](../../README.md#substantial)). Retries, sleeps of days and messages wait "without holding a process", and "the only infrastructure is a store": no broker, no server (same paragraph). This note states each promise the README, the reference and the integration guide make, names the law whose sentence states it, and the laws that make it true. Most are laws of the core, in [core](core/), as the core decides every store operation; the host's part is how it runs the code around them.

## A finished step never runs again

A run executes the function from its first line at every wakeup, and a recorded call returns its recorded outcome without running (I-1). [ftq7bbgff](core/steps.md#ftq7bbgff) states the promise: a recorded step never runs again, as every call the history answers returns its recorded outcome ([twravbm6m](core/history.md#twravbm6m)). Every worker commits each step as it returns ([ry26kkc7v.a](workers.md#ry26kkc7v), [wkryj60b0](core/lifecycle.md#wkryj60b0)), by an exclusive create of the next segment, so of two workers racing on a run only one outcome stands ([czkkbyhe0](core/commit.md#czkkbyhe0)).

**§ <a id="g4kv2kj51">g4kv2kj51</a> Every replay of a run calls its workflow function anew, from its first line, with the run's input.**

## Steps run at least once

Effects once is "Not guaranteed" ([Integration](../integration.md#what-start-and-send-guarantee)): a crash loses the steps running at that moment, which run again on the worker that takes the run ([ftq7bbgff](core/steps.md#ftq7bbgff)), and an abandoned attempt's outcome is never recorded ([v4n6ja42e](steps.md#v4n6ja42e)). A step is therefore idempotent, or passes the API a key that is the same for every attempt and replay ([b1cj6wtk9](steps.md#b1cj6wtk9)). So a step that calls an outside API runs at least once: a crash after the call and before its outcome is recorded runs it again. Its effect happens once only when the API takes the step key as an idempotency key, "pass it to a payment provider, an `Idempotency-Key` header, a database unique key" ([Interfaces](../interfaces.md#i-6-steps), `s.step_key()`).

## Durable when it returns

A start and a send are durable when they return: the run's `start` and due entry, or the message's inbox key and the run's due entry, exist in the store, as [ct862686r](core/lifecycle.md#ct862686r) states for a start and [dva6cx6nb.a](core/messages.md#dva6cx6nb) for a send. That rests on the store contract: an acknowledged create survives a crash of the store ([dfd6vt90h](store.md#dfd6vt90h)), and no object is ever overwritten ([kwvhb7ehw](store.md#kwvhb7ehw)).

## Idempotent under the caller's ids

A webhook route is idempotent "as long as the provider retries until it gets a 2xx and the ids are stable" ([Integration](../integration.md#what-start-and-send-guarantee)). A start is named by the caller's id ([cq034wpjp](clients.md#cq034wpjp)) and a second start of it returns the same run, as [ct862686r.a](core/lifecycle.md#ct862686r) states, its input compared in canonical JSON ([aaev3rs1q](values.md#aaev3rs1q)). A resend with the same id is delivered once within the dedup window, as [r1nrgn5h8](core/messages.md#r1nrgn5h8) states, and a workflow's own send is delivered once, its run crashing after it or not ([b7g8bsqaq](core/messages.md#b7g8bsqaq)).

A call that failed is the case the ids are for: "A `start` or `send` that raised `StoreError` may have landed or not: call it again with the same ids" ([Integration](../integration.md#what-start-and-send-guarantee), "Not guaranteed"). A start that stopped halfway is completed by the next one ([ct862686r.a](core/lifecycle.md#ct862686r)), and a message sent again under its id is consumed once ([r1nrgn5h8](core/messages.md#r1nrgn5h8)).

## Ordered messages

[a1qvxtaj6](core/messages.md#a1qvxtaj6) states it: a run consumes the messages of one sender in the order sent, each one recorded where the code first sees it ([vcgwgaf4n](core/messages.md#vcgwgaf4n)), as a client's ids are monotonic ([wtwx1e769](clients.md#wtwx1e769)).

## Waiting holds no process

[d9818h4m4](workflows.md#d9818h4m4) states it: the host drops the code of a suspended run where it waits, so no process holds it, and the run sends no request until its wakeup ([gnhavbn5m](core/cost.md#gnhavbn5m)). A sleep's end is fixed when it first runs ([eqpe8dchq](core/steps.md#eqpe8dchq)).

## No broker, no server

[w89qkqkaj](store.md#w89qkqkaj) states it: workers and clients share the store alone, and due runs are keys every worker lists ([e3rcyme0p](core/lifecycle.md#e3rcyme0p)), a claim keeps two workers from doing the same work ([t452yredf](core/lifecycle.md#t452yredf)), and a crashed worker's runs are taken over once its claims expire.

## Deterministic replay

The code runs again at every wakeup, so it must make the same calls in the same order, branching only on step values, inputs and messages (I-2). The order in which parallel branches see their outcomes is recorded and delivered one at a time ([k3zma9hjs](hosts.md#k3zma9hjs)), and [twravbm6m](core/history.md#twravbm6m) states the promise: every call the history answers returns its recorded outcome, in order, and a call it does not answer fails the run with `NonDeterminism`. An incompatible change of unversioned code fails its runs that way (I-5).

## Versions through a deploy

A deploy changes code under runs in flight. A version lets old and new code run side by side through a deploy, the old one served until its runs end (I-5, "so old and new code run side by side through a deploy"), as [pjrbw31h4](#pjrbw31h4) states. A continuation keeps the run's version unless it names another workflow ([deycjjrap](core/lifecycle.md#deycjjrap), [gjnefe4w6](chains.md#gjnefe4w6)), a retry runs the failed run's version ([vb6krfmej](core/lifecycle.md#vb6krfmej)), and a worker takes only the runs of the versions it serves ([e3rcyme0p.d](core/lifecycle.md#e3rcyme0p)). `runs` lists the live runs of every version, each with its version ([j08c2ntyv](core/lifecycle.md#j08c2ntyv)), so an old version has drained once none of them has it.

**§ <a id="pjrbw31h4">pjrbw31h4</a> A worker replays each run with the code it was given under the run's workflow name and version, or under that name alone for a run without a version, so old and new code run side by side through a deploy.**

## One core, three languages

[fsvns3j6x](hosts.md#fsvns3j6x) states it: every host loads the same core, so a namespace can be shared by workers and clients in all three languages ([README](../../README.md#how-it-works)). The objects of a namespace are encoded by the core alone ([sqbjc1adw](core/history.md#sqbjc1adw)), the values of the code are stored as JSON ([e1dc4dar4](values.md#e1dc4dar4)), an input is compared in one canonical encoding ([aaev3rs1q](values.md#aaev3rs1q)), and a failure has the same fields in every host ([r3sd261h4](errors.md#r3sd261h4)). A workflow name belongs to one language (I-5): a worker serves the name with the code it is given, so a stub given to a worker would serve the name with the stub (I-5, "never give it to a worker, which would serve the name with the stub").

**§ <a id="bxb0ygj7m">bxb0ygj7m</a> Every host starts a run or a child from a declared workflow, never from a name alone, so a workflow that a worker of another language serves is started from a stub: a declaration of the same name, version and input whose body never runs.**

The hosts run where the maintainer supports them: "Substantial runs on Linux, macOS and WSL" ([README](../../README.md#install)). A worker's `serve()` stops on SIGTERM, a Unix signal, in every host ([dh7y75pvn.a](workers.md#dh7y75pvn)).

**§ <a id="k6m3smdpk">k6m3smdpk</a> Every host runs on Linux and macOS.**

## A known cost

[qashr34zn](core/cost.md#qashr34zn) states it: on S3 a run costs its requests, and the per-verb tables of the architecture document give each verb's. Counting them adds no request ([czph3f9yv](core/cost.md#czph3f9yv)), an idle worker costs one listing per workflow per `max_idle` ([gnhavbn5m](core/cost.md#gnhavbn5m)), and a budget stops a run that passes it ([fkgsx3gvk](core/cost.md#fkgsx3gvk)).

## Every run ends and is collected

A run ends completed, failed, cancelled or continued ([g039jvy6k](workflows.md#g039jvy6k)); a cancel reaches every call the run is blocked on and ends it cancelled ([vmk2ydwta](core/lifecycle.md#vmk2ydwta)), and a saga or a rollback runs its undos first ([ctp0q370t](sagas.md#ctp0q370t), [zjw5cess8](sagas.md#zjw5cess8)). [yx9pxz6mr](core/lifecycle.md#yx9pxz6mr) states the promise: every ended run is collected once its retention passes, so a namespace stops growing.

## Tests run the real protocol

[bsw9tpftr](testing.md#bsw9tpftr) states it: a test runs its workflows on a virtual clock, on the core production loads, so a passing test replays its runs as production does.

## Reading order

The other notes follow in this order. The architecture: [the store](store.md) and [one core, three hosts](hosts.md) and [guest workflows](guests.md), then the laws of the core, [the commit point](core/commit.md), [a run's lifecycle](core/lifecycle.md), [the history](core/history.md), [steps and waits](core/steps.md), [messages](core/messages.md), [undos, rollbacks and sagas](core/sagas.md), [children and chains](core/children.md) and [cost and budgets](core/cost.md). A run's life, from its start to its collection: [lifecycle](lifecycle.md). The programming model: [clients](clients.md), [the replay drive](workflows.md), [steps](steps.md), [waiting](waiting.md), [messages and handlers](messages.md), [sagas](sagas.md), [children, chains and loops](chains.md), [values](values.md) and [polling and schedules](schedules.md). Operating: [logs, progress and observing](observing.md), [workers](workers.md), [store adapters](adapters.md), [cost](cost.md) and [errors](errors.md). Testing: [the test environment](testing.md).
