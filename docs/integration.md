# Substantial in a web service

The worker runs inside the web service's own process, started and stopped by the app's lifespan: no worker container, no broker. Several pods share one namespace on S3, and every pod's worker takes the runs that are due. This page builds such a service, a Starlette app that applies provider webhooks once per event id, debounces CRM changes, serializes the operations of each account through an actor, and runs a nightly job.

The whole service is [`python/examples/webhooks.py`](../python/examples/webhooks.py), and [`python/tests/test_integration_guide.py`](../python/tests/test_integration_guide.py) runs every snippet below on the [test environment](testing.md). The snippets are Python; TypeScript and Rust are noted where they differ. `I-n` points to [the reference](interfaces.md).

- [The worker in the lifespan](#the-worker-in-the-lifespan)
- [What start and send guarantee](#what-start-and-send-guarantee)
- [Webhooks applied once](#webhooks-applied-once)
- [A durable debounce](#a-durable-debounce)
- [Several pods](#several-pods)
- [Scheduling](#scheduling)
- [Testing the app](#testing-the-app)

## The worker in the lifespan

```python
def create_app(
    open_engine: Callable[[], AbstractAsyncContextManager[s.Substantial]], concurrency: int
) -> Starlette:
    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        async with (
            open_engine() as sub,
            sub.worker(WORKFLOWS, preset="service", concurrency=concurrency) as worker,
        ):
            await sub.start(nightly, id="nightly")
            app.state.sub, app.state.worker = sub, worker
            yield

    def live(request: Request) -> Response:
        return Response(status_code=500 if request.app.state.worker.error else 200)

    def ready(request: Request) -> Response:
        return Response(status_code=200 if request.app.state.worker.alive else 503)

    async def store_down(request: Request, e: Exception) -> Response:
        return JSONResponse({"error": str(e)}, 503, headers={"Retry-After": "5"})

    return Starlette(
        lifespan=lifespan,
        routes=[..., Route("/livez", live), Route("/readyz", ready)],
        exception_handlers={s.StoreError: store_down},
    )


@dataclass(frozen=True)
class Config:
    url: str
    partitions: int
    concurrency: int

    @classmethod
    def from_env(cls) -> Config:
        e = os.environ
        return cls(
            e["SUBSTANTIAL_URL"],
            int(e["SUBSTANTIAL_PARTITIONS"]),
            int(e["SUBSTANTIAL_CONCURRENCY"]),
        )

    def open(self) -> Any:
        s3 = self.url.split(":", 1)[0] in {"s3", "h3", "s3+http", "h3+http"}
        credentials = "env" if s3 else None
        return s.Substantial.create(self.url, partitions=self.partitions, credentials=credentials)


def app() -> Starlette:
    config = Config.from_env()
    return create_app(config.open, config.concurrency)
```

```sh
SUBSTANTIAL_URL='s3://s3.eu-central-1.amazonaws.com/acme/webhooks?region=eu-central-1' \
SUBSTANTIAL_PARTITIONS=4 SUBSTANTIAL_CONCURRENCY=16 \
uvicorn webhooks:app --factory --timeout-graceful-shutdown 10
```

- **The store.** `create` makes the namespace at the first pod's start and opens it at every other; a pod configured with other partitions fails at start with `LayoutError`, and one that cannot reach the store with `StoreError`, so it never becomes ready (I-16). On S3 the namespace is a prefix of a bucket, and the pods need no disk: a worker keeps the histories it read in memory, up to 16 MiB ([architecture](architecture.md#how-it-works)). Every setting comes from the environment, and a missing one stops the app at start.
- **The worker.** `async with sub.worker(...)` starts it with the app and stops it with the app, in the server's process. Use it rather than `serve()`, which installs SIGTERM and SIGINT handlers of its own where the server owns them. The worker runs on the server's event loop: a blocking call in an `async` step stalls the HTTP requests too, so write blocking code as a plain `def` step, which runs in a thread (I-6).
- **Concurrency.** `concurrency` is how many runs one worker replays at once, 16 in the `service` preset (I-18). It bounds replays, not steps: a run that gathers four steps runs four at once. Across pods it adds up, 3 pods at 16 replaying up to 48 runs. Nothing bounds the calls to one API across pods: route them through an actor ([Several pods](#several-pods)) or give the API client a budget of its own.
- **The nightly run.** Every pod starts it at startup under one id: the first creates it, the others get the same run (I-17).
- **Health.** `worker.alive` is whether the worker's loop runs, `worker.error` the error that killed it, else `None` (I-18). A worker that died stays dead: liveness fails on its error so that Kubernetes restarts the pod, and readiness fails whenever the worker does not run.

### Shutdown and Kubernetes

On SIGTERM an ASGI server stops taking requests, lets the open ones end, then runs the lifespan's shutdown: leaving `async with` stops the worker, then closes the client. The worker starts no new replay and lets the steps in flight end within `shutdown_grace`, 25 s in `service` (I-18). A run it leaves keeps its wakeup and loses its claim, so another pod resumes it at its next scan, within `max_idle`. A step still running when the grace ends is abandoned: an `async` step is cancelled at its `await`, a synchronous one goes on in its thread until the process exits, its outcome is not recorded, and the pod that resumes the run runs it again (I-6).

Kubernetes sends SIGKILL `terminationGracePeriodSeconds` after SIGTERM, 30 s by default. Set it above the server's drain plus `shutdown_grace`, with a margin: with uvicorn's `--timeout-graceful-shutdown 10` and the `service` preset, 40 s or more. A step that may run longer than `shutdown_grace` runs again at every deploy that catches it: keep it idempotent, or raise both values.

A pod that dies, by SIGKILL, out of memory or with its node:

- the runs it was replaying keep its claims until `claim_ttl` plus `claim_skew`, 35 s after its last renewal; then the pods serving their workflows take them over (I-18);
- the steps it was running run again on those pods, and their bodies may have had their effects already: steps run at least once (I-1);
- a suspended run, waiting on a timer or a message, belongs to no pod and is not affected.

### Errors

| Raised | By | Answer |
|---|---|---|
| `StoreError` | any client call, once the adapter's 5 attempts, 100 ms to 5 s apart, failed (I-26) | 503 with `Retry-After`: the provider retries, and a retry with the same ids does nothing twice |
| `RunExists` | `start` of an id taken by another workflow, version or input; or, `collected` true, by a run collected less than 7 days ago (I-17) | 409, for a person to look; when `collected`, what the route means: the provider webhook and the order route answer 200, the event or the order handled before |
| `RunEnded`, `RunNotFound` | `send` to a chain that ended, or that no `start` made (I-17) | what the route means: 409, 404; the example's actors never end, so its routes never meet them |
| `WaitTimeout` | `run.result(timeout=)` | 202, and the caller polls |
| `RunFailed` | `run.result()` of a run that failed or was cancelled, with `error.type` and `error.message`, and `cause`, the failure of the step that failed it, if one did (I-20) | the route's own failure |
| the worker's error | the `result()` waits in progress of a pod whose worker died (I-18) | 500, and liveness restarts the pod |
| `InvalidArgument` | an id or a key outside `[a-z0-9._-]{1,100}` (I-17) | 400 |

A store that hangs can hold a client call for minutes, each S3 attempt timing out after 60 s (I-26). Bound a webhook route with `asyncio.timeout` below the provider's own timeout: a call cut short is a crash at that point, which the provider's retry completes, as the next section shows.

In TypeScript a worker runs from `sub.worker(...)`: create it when the server starts, and on SIGTERM close the HTTP server, then `await worker.shutdown()` and `await sub.close()`; health reads `worker.alive` and `worker.error`. In Rust, `sub.worker(workflows).preset(s::Preset::Service).set("concurrency", 16).start()?` starts it, and `worker.shutdown().await?` follows the server's graceful shutdown, such as axum's `with_graceful_shutdown`; health reads `worker.is_alive()` and `worker.error()`.

## What start and send guarantee

When `await sub.start(workflow, input, id=...)` returns:

- **Durable.** The run's `start` key and its wakeup, a due entry, are created in the store ([architecture](architecture.md#clients)). On S3 these are two conditional `PutObject`s that S3 acknowledged, and the store contract requires an acknowledged create to survive a crash of the store and to be in every later listing (I-26): every pod finds the run, whatever happens to this one.
- **Seen.** This pod's worker lists at once, and takes the run unless it is full or another pod claimed it first. The other pods find it at their next scan, within `max_idle`: 30 s in `service` on `s3://`, at once on `h3://`, whose listings wait (I-18).
- **Deduplicated.** A `start` with the same id, workflow, version and input returns that run, and completes a start that stopped halfway; another workflow, version or input raises `RunExists` (I-17). The input is compared as canonical JSON (RFC 8785), whichever host started it. A run is kept for its `retention` after its chain ends, 30 days by default, then its id raises `RunExists` for 7 days, then starts a new run: a redelivery later than the retention plus 7 days runs again, so give the workflow a `retention` past the provider's retry window, or `s.FOREVER`. A failed run is final until it is collected: starting it again returns the failed run (I-16, I-17).

When `await run.send(name, value, id=...)` returns:

- **Durable.** The message's inbox key and the run's wakeup are created in the store, after a read found the chain's last run not ended ([architecture](architecture.md#clients)).
- **Seen.** As a start: this pod at once, the others within `max_idle`.
- **Deduplicated.** The same `id` sent again to the run is delivered once, within `dedup_window`, 7 days from its consumption and across continuations. After it, it is delivered again once the run has continued as new since it consumed the id, as an actor that compresses does; a run that has not continued keeps deduplicating it (I-9, I-18, I-19). A send without `id` is a new message every time.
- **Ordered.** The run consumes its messages in the order of the sender's clock, then in the order one sender sent them: one client process's messages in the order it sent them, ids given or not. Across processes within one millisecond, the order is arbitrary. Across a continue as new, a message that lands as the run continues may be consumed after a later one (I-9).

A webhook route that starts, then sends, and answers 2xx only once both returned, against a crash at each point ([architecture](architecture.md#when-something-goes-wrong)):

| The pod dies | Left in the store | The provider's retry |
|---|---|---|
| before `start` created anything | nothing | starts the run |
| after the `start` key, before its due entry | a run no worker finds | completes the start (I-17) |
| after `start`, before `send` | a run waiting | gets the same run and sends |
| after the inbox key, before its due entry | a message the run reads at its next wakeup | sends it again: the wakeup is created, the message delivered once |
| after `send`, before answering | all of it | gets the same run; the message, sent again with its id, is delivered once |

So the route is idempotent, as long as the provider retries until it gets a 2xx and the ids are stable: the provider's own event and delivery ids, hashed ([Webhooks applied once](#webhooks-applied-once)). With a provider that never retries, the second row is a run that waits forever.

Not guaranteed:

- **Effects once.** A step runs at least once: a crash, a pod paused past its claim, an attempt past its timeout or a shutdown past its grace runs it again, after its body may have had its effect, or half of it, as an attempt past its timeout or its grace is interrupted at its `await` (I-1, I-6, I-18). Give the API an idempotency key: `s.step_key()`, the same for every attempt and replay, or a business id from the input (I-6, I-17).
- **A message consumed.** A send accepted while the run ends is never consumed (I-9).
- **A deadline.** A run waits for a free worker: while every worker is full, due runs wait. A run whose workflow no pod serves waits silently (I-18).
- **A call that raised.** A `start` or `send` that raised `StoreError` may have landed or not: call it again with the same ids.

## Webhooks applied once

```python
def run_id(prefix: str, external: str) -> str:
    """A run or message id for a provider's id: ids are lowercase `[a-z0-9._-]{1,100}`, and
    lowercasing a case-sensitive id would merge two of them."""
    return f"{prefix}-{hashlib.sha256(external.encode()).hexdigest()[:32]}"


@s.step(retry=RETRY)
async def apply_event(event: str, kind: str, data: dict) -> None:
    await crm.apply(f"evt-{event}", kind, data)


@s.workflow
async def provider_event(c: s.Context, event: str, kind: str, data: dict) -> None:
    await apply_event(event, kind, data)


async def provider_webhook(request: Request) -> Response:
    event = await request.json()
    stable = {"event": event["id"], "kind": event["type"], "data": event["data"]}
    try:
        await engine(request).start(provider_event, stable, id=run_id("evt", event["id"]))
    except s.RunExists as e:
        if e.collected:
            return Response(status_code=200)
        return JSONResponse({"error": "this event id came with another body"}, 409)
    return Response(status_code=200)
```

- **Ids.** Run and message ids are lowercase `[a-z0-9._-]{1,100}` (I-17), and a provider's ids are often case-sensitive: `evt_1AbC` and `evt_1abc` are two events, which lowercasing would merge. Hash the provider's id, under a prefix that says what it is.
- **The input.** `start` compares the input as canonical JSON (RFC 8785): keys in another order, `1.0` for `1`, or a pod in another language starting the same body gives the same run. A body that holds a delivery counter or a timestamp still raises `RunExists` at its first redelivery: start with the fields a redelivery keeps. A `RunExists` then means the provider sent another body under the same id. A NaN, an infinity, or in Python and Rust an integer past 2^53 - 1 raises `InvalidArgument`: send a large id as a string.
- **The effect.** The step's idempotency key is the event id: if the step runs twice, after a crash that came once the CRM had applied it, the CRM applies it once.
- **Long after.** A redelivery after the event's run was collected, past its `retention`, raises `RunExists` with `collected` true for 7 days: the event was applied then, and the route answers 200 without comparing the body, which is gone. After those 7 days the id starts a new run, which applies the event again under the same idempotency key.

```python
for event in ("evt_1AbC", "evt_1abc"):
    stable = {"event": event, "kind": "invoice.paid", "data": {}}
    await p.env.start(web.provider_event, stable, id=web.run_id("evt", event))
await p.env.settle()
assert sorted(crm.events) == ["evt-evt_1AbC", "evt-evt_1abc"]

stable = {"event": "evt_2", "kind": "invoice.paid", "data": {"invoice": "in_2"}}
rid = web.run_id("evt", "evt_2")
await p.env.start(web.provider_event, stable, id=rid)
for other in (
    {**stable, "delivery_attempt": 2},
    {"kind": "invoice.paid", "event": "evt_2", "data": {"invoice": "in_2"}},
):
    with pytest.raises(s.RunExists):
        await p.env.start(web.provider_event, other, id=rid)
```

## A durable debounce

Each change of a contact pushes its sync back by `QUIET`, and a burst of changes syncs at most `MAX_WAIT` after its first one.

```python
QUIET = s.minutes(2)
MAX_WAIT = s.minutes(15)


@dataclass
class Burst:
    versions: list[int] = field(default_factory=list)
    deadline: str | None = None


@s.step(retry=RETRY)
async def push_contact(contact: str, version: int) -> None:
    await crm.push(contact, version)


@s.workflow
async def contact_sync(c: s.Context, contact: str) -> None:
    changes: list[int] = []
    c.handle("changed", changes.append)
    async for b in c.iterate(Burst()):
        if not b.versions:
            await c.wait_until(lambda: bool(changes))
        else:
            left = datetime.fromisoformat(str(b.deadline)) - await c.now()
            quiet = left > timedelta(0) and await c.wait_until(
                lambda: bool(changes), timeout=min(QUIET, left)
            )
            if not quiet:
                await push_contact(contact, max(b.versions))
                b.versions, b.deadline = [], None
        if changes and b.deadline is None:
            b.deadline = (await c.now() + MAX_WAIT).isoformat()
        # Last, as messages also arrive while a step runs: only the state crosses a compression.
        b.versions += changes
        changes.clear()


async def crm_webhook(request: Request) -> Response:
    change = await request.json()
    sub = engine(request)
    contact = change["contact"]
    run = await sub.start(contact_sync, {"contact": contact}, id=run_id("contact", contact))
    await run.send("changed", change["version"], id=run_id("delivery", change["delivery"]))
    return Response(status_code=202)
```

- **The deadline moves.** A change ends the wait, and the next iteration waits again. Each `wait_until` has its own timeout, fixed when the call first runs (I-8), so the sync comes `QUIET` after the replay that consumed the last change: at once on the pod that received the webhook, which wakes its own worker, or within `max_idle` on another.
- **A maximum wait.** `deadline`, set at a burst's first change, caps every wait: an endless stream of changes still syncs every 15 minutes.
- **Compressible.** `iterate` continues the run as a new one once its history passes `continue_as_new_after`, carrying `Burst` and only it (I-12), so the run can debounce for years on a short history. The handler's list `changes` is local state, and messages are consumed while the push step runs too, after the code read the list: the loop moves the list into the state as its last statement. A list left in a handler at the end of an iteration is lost at the next compression.
- **The route.** It starts the run, the same input every time, then sends the change under the delivery's id, so that a redelivery within 7 days is consumed once.

```python
async def test_each_change_pushes_the_sync_back(crm: Any) -> None:
    async with pod() as p:
        run = await p.env.start(web.contact_sync, {"contact": "C1"}, id=web.run_id("contact", "C1"))
        for version in (1, 2, 3):
            await run.send("changed", version)
            await p.env.advance(s.minutes(1))
        await p.env.advance(s.seconds(59))
        assert crm.pushed == []
        await p.env.advance(s.seconds(1))
        assert crm.pushed == [("C1", 3)]


@pytest.mark.parametrize("apart", [False, True], ids=["one-instant", "settled"])
async def test_two_changes_in_one_instant_or_apart(crm: Any, apart: bool) -> None:
    async with pod() as p:
        run = await p.env.start(web.contact_sync, {"contact": "C1"}, id="contact-c1")
        await run.send("changed", 2)
        if apart:
            await p.env.settle()
        await run.send("changed", 1)
        await p.env.advance(s.minutes(2))
        assert crm.pushed == [("C1", 2)]


async def test_a_change_during_the_push_crosses_a_compression(crm: Any) -> None:
    async with pod(continue_as_new_after=0) as p:
        push = p.env.hold("push_contact")
        run = await p.env.start(web.contact_sync, {"contact": "C1"}, id="contact-c1")
        await run.send("changed", 1)
        await p.env.advance(s.minutes(2))
        assert push.held == 1
        await run.send("changed", 2)
        push.release()
        await p.env.advance(s.minutes(2))
        assert crm.pushed == [("C1", 1), ("C1", 2)]
```

`pod()` opens an environment with one `service` worker of the example's workflows and ends with `env.check()`. Two sends in a row reach the run together, in one replay, and `settle()` between them has it see the first alone (Time, [testing](testing.md#time)): the debounce gives the same push either way. `continue_as_new_after=0` compresses at every iteration (I-19), and `hold` keeps the push running while the second change arrives. `test_an_endless_stream_is_synced_within_the_max_wait` sends a change every minute for 40 minutes and finds pushes at 15 and 30 minutes.

## Several pods

What holds for one run, whichever pods replay it:

- **One writer per segment.** A run's history is a sequence of keys, each created once by an exclusive create: of two pods replaying the run, one commits and the other drops its work and replays from the winner's history ([architecture](architecture.md#how-it-works)). Every replay sees the same history, the same messages in the same places.
- **Claims.** A pod claims a run before replaying it and renews the claim every `claim_ttl / 2`, 15 s, while it replays. A claim avoids duplicate work; it is not a lock, and the segment decides.
- **Takeover.** The runs of a pod that leaves are resumed at the other pods' next scan, as it deletes its claims; those of a pod that dies, 35 s after its last renewal (I-18). Pod clocks must agree within `claim_skew`, 5 s.
- **A paused pod.** A pod frozen past its claim, by a long garbage collection or a network partition, has its runs taken over while its steps go on: their effects happen, then its commits lose. Two pods may then run one step at the same time, and only the API's idempotency key keeps the effect once.

What does not hold across runs: two runs are independent, on one pod or several. Nothing locks an entity and nothing orders two runs: two webhooks about one account start two runs that may call the ledger at the same time, in either order. `concurrency` and partitions bound the load, not the access to one entity.

For operations on one entity that must not overlap, one long-lived run per entity id, an actor, receives them as messages and applies them one at a time:

```python
@dataclass
class Account:
    balance: int = 0
    closed: bool = False
    queue: list[dict] = field(default_factory=list)


@s.step(retry=RETRY)
async def post(op: str, account: str, amount: int) -> None:
    await ledger.post(op, account, amount)


async def apply(account: str, a: Account, op: dict) -> dict:
    amount = op["amount"] if op["kind"] == "credit" else -op["amount"]
    if a.balance + amount < 0:
        return {"ok": False, "balance": a.balance}
    await post(op["id"], account, amount)
    a.balance += amount
    return {"ok": True, "balance": a.balance}


@s.workflow
async def account_actor(c: s.Context, account: str) -> None:
    ops: list[dict] = []
    c.handle("op", ops.append)
    async for a in c.iterate(Account()):
        if not a.queue:
            await c.wait_until(lambda: bool(ops))
        else:
            op = a.queue.pop(0)
            if a.closed:
                outcome = {"ok": False, "closed": True}
            elif op["kind"] == "close":
                # The run goes on, closed: its id stays taken, so the account never reopens.
                a.closed = True
                c.publish("closed", True)
                outcome = {"ok": True, "balance": a.balance, "closed": True}
            else:
                outcome = await apply(account, a, op)
            c.publish(f"op.{op['id']}", outcome)
            if "reply_to" in op:
                try:
                    await c.send(op["reply_to"], "reply", outcome)
                except (s.RunEnded, s.RunNotFound):
                    c.log.warning(
                        "op %s: %s no longer waits for its reply", op["id"], op["reply_to"]
                    )
        a.queue += ops
        ops.clear()


async def account_op(request: Request) -> Response:
    account, op = request.path_params["account"], await request.json()
    sub = engine(request)
    run = await sub.start(account_actor, {"account": account}, id=f"account-{account}")
    if await run.read("closed"):
        return JSONResponse({"error": f"account {account} is closed"}, 410)
    await run.send("op", op, id=op["id"])
    return Response(status_code=202)


async def op_outcome(request: Request) -> Response:
    p = request.path_params
    run = engine(request).run(account_actor, f"account-{p['account']}")
    outcome = await run.read(f"op.{p['op']}")
    return JSONResponse(outcome or {"pending": True}, 200 if outcome else 202)


@s.workflow
async def pay_order(c: s.Context, order: str, account: str, amount: int) -> bool:
    op = {"id": f"order-{order}", "kind": "debit", "amount": amount, "reply_to": c.chain}
    await c.send(f"account-{account}", "op", op)
    reply = await c.receive("reply")
    return reply["ok"]
```

- **One queue.** Whichever pod receives an operation starts the account's run, idempotently, and sends it the operation; the run applies the operations in the order it consumed them, one step at a time, whichever pod replays it. Its balance and its queue are the loop's state, and `ops` is drained into it last, as in the debounce.
- **Order.** Operations are consumed in the order of the sender's clock, then the message id (I-9): from two pods, in the order of their clocks.
- **An answer over HTTP.** The run publishes each outcome under the operation's key, and a client reads it with `run.read` (I-14): the route answers 202 until it exists. A published key follows the id rules, so an operation id has at most 97 characters here.
- **An answer to a workflow.** A run passes `c.chain` as `reply_to` and waits on `c.receive("reply")`. The actor logs a requester that ended rather than failing on its `RunEnded`.
- **A closed account.** Closing is an operation, `{"kind": "close"}`: the actor publishes `closed` and goes on, answering every later operation `{"ok": false, "closed": true}`, and the route answers 410 once it reads `closed`. The run never ends, so its id is never collected: an actor that ended, cancelled for one, would be collected after its retention, its id would raise `RunExists` for 7 days, then start a new actor with a zero balance, and the account would reopen empty. Its compressions keep its history bounded (I-19): the runs it continued from are collected after their retention. Close an account with the operation, never by cancelling its actor.
- **Throughput.** An actor applies one operation at a time, and each costs its store round trips ([architecture](architecture.md#workflow-verbs)): keep actors for entities whose operations must not overlap.

```python
async def test_an_actor_applies_its_ops_in_order_through_a_crash(ledger: Any) -> None:
    async with (
        pod(continue_as_new_after=0) as p,
        p.env.worker(web.WORKFLOWS, preset="service", continue_as_new_after=0),
    ):
        post = p.env.hold("post")
        run = await p.env.start(web.account_actor, {"account": "acc-1"}, id="account-acc-1")
        await send_ops(run)
        await p.env.settle()
        assert post.held == 1
        post.crash()
        await p.env.advance(s.seconds(35))
        assert await run.read("op.op-0") is None
        await p.env.advance(s.seconds(1))
        assert [await run.read(f"op.op-{i}") for i in range(4)] == OUTCOMES
        assert ledger.calls == 4
        assert ledger.posted == {
            "op-0": ("acc-1", 50),
            "op-1": ("acc-1", -20),
            "op-3": ("acc-1", 5),
        }
```

Two workers stand for two pods. `send_ops` sends a credit of 50, debits of 20 and 40, and a credit of 5; `OUTCOMES` are their outcomes, the debit of 40 refused. The ledger posted the first credit, then its pod crashed before recording it: the other pod takes the run over just past 35 s later, posts it again under the same key, and the ledger keeps it once. `test_a_pod_that_leaves_hands_its_runs_over_at_the_next_scan` does the same with `shutdown()`, and the other pod resumes within 30 s.

## Scheduling

A job at a given time is a workflow that begins with `await c.sleep_until(at)`: start it under an id, and `run.cancel(reason)` drops it, raising `Cancelled` at the sleep (I-8, I-13). A recurring job is a loop of them:

```python
ZURICH = ZoneInfo("Europe/Zurich")


def next_night(now: datetime) -> datetime:
    local = now.astimezone(ZURICH)
    night = local.replace(hour=3, minute=0, second=0, microsecond=0)
    if night <= local:
        night += timedelta(days=1)
    return night.astimezone(UTC)


@dataclass
class Nights:
    done: int = 0


@s.workflow
async def nightly(c: s.Context) -> None:
    async for n in c.iterate(Nights()):
        night = next_night(await c.now())
        await c.sleep_until(night)
        await report(night.astimezone(ZURICH).date().isoformat())
        n.done += 1
```

- The lifespan starts it under the id `nightly` on every pod: one run exists.
- A sleep's end is fixed when it first runs, and its firing is recorded (I-8), so the run sees it once, whichever pods try. It fires at its time on a pod whose scan saw it, the pod that suspended the run among them, and at the latest at a pod's next scan after it ([architecture](architecture.md#scans)). With every pod down at 03:00, it fires once one comes back.
- **Ids reused on a schedule.** An id reused sooner than its run's `retention` plus 7 days after the run ended meets its own tombstone: the run is collected after its retention, no sooner than 2 hours after it ended, and its id then raises `RunExists` for 7 days (I-17). A weekly child named `report-fri` with a retention of 1 hour is collected about 2 hours after it ends, and next week's start fails. Put the date or the period in such ids: `report-2026-11-06`.
- `iterate` keeps the history short over the years (I-12); `next_night` reads the time from `c.now()`, recorded, and 03:00 is an hour every day has once in Zurich.

```python
async with pod(now=datetime(2026, 10, 23, 12, 0, tzinfo=UTC)) as p:
    ...
    await p.env.start(web.nightly, id="nightly")
    await p.env.advance(s.days(3))
    assert nights == [
        ("2026-10-24", datetime(2026, 10, 24, 1, 0, tzinfo=UTC)),
        ("2026-10-25", datetime(2026, 10, 25, 2, 0, tzinfo=UTC)),
        ("2026-10-26", datetime(2026, 10, 26, 2, 0, tzinfo=UTC)),
    ]
```

## Testing the app

`env.client()` opens a `Substantial` on the environment's namespace and clock, which the app's lifespan takes as its own: the app's worker runs on the virtual clock, and leaving the lifespan closes that client, not the environment ([testing](testing.md#the-environment)).

```python
async with s.testing.Env.open(now=T0) as env:
    app = web.create_app(env.client, concurrency=4)
    async with serving(app):
        event = {"id": "evt_1AbC", "type": "invoice.paid", "data": {"invoice": "in_1"}}
        for attempt in (1, 2):
            body = {**event, "delivery_attempt": attempt}
            assert await call(app, "POST", "/webhooks/provider", body) == (200, None)

        op = {"id": "op-1", "kind": "credit", "amount": 30}
        assert await call(app, "POST", "/accounts/acc-1/ops", op) == (202, None)
        assert await call(app, "GET", "/accounts/acc-1/ops/op-1") == (202, {"pending": True})
        await env.settle()
        assert await call(app, "GET", "/accounts/acc-1/ops/op-1") == (
            200,
            {"ok": True, "balance": 30},
        )

        env.outage(s.minutes(1))
        assert (await call(app, "POST", "/accounts/acc-1/ops", op))[0] == 503
        await env.advance(s.minutes(1))
        assert await call(app, "POST", "/accounts/acc-1/ops", op) == (202, None)
    await env.check()
```

`serving` runs the app's lifespan as an ASGI server does, and `call` sends it one request, in a few lines of the test, with no server and no HTTP client. Nothing runs between two requests: the first GET answers 202, and `settle()` lets the worker apply the operation. During the outage the route answers 503, and the provider's retry, the same operation under the same id, is applied once.
