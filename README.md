# Substantial

Brokerless durable execution for Rust, Python and TypeScript.

Write a workflow as a plain async function. Substantial records the result of each step, so after a crash, a restart or a deploy the function picks up where it was, without running finished steps again. Retries, sleeps of days and messages from the outside wait without holding a process. The only infrastructure is a store: memory, a directory, S3 or Redis. No broker, no server.

## Example

An order: charge the customer, wait for the delivery address, wait out a delay, ship.

```python
import asyncio

import substantial as s


@s.step(retry=s.Retry(max_attempts=5, initial=s.seconds(1), max=s.minutes(1), multiplier=2))
async def charge(order: str, amount: int) -> str:
    print(f"charging {amount} for {order}")
    return f"pay-{order}"


async def ship(order: str, address: str) -> str:
    print(f"shipping {order} to {address}")
    return f"track-{order}"


@s.workflow
async def checkout(c: s.Context, order: str, amount: int) -> dict:
    payment = await charge(order, amount)
    address = await c.receive("address")
    await c.sleep(s.seconds(5))
    tracking = await c.step("ship", lambda: ship(order, address))
    return {"payment": payment, "tracking": tracking}


async def main() -> None:
    async with (
        s.Substantial.create("memory://shop") as sub,
        sub.worker([checkout], preset="script"),
    ):
        run = await sub.start(checkout, {"order": "a-42", "amount": 1999}, id="order-a-42")
        await run.send("address", "1 rue du Lac, Lausanne")
        print(await run.result())


asyncio.run(main())
```

```text
charging 1999 for a-42
shipping a-42 to 1 rue du Lac, Lausanne
{'payment': 'pay-a-42', 'tracking': 'track-a-42'}
```

- `charge` and `ship` are steps: their results are recorded, and the function, replayed from its first line at every wakeup, gets them back without running them again. A step can be declared, as `charge` is with its retry of up to five attempts and an exponential backoff, or wrapped inline on the fly under a name, as `c.step("ship", ...)` does with `ship`, a plain function.
- `c.receive("address")` takes the message the client sends with `run.send`. A message sent before the workflow asks for it waits in the run's inbox.
- `c.sleep` is a durable timer: while it waits, the run is suspended and holds no process.
- The worker runs the workflow; the client starts it, messages it and awaits its result. They can be the same process, as here, or many processes on many machines sharing the store. Leaving `async with` stops the worker, then closes the client.
- `id="order-a-42"` makes the start idempotent: starting it again finds the same run.

To see it survive a crash, replace `memory://shop` with `file:shop`, kill the process during the sleep and run it again: `charge` does not print again, and the run ships and returns.

<details>
<summary>The same in Rust</summary>

```rust
use serde::{Deserialize, Serialize};
use substantial as s;

#[derive(Serialize, Deserialize)]
struct Order {
    order: String,
    amount: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct Receipt {
    payment: String,
    tracking: String,
}

#[s::step(retry = s::Retry { max_attempts: 5, initial: s::seconds(1), max: s::minutes(1), multiplier: 2.0 })]
async fn charge(order: &str, amount: u32) -> anyhow::Result<String> {
    println!("charging {amount} for {order}");
    Ok(format!("pay-{order}"))
}

async fn ship(order: String, address: String) -> anyhow::Result<String> {
    println!("shipping {order} to {address}");
    Ok(format!("track-{order}"))
}

#[s::workflow]
async fn checkout(c: s::Context, Order { order, amount }: Order) -> s::Result<Receipt> {
    let payment = charge(&order, amount).await?;
    let address: String = c.receive("address").await?;
    c.sleep(s::seconds(5)).await?;
    let tracking = c.step("ship", move || ship(order.clone(), address.clone())).await?;
    Ok(Receipt { payment, tracking })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sub = s::Substantial::create("memory://shop").await?;
    let worker = sub.worker(checkout).preset(s::Preset::Script).start()?;
    let order = Order { order: "a-42".into(), amount: 1999 };
    let run = sub.start(checkout, order).id("order-a-42").await?;
    run.send("address", "1 rue du Lac, Lausanne").await?;
    println!("{:?}", run.result().await?);
    worker.shutdown().await?;
    Ok(())
}
```

```text
charging 1999 for a-42
shipping a-42 to 1 rue du Lac, Lausanne
Receipt { payment: "pay-a-42", tracking: "track-a-42" }
```

`#[s::workflow]` makes `checkout` a value passed to the worker and to `start`; the run handle is typed by the workflow's result, so `result()` needs no annotation. A declared step takes references, made owned once at the call; an inline step's closure builds a fresh future for each attempt, so it clones what it moved in.

</details>

<details>
<summary>The same in TypeScript</summary>

```ts
import * as s from "@substantial/substantial";

const charge = s.step(
  "charge",
  async (order: string, amount: number) => {
    console.log(`charging ${amount} for ${order}`);
    return `pay-${order}`;
  },
  { retry: { maxAttempts: 5, initial: s.seconds(1), max: s.minutes(1), multiplier: 2 } },
);

async function ship(order: string, address: string) {
  console.log(`shipping ${order} to ${address}`);
  return `track-${order}`;
}

const checkout = s.workflow("checkout", async (c, { order, amount }: { order: string; amount: number }) => {
  const payment = await charge(order, amount);
  const address = await c.receive<string>("address");
  await c.sleep(s.seconds(5));
  const tracking = await c.step("ship", () => ship(order, address));
  return { payment, tracking };
});

await using sub = await s.Substantial.create("memory://shop");
await using _worker = sub.worker([checkout], { preset: "script" });
const run = await sub.start(checkout, { order: "a-42", amount: 1999 }, { id: "order-a-42" });
await run.send("address", "1 rue du Lac, Lausanne");
console.log(await run.result());
```

```text
charging 1999 for a-42
shipping a-42 to 1 rue du Lac, Lausanne
{ payment: 'pay-a-42', tracking: 'track-a-42' }
```

Run it with `node checkout.ts` or `deno run -A checkout.ts`. Steps and workflows take their name as the first argument, since bundlers rename functions. `await using` stops the worker, then closes the client, at the end of the module.

</details>

## Install

Substantial runs on Linux and macOS. Windows is not supported: run it in WSL, as on Linux.

Python 3.12 or later:

```sh
uv add substantial==0.3.0            # or: pip install substantial==0.3.0
uv add "substantial[s3]==0.3.0"      # with S3 stores, or: pip install "substantial[s3]==0.3.0"
uv add "substantial[redis]==0.3.0"   # with Redis stores, or: pip install "substantial[redis]==0.3.0"
```

The `s3` extra adds aiohttp, which sends the requests of `s3://` stores, and botocore, which signs them; the `redis` extra adds redis-py, which Redis stores need.

Rust 1.97 or later, with what the example uses:

```sh
cargo add substantial@0.3.0
cargo add tokio --features macros,rt-multi-thread
cargo add serde --features derive
cargo add anyhow
```

Redis stores need the `redis` feature: `cargo add substantial@0.3.0 --features redis`, and [guest workflows](#guest-workflows) the `guests` feature.

TypeScript, on Node 24 or later, or Deno, from [JSR](https://jsr.io/@substantial/substantial):

```sh
npx jsr add @substantial/substantial@0.3.0
deno add jsr:@substantial/substantial@0.3.0
```

`s3://` stores also need the AWS SDK: `npm install @aws-sdk/client-s3`, or `deno add npm:@aws-sdk/client-s3`. Redis stores need `@redis/client`: `npm install @redis/client`, or `deno add npm:@redis/client`.

## A tour

The API is the same in every host: `snake_case` in Python and Rust, `camelCase` in TypeScript. The snippets are Python, with Rust and TypeScript folded under each, inside a workflow unless they start from `sub` or `run`. The library is imported as `s` (`import substantial as s`, `import * as s from "@substantial/substantial"`, `use substantial as s;`): `s.` declares, `c.` acts. [The reference][reference] covers each host, and [its table of actions][I-21] gives every call in all three.

[The integration guide][integration] runs the worker inside a web service on several pods, and [the testing guide][testing] tests workflows on a virtual clock.

### Rules of workflow code

A run replays its function from the first line at every wakeup. So side effects go in steps, the clock and randomness come from `c.now()`, `c.random()` and `c.uuid()`, which are recorded, and the function makes the same calls on every replay, branching only on inputs, step results and messages. Values are JSON, up to 64 MiB each (`ValueTooLarge`). More in [I-1], [I-2] and [I-15].

### Steps

```python
@s.step(retry=s.Retry(max_attempts=5, initial=s.seconds(1), max=s.minutes(1), multiplier=2), timeout=s.minutes(10))
async def upload(archive: str, target: str) -> str:
    return await s3.put(archive, target, idempotency_key=s.step_key())

size = await c.step("size", lambda: os.path.getsize(archive))                       # an inline step
a, b = await asyncio.gather(fetch(url_a), fetch(url_b))                               # steps in parallel
sizes = await asyncio.gather(*[c.step("size", os.path.getsize, f) for f in files])   # arguments after the function
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
#[s::step(retry = s::Retry { max_attempts: 5, initial: s::seconds(1), max: s::minutes(1), multiplier: 2.0 }, timeout = s::minutes(10))]
async fn upload(archive: &str, target: &str) -> anyhow::Result<String> {
    s3.put(archive, target, s::step_key()).await
}

let size = c.step_blocking("size", move || fs::metadata(&archive).map(|m| m.len())).await?;   // an inline step, on a blocking thread
let (a, b) = tokio::try_join!(fetch(&url_a), fetch(&url_b))?;                                  // steps in parallel
```

```ts
const upload = s.step(
  "upload",
  async (archive: string, target: string) => s3.put(archive, target, { idempotencyKey: s.stepKey() }),
  { retry: { maxAttempts: 5, initial: s.seconds(1), max: s.minutes(1), multiplier: 2 }, timeout: s.minutes(10) },
);

const size = await c.step("size", async () => (await stat(archive)).size);   // an inline step
const [a, b] = await Promise.all([fetch(urlA), fetch(urlB)]);              // steps in parallel
```

</details>

An inline step takes its name first; in Python, the function's arguments may follow it, since a lambda in a comprehension would read the loop's last value. A crash during a step runs it again, so a step runs at least once: `s.step_key()` is the same for every attempt and replay, for an idempotency key. `raise s.Permanent(...)` fails a step without retrying it. Every attempt has a timeout: the step's own, else its workflow's `step_timeout`, else the worker's, 5 minutes by default. A step out of attempts raises `StepFailed`, whose `cause` is the last attempt's failure, such as `StepTimeout`. More in [I-6].

### Sleeping and waiting

```python
await c.sleep(s.hours(1))
await c.sleep_until(datetime(2026, 12, 24, 18, 0, tzinfo=UTC))
approved = await c.wait_until(lambda: len(approvers) >= 2, timeout=s.days(1))   # False after a day
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
c.sleep(s::hours(1)).await?;
c.sleep_until(Utc.with_ymd_and_hms(2026, 12, 24, 18, 0, 0).unwrap()).await?;
let approved = c.wait_until(approvers.check(|a| a.len() >= 2)).timeout(s::days(1)).await?;   // false after a day
```

```ts
await c.sleep(s.hours(1));
await c.sleepUntil(new Date("2026-12-24T18:00:00Z"));
const approved = await c.waitUntil(() => approvers.length >= 2, { timeout: s.days(1) });   // false after a day
```

</details>

A sleep's end is fixed when it first runs, and a replay never sleeps again. `wait_until` checks its predicate after each message, child end and timer. More in [I-8].

### Polling and schedules

```python
url = await c.poll("export_url", provider.export_url, job, every=s.minutes(1), timeout=s.hours(2))   # None after 2 hours


@s.workflow
async def nightly(c: s.Context) -> None:
    async for at in c.schedule():                     # 03:00 in Zurich, missed nights backfilled
        await c.start(report, {"day": f"{at:%Y-%m-%d}"}, id=f"report-{at:%Y-%m-%d}")

await sub.schedule(nightly, "nightly", s.Cron("0 3 * * *", "Europe/Zurich"), revision=1, backfill=7)
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
let url: Option<String> = c
    .poll("export_url", move || { let job = job.clone(); async move { provider::export_url(&job).await } })
    .every(s::minutes(1))
    .timeout(s::hours(2))
    .await?;

#[s::workflow]
async fn nightly(c: s::Context) -> s::Result<()> {
    c.schedule(|c, at| async move {
        let day = at.format("%Y-%m-%d").to_string();
        c.start(&report, Report { day: day.clone() }).id(&format!("report-{day}")).await?;
        Ok(ControlFlow::Continue(()))
    })
    .await
}

sub.schedule(nightly, "nightly", Some(s::Cron::new("0 3 * * *", "Europe/Zurich")?), 1)
    .backfill(7)
    .await?;
```

```ts
const url = await c.poll("export_url", () => provider.exportUrl(job), {
  every: s.minutes(1),
  timeout: s.hours(2),
});

export const nightly = s.workflow("nightly", async (c) => {
  for await (const at of c.schedule()) {
    const day = at.toISOString().slice(0, 10);
    await c.start(report, { day }, { id: `report-${day}` });
  }
});

await sub.schedule(nightly, "nightly", new s.Cron("0 3 * * *", "Europe/Zurich"), { revision: 1, backfill: 7 });
```

</details>

`poll` tries a step until it returns a value, sleeping between tries, each try recorded. A schedule is a run looping on `c.schedule()`, configured by `sub.schedule`: every pod can call it at startup, a config applies only if its revision is higher, fires missed while no worker ran come oldest first, up to `backfill` of them (5 unless given, 7 nights here), and clock changes fire once. More in [I-30] and [I-31].

### Messages

```python
approvers: list[str] = []
c.handle("approve", approvers.append)        # every "approve" so far, then each new one
c.handle({"add": add, "remove": remove})     # several names bound together
by = await c.receive("by")                   # the next "by"
note = await c.receive("note", timeout=s.hours(1))   # or WaitTimeout after an hour
await c.send(other_chain, "note", "hello")   # to another run
await register(webhook, reply_to=c.chain)    # this run's id, for an answer
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
let approvers = s::Var::new(Vec::<String>::new());                         // shared by handlers and predicates
c.handle("approve", approvers.handler(|a, name: String| a.push(name)))?;
c.handle_all(s::Handlers::new().on("add", add).on("remove", remove))?;
let by: u32 = c.receive("by").await?;
let note: String = c.receive("note").timeout(s::hours(1)).await?;
c.send(&other_chain, "note", "hello").await?;
register(webhook, c.chain().to_owned()).await?;
```

```ts
const approvers: string[] = [];
c.handle<string>("approve", (name) => approvers.push(name));
c.handle({ add, remove });
const by = await c.receive<number>("by");
const note = await c.receive<string>("note", { timeout: s.hours(1) });
await c.send(otherChain, "note", "hello");
await register(webhook, c.chain);
```

</details>

Messages come from clients (`run.send`) or other runs (`c.send`), and a message id makes a resend delivered once. Sending to a run that ended raises `RunEnded`, to a chain that does not exist `RunNotFound`, from a run too. Handlers bound together see their past messages in arrival order, across names. A run consumes one sender's messages in the order sent; across a continue as new, a message that arrives as the run continues may be consumed after one sent later. More in [I-9].

### Children

```python
check = await c.start(verify, {"archive": archive})     # a run of its own
ok = await check.result()
await other.cancel("not needed")                         # a child ends cancelled
paid = await c.result("payment-42")                      # the end of any chain, a child or not
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
let check = c.start(&verify, VerifyInput { archive }).await?;   // a run of its own
let ok = check.result().await?;
other.cancel("not needed").await?;
let paid: Paid = c.result("payment-42").await?;
```

```ts
const check = await c.start(verify, { archive });   // a run of its own
const ok = await check.result();
await other.cancel("not needed");
const paid = await c.result<Paid>("payment-42");
```

</details>

A parent cancels a child it started as a client would, its outcome recorded. `c.result` awaits the end of any chain, without polling: the awaited run tells the waiting one when it ends. A child may start itself, for recursion over a tree, and may be a workflow of another language, through a stub of the same name. More in [I-11] and [I-5].

### Loops and continue as new

```python
async for w in c.iterate(Walk()):
    page = await list_page(source, after=w.after)
    if not page:
        break
    await copy_files(page, target)
    w.after = page[-1]
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
let walk = c
    .iterate(Walk::default(), |_, mut w| {
        let (source, target) = (source.clone(), target.clone());
        async move {
            let page = list_page(source, w.after.clone()).await?;
            if page.is_empty() {
                return Ok(ControlFlow::Break(w));
            }
            copy_files(page.clone(), target).await?;
            w.after = page.last().cloned();
            Ok(ControlFlow::Continue(w))
        }
    })
    .await?;
```

```ts
for await (const w of c.iterate(new Walk())) {
  const page = await listPage(source, w.after);
  if (page.length === 0) break;
  await copyFiles(page, target);
  w.after = page.at(-1);
}
```

</details>

Once the history passes a threshold, `iterate` continues the run as a new one carrying the loop's state, and only it, so a loop can run forever on a short history. `await c.continue_as_new(input)` does it by hand. More in [I-12].

### Undo and sagas

```python
@s.step(undo=refund)                   # refund(payment, card, amount)
async def charge(card: str, amount: int) -> str: ...

async with c.saga():                   # on an error, undo the steps inside, then raise it again
    payment = await charge(card, amount)
    await reserve(items)

await c.rollback("fraud")              # undo every step so far, most recent first; the run fails
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
#[s::step(undo = refund)]                 // refund(payment, card, amount)
async fn charge(card: &str, amount: u32) -> anyhow::Result<String> { ... }

let payment = c
    .saga(|_| async move {                // on an error, undo the steps inside, then return it again
        let payment = charge(&card, amount).await?;
        reserve(&items).await?;
        Ok(payment)
    })
    .await?;

return Err(c.rollback("fraud").await);    // undo every step so far, most recent first; the run fails
```

```ts
const charge = s.step("charge", chargeCard, { undo: refund });   // refund(payment, card, amount)

await c.saga(async () => {               // on an error, undo the steps inside, then throw it again
  const payment = await charge(card, amount);
  await reserve(items);
});

await c.rollback("fraud");               // undo every step so far, most recent first; the run fails
```

</details>

An undo receives the step's result then its arguments, and runs with the step's retry. More in [I-10].

### Cancellation

```python
await run.cancel("not needed")         # from a client

try:
    await c.wait_until(lambda: len(approvers) >= 2)
except s.Cancelled:
    await c.rollback("cancelled")
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
run.cancel("not needed").await?;      // from a client

match c.wait_until(approvers.check(|a| a.len() >= 2)).await {
    Err(s::Error::Cancelled(_)) => return Err(c.rollback("cancelled").await),
    other => other?,
};
```

```ts
await run.cancel("not needed");       // from a client

try {
  await c.waitUntil(() => approvers.length >= 2);
} catch (e) {
  if (!(e instanceof s.Cancelled)) throw e;
  await c.rollback("cancelled");
}
```

</details>

A cancelled run raises `Cancelled` where it waits; a step already running finishes, and undos are never cancelled. More in [I-13].

### Clients

```python
sub = await s.Substantial.create("file:/var/lib/shop")   # or s.Substantial.open(url)
async with s.Substantial.open(url) as sub: ...           # closed at the end
run = await sub.start(checkout, {"order": "a-42", "amount": 1999}, id="order-a-42")
run = sub.run(checkout, "order-a-42")                 # a handle on an existing run
await run.send("address", "1 rue du Lac", id="address-a-42")
status = await run.status()       # pending, running, suspended, completed, failed, cancelled
result = await run.result(timeout=s.hours(2))
progress = await run.read("progress")                 # what the run published with c.publish
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
let sub = s::Substantial::create("file:/var/lib/shop").await?;   // or s::Substantial::open(url)
let run = sub.start(checkout, order).id("order-a-42").await?;
let run = sub.run(checkout, "order-a-42");                // a handle on an existing run
run.send("address", "1 rue du Lac").id("address-a-42").await?;
let status = run.status().await?;                         // an s::RunStatus
let result = run.result().timeout(s::hours(2)).await?;
let progress: Option<Progress> = run.read("progress").await?;
```

```ts
await using sub = await s.Substantial.create("file:/var/lib/shop");   // or s.Substantial.open(url); closed at the end of the scope
let run = await sub.start(checkout, { order: "a-42", amount: 1999 }, { id: "order-a-42" });
run = sub.run(checkout, "order-a-42");                    // a handle on an existing run
await run.send("address", "1 rue du Lac", { id: "address-a-42" });
const status = await run.status();
const result = await run.result({ timeout: s.hours(2) });
const progress = await run.read("progress");
```

</details>

`create` makes the namespace or opens it; `open` refuses one that does not exist. A Rust `Substantial` is freed when dropped. More in [I-16] and [I-17].

### Live runs, history and retry

```python
async for info in sub.runs(checkout):        # pending, waiting on time or messages, every version
    print(info.run, info.version, info.waiting_on, info.due_at)

for event in await run.history():            # the events its replays read
    print(event.segment, event.at, event.kind, event.data)

try:
    await run.result()
except s.RunFailed:
    ...                                      # fix the code, or wait out the outage
    await run.retry()                        # recorded steps return their values, the failed step runs again
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
use futures::TryStreamExt;

let mut runs = sub.runs(&checkout);
while let Some(info) = runs.try_next().await? {
    println!("{} {:?} {} {:?}", info.run, info.version, info.waiting_on, info.due_at);
}
for event in run.history().await? {
    println!("{} {} {} {}", event.segment, event.at, event.kind, event.data);
}
if run.result().await.is_err() {
    run.retry().await?;
}
```

```ts
for await (const info of sub.runs(checkout)) console.log(info.run, info.version, info.waitingOn, info.dueAt);
for (const event of await run.history()) console.log(event.segment, event.at, event.kind, event.data);
try {
  await run.result();
} catch (e) {
  if (!(e instanceof s.RunFailed)) throw e;
  await run.retry();
}
```

</details>

`runs` reads a workflow's queue, never its histories: ended runs are not listed, though one that just ended may be for a moment. `retry` starts a new run of the chain that replays the failed one without its failure, so a payment that succeeded is not charged again and the failed step gets a fresh retry budget; `s.step_key()` stays the same. More in [I-17].

### Cost, budgets and observing

```python
@s.step
async def summarize(text: str) -> str:
    reply = await llm.complete(text, idempotency_key=s.step_key())
    s.charge("USD", reply.cost_nano_usd)                 # this attempt's spend, integer nano-dollars
    return reply.text

@s.workflow(budget={"requests": 5_000, "USD": 2_000_000_000})   # each run: 5,000 store requests, $2
async def digest(c: s.Context, urls: list[str]) -> str:
    summary = await summarize(" ".join(urls))
    cost = await c.cost()                                # recorded, as c.now() is
    c.log.info("%d requests, %s", cost.requests, cost.amounts)
    return summary

def observe(event: s.Observed) -> None:                  # an s.StoreOp, s.Attempt, s.Replay or s.Scan
    print(event)                                         # forward to your metrics

sub = await s.Substantial.open("s3://s3.us-east-1.amazonaws.com/acme/prod?region=us-east-1", credentials="env",
                               prices=s.Prices("aws-s3-standard-us-east-1-2026-11-09"), observe=observe)
run = await sub.start(digest, {"urls": urls}, budget={"USD": 5_000_000_000})   # over the workflow's
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
#[s::step]
async fn summarize(text: String) -> anyhow::Result<String> {
    let reply = llm::complete(&text, &s::step_key()).await?;
    s::charge("USD", reply.cost_nano_usd)?;
    Ok(reply.text)
}

#[s::workflow(budget = [("requests", 5_000), ("USD", 2_000_000_000)])]
async fn digest(c: s::Context, input: Digest) -> s::Result<String> {
    let summary = summarize(input.urls.join(" ")).await?;
    let cost = c.cost().await?;
    c.log.info(format!("{} requests, {:?}", cost.requests, cost.amounts));
    Ok(summary)
}

let sub = s::Substantial::open("s3://s3.us-east-1.amazonaws.com/acme/prod?region=us-east-1")
    .credentials(s::Credentials::Env)
    .prices(s::Prices::named("aws-s3-standard-us-east-1-2026-11-09")?)
    .observe(|event: &s::Observed| println!("{event:?}"))
    .await?;
let run = sub.start(&digest, input).budget([("USD", 5_000_000_000)]).await?;
```

```ts
const summarize = s.step("summarize", async (text: string) => {
  const reply = await llm.complete(text, { idempotencyKey: s.stepKey(), signal: s.stepSignal() });
  s.charge("USD", reply.costNanoUsd);                    // a bigint
  return reply.text;
});

const digest = s.workflow("digest", async (c, input: { urls: string[] }) => {
  const summary = await summarize(input.urls.join(" "));
  const cost = await c.cost();
  c.log.info(`${cost.requests} requests`, cost.amounts);
  return summary;
}, { budget: { requests: 5_000n, USD: 2_000_000_000n } });

const sub = await s.Substantial.open("s3://s3.us-east-1.amazonaws.com/acme/prod?region=us-east-1", {
  credentials: "env",
  prices: new s.Prices("aws-s3-standard-us-east-1-2026-11-09"),
  observe: (event) => console.log(event),
});
const run = await sub.start(digest, { urls }, { budget: { USD: 5_000_000_000n } });
```

</details>

The core counts each run's store requests, the adapter's retries included, and with `prices` their money; steps add what they charge, children their totals. A run past a limit of its budget raises `s.BudgetExceeded` where it waits or calls next, may still roll back or publish, and ends failed. `observe` gets every store operation, step attempt, replay and scan, to feed any metrics system. More in [I-27], [I-28] and [I-29].

### Versions

```python
@s.workflow(name="checkout", version=2)
async def checkout_v2(c: s.Context, order: str, amount: int) -> dict: ...
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
#[s::workflow(name = "checkout", version = 2)]
async fn checkout_v2(c: s::Context, input: Order) -> s::Result<Receipt> { ... }
```

```ts
const checkoutV2 = s.workflow("checkout", async (c, input: Order) => { ... }, { version: 2 });
```

</details>

A run started with a version is replayed only by that version's code, so old and new code serve their runs side by side through a deploy. More in [I-5].

### Stores

| URL | Store |
| --- | --- |
| `memory://<ns>` | memory, in the process, for tests |
| `file:<path>` | a directory, for development and one machine |
| `s3://<endpoint>/<bucket>/<ns>?region=<r>` | S3 with conditional writes: AWS, MinIO, Ceph RGW, R2 |
| `redis://<host>:<port>/<ns>?db=0&replicas=0`, `redis+sentinel://`, `redis+cluster://` | Redis or Valkey 7.2+ with AOF, one server, Sentinel or a cluster, every create fsynced on every replica; workers wake as soon as a run is started or messaged |

```python
sub = await s.Substantial.open("s3://s3.eu-central-1.amazonaws.com/shop/prod?region=eu-central-1",
                               credentials="env")
sub = await s.Substantial.open(url, credentials=role_keys)   # a function returning s.Credentials
sub = await s.Substantial.open("redis+sentinel://10.0.0.1:26379,10.0.0.2:26379/shop?master=main&db=0&replicas=all",
                               credentials=s.Password("...", username="shop"))
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
let sub = s::Substantial::open(url).credentials(s::Credentials::Env).await?;
let sub = s::Substantial::open(url).credentials(s::Credentials::provider(role_keys)).await?;
let sub = s::Substantial::open(redis_url)
    .credentials(s::Credentials::Password { password: "...".into(), username: Some("shop".into()) })
    .await?;
```

```ts
const sub = await s.Substantial.open(url, { credentials: "env" });
const sub = await s.Substantial.open(url, { credentials: roleKeys });
const sub = await s.Substantial.open(redisUrl, {
  credentials: new s.Password({ password: "...", username: "shop" }),
});
```

</details>

S3 takes `"env"` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`), explicit `s.Credentials(...)`, or a function, sync or async, returning keys with an optional `expires_at`: it is called at open and again within 5 minutes of that expiry, for roles and other temporary keys. `s3+http://` reaches a local server. Redis takes none, `"env"` (`REDIS_PASSWORD`, `REDIS_USERNAME`), a password, or such a function returning one (`s::Credentials::password_provider` in Rust). Ended runs are deleted 30 days after they end, or after the `retention` a worker or a workflow sets, at most 100 years; never delete keys under a namespace yourself, by hand or with a lifecycle rule. More in [I-16].

### Workers

```python
async with sub.worker([checkout], preset="script"):                             # inside a script
    ...

await sub.worker([checkout, verify], preset="service", concurrency=32).serve()   # a long-lived process
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
let worker = sub.worker(checkout).preset(s::Preset::Script).start()?;
sub.worker((checkout, verify)).preset(s::Preset::Service).set("concurrency", 32).serve().await?;
```

```ts
await using _worker = sub.worker([checkout], { preset: "script" });
await sub.worker([checkout, verify], { preset: "service", concurrency: 32 }).serve();
```

</details>

A preset, `script` or `service`, sets every setting; `concurrency`, `max_idle`, `shutdown_grace`, `claim_ttl`, `continue_as_new_after`, `step_timeout` and `retention` can be overridden by name. `serve()` stops on SIGTERM or Ctrl-C, letting steps in flight end. A worker logs a warning for each run that ends failed or cancelled, and a worker that dies logs at once and fails the `result()` waits of its process. The runs of a worker that crashed are taken over by the others within 35 seconds. More in [I-18].

A job run on a schedule, such as a Kubernetes CronJob, drains instead of serving:

```python
await sub.worker([nightly, report], preset="script").drain(within=s.minutes(2))
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
sub.worker((nightly, report)).preset(s::Preset::Script).drain(s::minutes(2)).await?;
```

```ts
await sub.worker([nightly, report], { preset: "script" }).drain({ within: s.minutes(2) });
```

</details>

`drain` serves every run due now or within 2 minutes, takes over the runs of crashed workers whose claims expire by then, and returns once nothing is due within that horizon and nothing is in flight; between two jobs no worker lists the store. A schedule it serves needs a `backfill` of at least the job's period divided by the cron's. More in [I-18] and the [integration] guide.

### Guest workflows

A Rust worker also serves workflows compiled to WebAssembly, guests, which it loads from files when it starts: a workflow's code changes without a rebuild of the worker, and runs isolated from it. A guest is written with the Rust API, from the crate `substantial-guest`, as a `cdylib`:

```rust
use serde::Deserialize;
use substantial_guest as s;

#[derive(Deserialize)]
struct Input {
    name: String,
}

#[s::step]
async fn greet(name: &str) -> Result<String, std::io::Error> {
    Ok(format!("hello {name}"))
}

#[s::workflow]
async fn greeting(c: s::Context, input: Input) -> s::Result<String> {
    c.sleep(s::days(1)).await?;
    greet(&input.name).await
}

s::export!(greeting, steps = [greet]);
```

It is compiled for `wasm32-unknown-unknown`, then made a component of the world in [`wit/guest.wit`](wit/guest.wit):

```sh
cargo build --release --target wasm32-unknown-unknown
wasm-tools component new target/wasm32-unknown-unknown/release/greeting.wasm -o greeting.wasm
```

A worker with the `guests` feature loads it and serves it as any workflow, within the memory and the time between two waits it is given:

```rust
use substantial::guest::{Guest, Limits};

let limits = Limits { memory: 64 << 20, code_time: s::seconds(1) };
let guest = Guest::new(&std::fs::read("greeting.wasm")?, limits, |_| Ok(()))?;
let greeting = guest.workflow("greeting");
let worker = sub.worker(greeting.clone()).preset(s::Preset::Service).start()?;
sub.start(&greeting, serde_json::json!({ "name": "Ada" })).await?;
```

A guest has no network, clock or filesystem: its workflow code calls only Substantial's verbs, each recorded, and its steps only the interfaces the worker adds with the closure of `Guest::new`, a wasmtime linker. Each replay and each step attempt runs in an instance of its own, and a guest that traps or passes its bounds fails with `GuestTrap` while the worker goes on. A guest's workflow code calls steps, with their retry and timeout, `sleep`, `sleep_until`, `receive`, with its timeout, `send`, `publish`, `now`, `random`, `uuid` and `c.log`. The decisions are in [the guest notes](docs/decisions/guests.md).

## How it works

- **No broker, no server.** Workers and clients coordinate through the store alone, with four operations: an exclusive create, a get, a list and a delete. A run's history is a sequence of keys, each created once and never overwritten. A worker takes a run with a claim, an exclusive create it renews while it works, and finds due runs, timers and messages by listing the due entries of each workflow it serves, one prefix per workflow.
- **Checked at open.** Opening a namespace creates a key twice and refuses a store that accepts both, as an S3-compatible store ignoring conditional writes would.
- **What it costs.** On S3, a run that starts, calls 3 steps, sleeps a day and completes makes 13 PUT, 6 GET, 9 LIST and 13 free DELETE over its life, collection included: $1.12 a month for 10,000 runs. An idle worker of the `service` preset lists each workflow it serves every 30 s, $0.43 a month per workflow.
- **One core.** The protocol lives in a core compiled to WebAssembly, with no I/O, clock or randomness of its own. Every host loads the same module and only runs your code and talks to the store, so a namespace can be shared by workers and clients in all three languages.

The store contract and the core's boundary are in [the reference][I-22]. What each verb does to the store, its round trips, its failure behaviour and its monthly cost on S3 are in [the architecture](https://github.com/zifeo/substantial/blob/main/docs/architecture.md).

## Development

```sh
cargo test --workspace
uv run pytest
npm ci && npm run check && npm test
```

The Redis tests run, in Rust with `--features redis`, when `SUBSTANTIAL_REDIS_URL`, `SUBSTANTIAL_REDIS_SENTINEL_URL` or `SUBSTANTIAL_REDIS_CLUSTER_URL` is set, against the Valkey of `dev/redis.compose.yaml`, `dev/redis-sentinel.compose.yaml` or `dev/redis-cluster.compose.yaml`, whose headers give each URL.

The S3 tests run when `SUBSTANTIAL_S3_URL` is set, against a local RustFS:

```sh
docker compose -f dev/rustfs.compose.yaml up -d --wait
AWS_ACCESS_KEY_ID=substantial AWS_SECRET_ACCESS_KEY=substantial-secret \
SUBSTANTIAL_S3_URL='s3+http://127.0.0.1:39400/substantial/tests?region=us-east-1' \
cargo test --workspace  # or uv run pytest, or npm test
```

Each host has two examples on a file or memory store, `first_workflow` and `backup`: `cargo run -p substantial --example first_workflow -- /tmp/demo`, `uv run python/examples/first_workflow.py /tmp/demo-py`, `node --conditions=substantial-source typescript/examples/first_workflow.ts /tmp/demo-ts`.

`rust/wasm` and `rust/proto` are symlinks to `wasm/` and `proto/`, so that the published crate carries them. `npm run gen` regenerates the TypeScript protobuf code from `proto/`.

## License

[MPL-2.0](https://github.com/zifeo/substantial/blob/main/LICENSE)

[I-1]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-1-how-a-workflow-runs
[I-2]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-2-rules-for-workflow-code
[I-5]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-5-workflows
[I-6]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-6-steps
[I-8]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-8-sleeping-and-waiting
[I-9]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-9-messages
[I-10]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-10-undo-rollback-and-sagas
[I-11]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-11-starting-other-workflows
[I-12]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-12-loops-and-compression
[I-13]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-13-cancellation
[I-15]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-15-large-values
[I-16]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-16-creating-and-opening-a-namespace
[I-17]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-17-starting-and-following-runs
[I-18]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-18-workers
[I-21]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-21-actions-in-each-host
[I-22]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-22-for-implementers
[I-27]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-27-cost-and-charges
[I-28]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-28-budgets
[I-29]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-29-observing
[I-30]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-30-polling
[I-31]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md#i-31-schedules
[integration]: https://github.com/zifeo/substantial/blob/main/docs/integration.md
[reference]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md
[testing]: https://github.com/zifeo/substantial/blob/main/docs/testing.md
