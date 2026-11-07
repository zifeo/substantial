# Substantial

Brokerless durable execution for Rust, Python and TypeScript.

Write a workflow as a plain async function. Substantial records the result of each step, so after a crash, a restart or a deploy the function picks up where it was, without running finished steps again. Retries, sleeps of days and messages from the outside wait without holding a process. The only infrastructure is a store: memory, a directory or S3. No broker, no server.

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
        s.Substantial.create("memory://shop", partitions=1) as sub,
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
    let sub = s::Substantial::create("memory://shop").partitions(1).await?;
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

await using sub = await s.Substantial.create("memory://shop", { partitions: 1 });
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

Python 3.13 or later:

```sh
uv add substantial==0.2.0b1         # or: pip install substantial==0.2.0b1
```

The exact pin installs the pre-release; without one, pip needs `--pre` and uv `--prerelease=allow`.

Rust 1.97 or later, with what the example uses:

```sh
cargo add substantial@0.2.0-beta.1
cargo add tokio --features macros,rt-multi-thread
cargo add serde --features derive
cargo add anyhow
```

TypeScript, on Node 24 or later, or Deno, from [JSR](https://jsr.io/@substantial/substantial):

```sh
npx jsr add @substantial/substantial@0.2.0-beta.1
deno add jsr:@substantial/substantial@0.2.0-beta.1
```

## A tour

The API is the same in every host: `snake_case` in Python and Rust, `camelCase` in TypeScript. The snippets are Python, with Rust and TypeScript folded under each, inside a workflow unless they start from `sub` or `run`. The library is imported as `s` (`import substantial as s`, `import * as s from "@substantial/substantial"`, `use substantial as s;`): `s.` declares, `c.` acts. [The reference][reference] covers each host, and [its table of actions][I-21] gives every call in all three.

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

### Messages

```python
approvers: list[str] = []
c.handle("approve", approvers.append)        # every "approve" so far, then each new one
c.handle({"add": add, "remove": remove})     # several names bound together
by = await c.receive("by")                   # the next "by"
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
c.send(&other_chain, "note", "hello").await?;
register(webhook, c.chain().to_owned()).await?;
```

```ts
const approvers: string[] = [];
c.handle<string>("approve", (name) => approvers.push(name));
c.handle({ add, remove });
const by = await c.receive<number>("by");
await c.send(otherChain, "note", "hello");
await register(webhook, c.chain);
```

</details>

Messages come from clients (`run.send`) or other runs (`c.send`), and a message id makes a resend delivered once. Sending to a run that ended raises `RunEnded`, to a chain that does not exist `RunNotFound`, from a run too. Handlers bound together see their past messages in arrival order, across names. A run consumes its messages in the order sent; across a continue as new, a message that arrives as the run continues may be consumed after one sent later. More in [I-9].

### Children

```python
check = await c.start(verify, {"archive": archive})     # a run of its own
ok = await check.result()
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
let check = c.start(&verify, VerifyInput { archive }).await?;   // a run of its own
let ok = check.result().await?;
```

```ts
const check = await c.start(verify, { archive });   // a run of its own
const ok = await check.result();
```

</details>

A child may start itself, for recursion over a tree, and may be a workflow of another language, through a stub of the same name. More in [I-11] and [I-5].

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
    .iterate(Walk::default(), |_, mut w| async move {
        let page = list_page(source.clone(), w.after.clone()).await?;
        if page.is_empty() {
            return Ok(ControlFlow::Break(w));
        }
        copy_files(page.clone(), target.clone()).await?;
        w.after = page.last().cloned();
        Ok(ControlFlow::Continue(w))
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
sub = await s.Substantial.create("file:/var/lib/shop", partitions=1)   # or s.Substantial.open(url)
async with s.Substantial.open(url) as sub: ...                         # closed at the end
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
let sub = s::Substantial::create("file:/var/lib/shop").partitions(1).await?;   // or s::Substantial::open(url)
let run = sub.start(checkout, order).id("order-a-42").await?;
let run = sub.run(checkout, "order-a-42");                // a handle on an existing run
run.send("address", "1 rue du Lac").id("address-a-42").await?;
let status = run.status().await?;                         // an s::RunStatus
let result = run.result().timeout(s::hours(2)).await?;
let progress: Option<Progress> = run.read("progress").await?;
```

```ts
await using sub = await s.Substantial.create("file:/var/lib/shop", { partitions: 1 });   // or s.Substantial.open(url); closed at the end of the scope
let run = await sub.start(checkout, { order: "a-42", amount: 1999 }, { id: "order-a-42" });
run = sub.run(checkout, "order-a-42");                    // a handle on an existing run
await run.send("address", "1 rue du Lac", { id: "address-a-42" });
const status = await run.status();
const result = await run.result({ timeout: s.hours(2) });
const progress = await run.read("progress");
```

</details>

`create` makes the namespace or opens it; `open` refuses one that does not exist. A Rust `Substantial` is freed when dropped. More in [I-16] and [I-17].

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

```python
sub = await s.Substantial.open("s3://s3.eu-central-1.amazonaws.com/shop/prod?region=eu-central-1",
                               credentials="env")
sub = await s.Substantial.open(url, credentials=role_keys)   # a function returning s.Credentials
```

<details>
<summary>In Rust and TypeScript</summary>

```rust
let sub = s::Substantial::open(url).credentials(s::Credentials::Env).await?;
let sub = s::Substantial::open(url).credentials(s::Credentials::provider(role_keys)).await?;
```

```ts
const sub = await s.Substantial.open(url, { credentials: "env" });
const sub = await s.Substantial.open(url, { credentials: roleKeys });
```

</details>

S3 takes `"env"` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`), explicit `s.Credentials(...)`, or a function, sync or async, returning keys with an optional `expires_at`: it is called at open and again within 5 minutes of that expiry, for roles and other temporary keys. `s3+http://` reaches a local server. Ended runs stay in the store: never delete keys under a namespace, by hand or with a lifecycle rule. More in [I-16].

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

A preset, `script` or `service`, sets every setting; `concurrency`, `max_idle`, `shutdown_grace`, `claim_ttl`, `continue_as_new_after` and `step_timeout` can be overridden by name. `serve()` stops on SIGTERM or Ctrl-C, letting steps in flight end. A worker logs a warning for each run that ends failed or cancelled, and a worker that dies logs at once and fails the `result()` waits of its process. The runs of a worker that crashed are taken over by the others within 35 seconds. More in [I-18].

## How it works

- **No broker, no server.** Workers and clients coordinate through the store alone, with four operations: an exclusive create, a get, a list and a delete. A run's history is a sequence of keys, each created once and never overwritten. A worker takes a run with a claim, an exclusive create it renews while it works, and finds due runs, timers and messages by listing due entries.
- **Checked at open.** Opening a namespace creates a key twice and refuses a store that accepts both, as an S3-compatible store ignoring conditional writes would.
- **One core.** The protocol lives in a core compiled to WebAssembly, with no I/O, clock or randomness of its own. Every host loads the same module and only runs your code and talks to the store, so a namespace can be shared by workers and clients in all three languages.

The store contract and the core's boundary are in [the reference][I-22]. What each verb does to the store, its round trips, its failure behaviour and its monthly cost on S3 are in [the architecture](https://github.com/zifeo/substantial/blob/main/docs/architecture.md).

## Development

```sh
cargo test --workspace
uv run pytest
npm ci && npm run check && npm test
```

The S3 tests run when `SUBSTANTIAL_S3_URL` is set, against a local RustFS:

```sh
docker compose -f dev/rustfs.compose.yaml up -d --wait
AWS_ACCESS_KEY_ID=substantial AWS_SECRET_ACCESS_KEY=substantial-secret \
SUBSTANTIAL_S3_URL='s3+http://127.0.0.1:39400/substantial/tests?region=us-east-1' \
cargo test --workspace  # or uv run pytest, or npm test
```

Each host has two examples on a file or memory store, `first_workflow` and `backup`: `cargo run -p substantial --example first_workflow -- /tmp/demo`, `uv run python/examples/first_workflow.py /tmp/demo-py`, `node --conditions=substantial-source typescript/examples/first_workflow.ts /tmp/demo-ts`.

`rust/wasm` and `rust/proto` are symlinks to `wasm/` and `proto/`, so that the published crate carries them (on Windows, clone with `git clone -c core.symlinks=true`). `npm run gen` regenerates the TypeScript protobuf code from `proto/`.

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
[reference]: https://github.com/zifeo/substantial/blob/main/docs/interfaces.md
