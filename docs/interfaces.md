# Substantial 0.2 interfaces

One API, three hosts. Examples are in Python first. Names are the same in every host, `snake_case` in Python and Rust, `camelCase` in TypeScript. JSON keys are `snake_case` in every host, so a value reads the same whichever language wrote it. The library is imported as `s` (`import substantial as s`, `import * as s from "@substantial/substantial"`, `use substantial as s;`): `s.` declares, `c.` acts. Sections are numbered `I-n`, and a number is never reused. What no host implements is listed in [Later](#later). Installing each host is in the [README](../README.md#install).

- [I-1 How a workflow runs](#i-1-how-a-workflow-runs)
- [I-2 Rules for workflow code](#i-2-rules-for-workflow-code)
- [I-3 A first workflow](#i-3-a-first-workflow)
- [I-4 A backup](#i-4-a-backup)
- [I-5 Workflows](#i-5-workflows)
- [I-6 Steps](#i-6-steps)
- [I-7 Time, randomness and identifiers](#i-7-time-randomness-and-identifiers)
- [I-8 Sleeping and waiting](#i-8-sleeping-and-waiting)
- [I-9 Messages](#i-9-messages)
- [I-10 Undo, rollback and sagas](#i-10-undo-rollback-and-sagas)
- [I-11 Starting other workflows](#i-11-starting-other-workflows)
- [I-12 Loops and compression](#i-12-loops-and-compression)
- [I-13 Cancellation](#i-13-cancellation)
- [I-14 Logs and progress](#i-14-logs-and-progress)
- [I-15 Large values](#i-15-large-values)
- [I-16 Creating and opening a namespace](#i-16-creating-and-opening-a-namespace)
- [I-17 Starting and following runs](#i-17-starting-and-following-runs)
- [I-18 Workers](#i-18-workers)
- [I-19 Testing](#i-19-testing)
- [I-20 Errors](#i-20-errors)
- [I-21 Actions in each host](#i-21-actions-in-each-host)
- [I-27 Cost and charges](#i-27-cost-and-charges)
- [I-28 Budgets](#i-28-budgets)
- [I-29 Observing](#i-29-observing)
- [I-30 Polling](#i-30-polling)
- [I-31 Schedules](#i-31-schedules)
- [I-22 For implementers](#i-22-for-implementers)
- [Later](#later)

## I-1 How a workflow runs

- A run executes the workflow function from its first line **every time it wakes up**: after a sleep, a message, a retry backoff, a crash, a deploy.
- A **step** already recorded returns its recorded value at once, without running. A step never recorded runs, and its value is recorded as soon as it returns (`commit_every`, Workers).
- The function stops where every branch of it waits (a sleep, a message, a backoff): the run is suspended, and nothing runs until the next wakeup.
- A crash loses only the steps running at that moment, so they run again: **steps run at least once**. A recorded step never runs again.
- A run ends when the function returns (completed), raises (failed), is cancelled, or continues as new.

## I-2 Rules for workflow code

1. **Side effects only in steps.** I/O, network, files, sending anything: inside a step. The workflow function runs again on every wakeup.
2. **Steps are idempotent**, or keyed with `s.step_key()`: a step may run more than once.
3. **No clock, randomness or identifiers outside steps.** Use `c.now()`, `c.random()`, `c.uuid()`.
4. **Same calls on every run of the function.** Branch only on step values, inputs and messages. Branches in parallel (`gather`, `wait`, `Promise.race`, `select!`) are fine: the order of their outcomes is recorded.
5. **Step names are permanent.** A declared step is named by its function unless `name=` is given; renaming a step function breaks the runs in flight, so pin the old name with `name=` before a refactor. An inline step takes its name first, `c.step("total", fn)`.
6. **Values are JSON**: inputs, step values, messages, results. Rust decodes them into the code's types with serde. TypeScript, whose types are erased, decodes them with `decode`: without it a step that returns a `Date` gives a string, whatever its static type. Python gives them as JSON reads them, `dict`, `list`, `str`, `int`, `float`, `bool`, `None`: convert them in the code.
7. **Never swallow Substantial's control flow.** In Python, cancellation, rollback and continue as new are `BaseException`s, and a suspension raises `asyncio.CancelledError`, another one, at the `await` the code is blocked on (I-20): catch errors by name or with `except Exception`, never with a bare `except:` or `except BaseException:`. In TypeScript, rethrow when `isControl(e)`. In Rust, propagate with `?`.
8. **Predicates read state only**: a `wait_until` predicate does no I/O.
9. **One `iterate` per workflow**; code before it runs again in each run of the chain.
10. **A step without `retry` runs one attempt**: its first error fails the step.

## I-3 A first workflow

A step with retries, a sleep, a message and a wait on a handler. `examples/first_workflow` runs it in each host on a file store (README, Development).

```python
import asyncio
import random
import substantial as s


@s.step(retry=s.Retry(max_attempts=6, initial=s.seconds(1), max=s.minutes(1), multiplier=2))
def roulette() -> int:
    if random.random() < 1 / 6:
        raise Exception("shot!")
    return 100


@s.step
def multiply(a: float, b: float) -> float:
    return a * b


@s.workflow
async def example(c: s.Context) -> float:
    res = await roulette()
    await c.sleep(s.seconds(10))
    res = await multiply(res, 0.5)

    n = await c.receive("by")
    res = await multiply(res, n)

    stopped: list[None] = []
    c.handle("stop", stopped.append)
    await c.wait_until(lambda: bool(stopped))
    return res


async def main() -> None:
    async with (
        s.Substantial.create("file:demo/logs") as sub,
        sub.worker([example], preset="script"),
    ):
        run = await sub.start(example)
        await run.send("by", 2)
        await run.send("stop")
        print("Final output", await run.result())  # 100.0


asyncio.run(main())
```

- Both messages are sent at once. The next replay consumes them in order and keeps them: `receive` takes `"by"`, and binding the `"stop"` handler delivers the `"stop"` already consumed, so the wait ends at once.
- `roulette` gets up to six attempts, one second apart at first.
- `create` creates the namespace the first time and opens it after.

```ts
import * as s from "@substantial/substantial";

const roulette = s.step("roulette", () => {
  if (Math.random() < 1 / 6) throw new Error("shot!");
  return 100;
}, { retry: { maxAttempts: 6, initial: s.seconds(1), max: s.minutes(1), multiplier: 2 } });

const multiply = s.step("multiply", (a: number, b: number) => a * b);

export const example = s.workflow("example", async (c) => {
  let res = await roulette();
  await c.sleep(s.seconds(10));
  res = await multiply(res, 0.5);

  const n = await c.receive<number>("by");
  res = await multiply(res, n);

  let stopped = false;
  c.handle("stop", () => {
    stopped = true;
  });
  await c.waitUntil(() => stopped);
  return res;
});

await using sub = await s.Substantial.create("file:demo/logs");
await using _worker = sub.worker([example], { preset: "script" });
const run = await sub.start(example);
await run.send("by", 2);
await run.send("stop");
console.log("Final output", await run.result());
```

```rust
use substantial as s;

#[s::step(retry = s::Retry { max_attempts: 6, initial: s::seconds(1), max: s::minutes(1), multiplier: 2.0 })]
async fn roulette() -> anyhow::Result<f64> {
    if rand::random::<f64>() < 1.0 / 6.0 { anyhow::bail!("shot!") }
    Ok(100.0)
}

#[s::step]
async fn multiply(a: f64, b: f64) -> anyhow::Result<f64> { Ok(a * b) }

#[s::workflow]
async fn example(c: s::Context) -> s::Result<f64> {
    let res = roulette().await?;
    c.sleep(s::seconds(10)).await?;
    let res = multiply(res, 0.5).await?;

    let n: f64 = c.receive("by").await?;
    let res = multiply(res, n).await?;

    let stopped = s::Var::new(false);
    c.handle("stop", stopped.handler(|stopped, ()| *stopped = true))?;
    c.wait_until(stopped.check(|stopped| *stopped)).await?;
    Ok(res)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sub = s::Substantial::create("file:demo/logs").await?;
    let worker = sub.worker(example).preset(s::Preset::Script).start()?;
    let run = sub.start(example, ()).await?;
    run.send("by", 2).await?;
    run.send("stop", ()).await?;
    println!("Final output {}", run.result().await?);
    worker.shutdown().await?;
    Ok(())
}
```

## I-4 A backup

A backup that two people approve within a day, then verified by another workflow.

```python
import asyncio
import substantial as s


async def delete_archive(archive: str, files: list[str], tag: str) -> None: ...
async def delete_upload(url: str, archive: str, target: str) -> None: ...


@s.step
async def list_files(source: str) -> list[str]: ...


@s.step(timeout=s.minutes(10), undo=delete_archive)
async def make_archive(files: list[str], tag: str) -> str: ...


@s.step(retry=s.Retry(max_attempts=5, initial=s.seconds(1), max=s.minutes(1), multiplier=2), undo=delete_upload)
async def upload(archive: str, target: str) -> str: ...


@s.step
async def check_archive(archive: str) -> bool: ...


@s.workflow
async def verify(c: s.Context, archive: str) -> bool:
    return await check_archive(archive)


@s.workflow
async def backup(c: s.Context, source: str, target: str) -> dict:
    tag = await c.uuid()
    files = await list_files(source)
    archive = await make_archive(files, str(tag))
    url = await upload(archive, target)
    c.log.info("archived %d files", len(files))

    approvers: list[str] = []

    def approve(name: str) -> None:
        if name not in approvers:
            approvers.append(name)

    c.handle("approve", approve)
    if not await c.wait_until(lambda: len(approvers) >= 2, timeout=s.days(1)):
        await c.rollback("not approved within a day")

    check = await c.start(verify, {"archive": archive})
    return {"url": url, "approved_by": approvers, "check": await check.result()}


async def main() -> None:
    url = "h3://s3.example.ch/backups/nightly?region=ch-1"
    async with (
        s.Substantial.open(url, credentials="env") as sub,
        sub.worker([backup, verify], preset="script"),
    ):
        run = await sub.start(backup, {"source": "/data", "target": "s3://archive"}, id="backup-2026-10-31")
        await run.send("approve", "alice")
        await run.send("approve", "bob")
        print(await run.result())


asyncio.run(main())
```

- Without two approvals within a day, `rollback` deletes the upload then the archive, most recent first, and the run ends failed with `RolledBack`. `list_files` has no undo: nothing to undo.
- `upload` gets five attempts; each backoff is a suspension, so a worker that restarts meanwhile loses nothing.
- `verify` started from a workflow is a child: a run of its own, whose result `backup` waits for.
- `id=` makes the start idempotent: a cron that fires twice, or a client that retries, starts one backup.
- The day of waiting costs no process: the run is suspended, a due entry holds the timeout.
- `h3://` names an S3-compatible store whose listings can wait, so that workers long-poll for due runs instead of listing every `max_idle` (I-16). On plain S3 the URL starts with `s3://`, and in development `file:` or `memory://` need no server.

```ts
import * as s from "@substantial/substantial";

const deleteArchive = async (archive: string, files: string[], tag: string) => { /* ... */ };
const deleteUpload = async (url: string, archive: string, target: string) => { /* ... */ };

const listFiles = s.step("list_files", async (source: string): Promise<string[]> => { /* ... */ });
const makeArchive = s.step("make_archive", async (files: string[], tag: string): Promise<string> => { /* ... */ },
  { timeout: s.minutes(10), undo: deleteArchive });
const upload = s.step("upload", async (archive: string, target: string): Promise<string> => { /* ... */ },
  { retry: { maxAttempts: 5, initial: s.seconds(1), max: s.minutes(1), multiplier: 2 }, undo: deleteUpload });
const checkArchive = s.step("check_archive", async (archive: string): Promise<boolean> => { /* ... */ });

export const verify = s.workflow("verify", async (c, input: { archive: string }) =>
  checkArchive(input.archive));

export const backup = s.workflow("backup", async (c, input: { source: string; target: string }) => {
  const tag = await c.uuid();
  const files = await listFiles(input.source);
  const archive = await makeArchive(files, tag);
  const url = await upload(archive, input.target);
  c.log.info(`archived ${files.length} files`);

  const approvedBy: string[] = [];
  c.handle<string>("approve", (name) => {
    if (!approvedBy.includes(name)) approvedBy.push(name);
  });
  if (!(await c.waitUntil(() => approvedBy.length >= 2, { timeout: s.days(1) }))) {
    await c.rollback("not approved within a day");
  }

  const check = await c.start(verify, { archive });
  return { url, approved_by: approvedBy, check: await check.result() };
});

await using sub = await s.Substantial.open("h3://s3.example.ch/backups/nightly?region=ch-1",
  { credentials: "env" });
await using _worker = sub.worker([backup, verify], { preset: "script" });
const run = await sub.start(backup, { source: "/data", target: "s3://archive" }, { id: "backup-2026-10-31" });
await run.send("approve", "alice");
await run.send("approve", "bob");
console.log(await run.result());
```

```rust
use serde::{Deserialize, Serialize};
use substantial as s;

#[derive(Serialize, Deserialize)]
struct BackupInput { source: String, target: String }

#[derive(Serialize, Deserialize)]
struct VerifyInput { archive: String }

#[derive(Debug, Serialize, Deserialize)]
struct Report { url: String, approved_by: Vec<String>, check: bool }

async fn delete_archive(archive: String, files: &[String], tag: &str) -> anyhow::Result<()> { /* ... */ }
async fn delete_upload(url: String, archive: &str, target: &str) -> anyhow::Result<()> { /* ... */ }

#[s::step]
async fn list_files(source: &str) -> anyhow::Result<Vec<String>> { /* ... */ }

#[s::step(timeout = s::minutes(10), undo = delete_archive)]
async fn make_archive(files: &[String], tag: &str) -> anyhow::Result<String> { /* ... */ }

#[s::step(retry = s::Retry { max_attempts: 5, initial: s::seconds(1), max: s::minutes(1), multiplier: 2.0 }, undo = delete_upload)]
async fn upload(archive: &str, target: &str) -> anyhow::Result<String> { /* ... */ }

#[s::step]
async fn check_archive(archive: &str) -> anyhow::Result<bool> { /* ... */ }

#[s::workflow]
async fn verify(c: s::Context, input: VerifyInput) -> s::Result<bool> {
    check_archive(&input.archive).await
}

#[s::workflow]
async fn backup(c: s::Context, input: BackupInput) -> s::Result<Report> {
    let tag = c.uuid().await?;
    let files = list_files(&input.source).await?;
    let archive = make_archive(&files, &tag).await?;
    let url = upload(&archive, &input.target).await?;
    c.log.info(format!("archived {} files", files.len()));

    let approvers = s::Var::new(Vec::<String>::new());
    c.handle("approve", approvers.handler(|names, name: String| {
        if !names.contains(&name) { names.push(name) }
    }))?;
    if !c.wait_until(approvers.check(|names| names.len() >= 2)).timeout(s::days(1)).await? {
        return Err(c.rollback("not approved within a day").await);
    }

    let check = c.start(&verify, VerifyInput { archive }).await?;
    Ok(Report { url, approved_by: approvers.get(), check: check.result().await? })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sub = s::Substantial::open("h3://s3.example.ch/backups/nightly?region=ch-1")
        .credentials(s::Credentials::Env).await?;
    let worker = sub.worker((backup, verify)).preset(s::Preset::Script).start()?;
    let input = BackupInput { source: "/data".into(), target: "s3://archive".into() };
    let run = sub.start(backup, input).id("backup-2026-10-31").await?;
    run.send("approve", "alice").await?;
    run.send("approve", "bob").await?;
    println!("{:?}", run.result().await?);
    worker.shutdown().await?;
    Ok(())
}
```

In Rust, a handler and a predicate are `'static` closures: what both use is shared through an `s::Var`, an `Arc<Mutex<_>>` that `var.handler(|v, msg: M| ..)` turns into a handler, for `c.handle` and `Handlers::on`, and `var.check(|v| ..)` into a predicate; `var.get()` copies the value out, `var.with(|v| ..)` reads it in place. `#[s::workflow]` makes the function a value of its name, passed where a workflow is; the handles `sub.start` and `c.start` return are typed by its result, so `result()` needs no annotation. A `#[s::step]` function finds its run through the workflow future polling it (`NotInWorkflow` elsewhere). Its parameters are values or shared references, such as `&str` or `&[String]`: a reference is made owned once at the call, with `ToOwned`, so the step's future borrows nothing; each attempt gets a clone, and its undo gets them after the value, with the step's parameter types.

## I-5 Workflows

```python
@s.workflow                                 # named "backup", no version
async def backup(c: s.Context, source: str, target: str) -> dict: ...

@s.workflow(name="backup", version=2)
async def backup_v2(c: s.Context, source: str, target: str) -> dict: ...
```

| Option | Meaning |
|---|---|
| `name` | the workflow's identity in the store; the function's name unless given; the first argument in TypeScript, where bundlers rename functions |
| `budget` | optional. Limits on the cost of each run of the workflow started without a budget of its own (I-28); TypeScript `{ budget }`, Rust `.with_budget(..)` or `#[s::workflow(budget = ..)]` |
| `retention` | optional. How long the runs of the workflow are kept after their chain ends, instead of the worker's `retention` (I-16, I-18): a duration of at least 1 hour and at most 100 years; TypeScript `{ retention }`, Rust `.with_retention(d)` or `#[s::workflow(retention = ..)]`. A shorter or longer duration raises `ConfigError` when a worker is given the workflow, as do versions of one name giving different retentions to one worker. A continued run takes the retention of its own workflow |
| `step_timeout` | optional. The timeout of every attempt of the workflow's steps and undos that set none, instead of the worker's `step_timeout` (I-18); TypeScript `{ stepTimeout }`, Rust `.with_step_timeout(d)` or `#[s::workflow(step_timeout = ..)]`. A zero duration is refused where the workflow is declared: `InvalidArgument` in Python and TypeScript, a panic in Rust, as for a zero `tokio::time::interval` |
| `version` | optional. A run started with a version is replayed only by the code of that version, so old and new code run side by side through a deploy, the old one served until its runs end. A run without a version is replayed by the code registered under its name; an incompatible change fails it with `NonDeterminism`: at the first call the history does not answer, the host stops the code and the run ends failed |

- The input is one JSON object: Python passes its fields as keyword arguments, TypeScript passes the object, Rust deserializes it into the second parameter's type. A workflow without input takes no second parameter and gets `{}`.
- In Rust that type must be a struct or a map: `#[s::workflow]` refuses a number, a string, a sequence or a tuple when it compiles. Another type that does not deserialize from an object, or any type given to `s::Workflow::new`, compiles, and every start of it raises `InvalidArgument` ("an input is a JSON object, not 5").
- A workflow or step name outside the rules of I-17 raises `InvalidArgument` where it is declared; in Rust `#[s::workflow]` and `#[s::step]` fail to compile, and `s::Workflow::new` is checked at start and by the worker.
- A worker serves the workflows it is given; a workflow name belongs to one language. A worker given only versions of a name leaves the runs of that name without a version to other workers.
- `sub.runs(w)` lists the live runs of a workflow, every version, each with its `version` (I-17): an old version has drained once none of them has it and no client starts it any more, and its code can go.
- `@s.workflow` and `@s.step` take their options in parentheses, or stand bare.
- In Rust, `s::Workflow::new("name", |c: s::Context, input: I| async move { .. })`, `s::Workflow::without_input("name", |c: s::Context| async move { .. })`, `.with_version(n)` and `.with_step_timeout(d)` build a workflow from a closure, as `#[s::workflow]` does from a function.

**Other languages.** Clients and children may be in any language, but every host starts a workflow from a declared one, never from a name alone. To start a workflow that a worker in another language serves, declare a stub: the same name, version and input, a body that never runs. Use it to start runs and children and for `sub.run`, and never give it to a worker, which would serve the name with the stub.

```python
@s.workflow(name="greet")
async def greet(c: s.Context, name: str) -> str:
    raise NotImplementedError("served by the TypeScript worker")

run = await sub.start(greet, {"name": "ada"})
```

```ts
const greet = s.workflow("greet", async (c, input: { name: string }): Promise<string> => {
  throw new Error("served by the Python worker");
});
```

```rust
#[s::workflow(name = "greet")]
async fn greet(_c: s::Context, _input: Greet) -> s::Result<String> {
    unreachable!("served by the Python worker")
}
```

## I-6 Steps

A step is a function whose value is recorded. Declare it with `@s.step`, or wrap code inline on the fly with `c.step`.

```python
@s.step(timeout=s.minutes(10), undo=delete_archive)
async def make_archive(files: list[str], tag: str) -> str: ...

archive = await make_archive(files, tag)                                      # a declared step
total = await c.step("total", lambda: sum(sizes))                             # inline: a name, then a function
tracking = await c.step("ship", lambda: ship(order, address))                 # inline: a lambda calling an async function
size = await c.step("size", os.path.getsize, archive, timeout=s.minutes(1))   # inline: the arguments after the function
a, b = await asyncio.gather(fetch(url_a), fetch(url_b))                       # steps in parallel
sizes = await asyncio.gather(*[c.step("size", os.path.getsize, f) for f in files])
```

| Option | Meaning |
|---|---|
| `name` | the step's identity: for a declared step, the function's bare name unless given, in every host; for an inline step, the first argument of `c.step` |
| `retry` | `s.Retry(max_attempts, initial, max, multiplier)` (TypeScript a plain object `{ maxAttempts, initial, max, multiplier }`, Rust `s::Retry { .. }`), every field required, exponential backoff; none means one attempt |
| `timeout` | per attempt, enforced by the host; it wins over the workflow's and the worker's `step_timeout`, larger or smaller, so every attempt has one, 5 minutes unless set (I-18); a late attempt is interrupted (Abandoned attempts below) and fails with a retryable `StepTimeout` |
| `undo` | a function taking the step's value then its arguments, run by a rollback (Undo, rollback and sagas) |
| `decode` | TypeScript only, as types are erased: a function of the JSON value giving the step's value. Rust decodes with serde into the step's type; Python gives the JSON value |

- **Inline steps.** `c.step(name, fn, *args, retry=, timeout=, undo=)` runs `fn(*args)` as the step `name`. Python binds the arguments given after the function at the call: in a comprehension or a loop, `c.step("ship", ship, order)` keeps each `order`, where `lambda: ship(order)` would read the last one once the step runs. TypeScript takes `c.step(name, fn, options)`, Rust `c.step(name, closure)` and its builder methods `.retry(..)`, `.timeout(..)` and `.undo(..)`. A Rust closure returns a fresh future for each attempt; an `async` block names its error type, as with `anyhow::Ok(x)`: `c.step("double", move || async move { anyhow::Ok(n * 2) })`.
- **Identity.** A step is its name and its occurrence: the first call of `fetch` in a run is `("fetch", 0)`, the second `("fetch", 1)`. The occurrence is taken when the step is called; in Rust, whose futures are lazy, when the step future is first polled. Two step functions with the same name in different modules collide: give one a `name=`.
- **Same value every time.** The value is encoded to JSON and decoded before the code sees it, the first time too, so the first run and every replay see the same value of the same type.
- **Permanent errors.** `raise s.Permanent(...)` (TypeScript `throw new s.Permanent(...)`, Rust `Err(s::Permanent::new(e).into())`) fails the step at once, attempts left or not: a 404, an invalid input.
- **Step key.** `s.step_key()`, called inside a step, returns `<run>/<step>/<occurrence>`, the same for every attempt and replay: pass it to a payment provider, an `Idempotency-Key` header, a database unique key. An undo's is `<run>/<step>.undo/<occurrence>`, the occurrence of the step it undoes.
- **Abandoned attempts.** An attempt the worker abandons, past its timeout or its worker's `shutdown_grace` (I-18), is interrupted: its outcome is not recorded, and the code after the point it stopped at never runs. Python cancels an `async` step at its `await` (`asyncio.CancelledError`), Rust drops the step's future at its `.await`. JavaScript cannot stop a function: TypeScript aborts `s.stepSignal()`, the attempt's `AbortSignal`, and stops awaiting the body, so pass the signal to `fetch`, timers and SDK calls, or the body runs to its end, its outcome dropped. A thread cannot be interrupted either: a Python plain `def` and a Rust `step_blocking` run to their end, their outcome dropped, maybe while the next attempt runs. An interrupted step may leave its effect half done: keep it idempotent, or keyed with `s.step_key()`.
- **Synchronous functions.** Python runs them in a thread (`asyncio.to_thread`), so a blocking call never stops the worker. A thread cannot be stopped: past its timeout, the attempt may still finish while the next one runs, so its effect may happen twice. A synchronous function that returns an awaitable, such as `lambda: ship(order, address)` calling an async function, has it awaited after the call. TypeScript runs them on the event loop, where a blocking call stops the worker until it returns. A Rust step is an `async fn`; blocking code is an inline `c.step_blocking(name, move || -> Result<T, E> { .. })`, which runs on tokio's blocking threads with the same past-the-timeout caveat, or goes through `tokio::task::spawn_blocking`.
- A declared step called outside a workflow raises `NotInWorkflow`.
- **Types.** In Python, a type checker (pyright, mypy) sees a declared step as an `s.Step[P, R]`, called with the function's parameters and awaited to an `R`, and `c.step` checks the arguments given after the function against it. In Rust and TypeScript, the step's value has the function's type.
- **Why a step failed.** `StepFailed` holds `cause`, the last attempt's failure (I-20): `StepTimeout`, `ValueTooLarge`, or the type of the error the step raised.
- **An outage longer than the retry budget.** A step out of attempts raises `StepFailed`, which fails the run if uncaught; `run.retry()` then runs it again from the failed step once the outage is over, its recorded steps kept (I-17). To wait the outage out inside the run instead, catch it, wait for a message, and call the step again, a new occurrence with a fresh budget:

```python
while True:
    try:
        url = await upload(archive, target)
        break
    except s.StepFailed as e:
        c.log.warning("upload failed, waiting for resume: %s", e)
        await c.receive("resume")
```

## I-7 Time, randomness and identifiers

```python
started = await c.now()            # datetime, UTC
x = await c.random()               # float in [0, 1)
tag = await c.uuid()               # UUID; a string in TypeScript and Rust
```

Each is a step named by the core, so a replay returns the recorded value. In Rust, `c.now()` returns a `chrono::DateTime<Utc>` and `c.uuid()` a `String`; in TypeScript, `c.now()` a `Date`. A Python `datetime` or `UUID` is not JSON: keep its `str()` in a loop state, a message, a published value or a result.

## I-8 Sleeping and waiting

```python
await c.sleep(s.hours(1))
await c.sleep_until(datetime(2026, 11, 1, 2, 0, tzinfo=UTC))
note = await c.receive("note")                                       # the next "note"
note = await c.receive("note", timeout=s.hours(1))                   # raises WaitTimeout after an hour
approved = await c.wait_until(lambda: len(approvers) >= 2, timeout=s.days(1))   # False after a day
```

- Durations are `s.seconds(n)`, `s.minutes(n)`, `s.hours(n)`, `s.days(n)` in every host (`s::seconds(n)` in Rust): a `timedelta` in Python, which also takes any `timedelta`; a `Duration` in TypeScript, whole milliseconds, also `s.milliseconds(n)` and `new s.Duration(ms)`; a `std::time::Duration` in Rust, which also takes any.
- A sleep's end is fixed when it first runs, and its firing is recorded, so a replay never sleeps again and a worker with a slow clock replays it the same.
- `wait_until` evaluates its predicate after each message, child end and timer, and returns `True` once it holds, or `False` when its timeout passes first.
- `receive(name, timeout=d)` (TypeScript `c.receive(name, { timeout })`, Rust `c.receive::<T>(name).timeout(d)`) returns the next message of its name, or raises `WaitTimeout` once `d` passed with none: a message's value may itself be `None`, so no value stands for none. It is a `wait_until` on the messages of its name, its timeout fixed when it first runs; caught, the code may wait again. A `receive` with a timeout and one without, of the same name, pending at the same time in one run raise `InvalidArgument`: which one a message goes to would depend on the order of the code's branches.
- A `wait_until`'s timeout is fixed when the call first runs, as a sleep's end: a loop that waits again after each message gets a new timeout from each call, which makes a debounce ([Integration](integration.md#a-durable-debounce)). For a deadline, pass `deadline - await c.now()` once checked not negative: a negative duration raises `InvalidArgument`.
- Several things at once: one predicate over them (`lambda: child.done or bool(stopped)`), or branches with `asyncio.wait`, `Promise.race`, `tokio::select!`.

## I-9 Messages

```python
approvers: list[str] = []
c.handle("approve", approvers.append)               # every "approve" so far, then each new one
await c.wait_until(lambda: len(approvers) >= 2)

c.handle({"add": items.append, "remove": items.remove})   # both names, past ones in arrival order

first = await c.receive("by")                       # the next "by" no receive took yet
await c.send(other_chain, "note", "hello", id="note-1")
await notify(url, callback=c.chain)                 # where to send the answer
```

- A message is a name and a JSON value (`None` when sent without one): `run.send(name, value, id=)` from a client, `c.send(run, name, value, id=)` from a workflow, `run` being a chain id (Rust: `c.send(run, name, value).id(..).await?`). A message sent again with the same `id` is delivered once, within `dedup_window` (I-18). A workflow's send is delivered once, its run crashing after it or not, while workers serve due runs within `dedup_window`.
- A workflow's send raises `RunNotFound` or `RunEnded` at the call, as a client's does: before its commit the replay reads the chain up to its last run, with no write, and records the outcome, so every replay gives the code the same one. A child this run started is accepted before its start exists. The target may still end between that read and the delivery, the same window a client's send has: the message is then never consumed.
- A message sent to a collected chain raises `RunNotFound` (I-16). Within 7 days of the collection it never reaches a new run under the same id.
- `c.chain` (Rust `c.chain()`) is the run's chain id, the one a client gives `sub.run`, the same through continuations: pass it to whoever must send the run a message back.
- A replay consumes every message waiting in the inbox, in order, whether the code asks for it yet or not, and records it. On every replay the code sees the same messages at the same place.
- **Order.** A run consumes its messages in the order of the sender's clock, then in the order one sender sent them: the sends of one client process, ids given or not, and a run's own sends in call order. Across processes within one millisecond, the order is arbitrary.
- **Order is not kept across a continue-as-new.** A message that reaches a run after its last replay, as it continues as new, is forwarded to the next run of the chain, and may be consumed there after a message sent later. Nothing is lost. When order matters across continuations, number the values and let the code sort them.
- **Binding delivers the past first.** `c.handle(name, cb)` first delivers every message of its name consumed so far, in order, then each new one as it is consumed.
- **Binding several names at once.** `c.handle({"add": f, "remove": g})` (TypeScript `c.handle({ add, remove })`, without `decode`; Rust `c.handle_all(s::Handlers::new().on("add", f).on("remove", g))?`) binds them together: their past messages come in the order they arrived, across names, then each new one. Separate `handle` calls deliver each name's past messages grouped, all of the first name's before the second's. In Rust, a name given twice to one `handle_all` raises `InvalidArgument`.
- `receive` takes the next message of its name that no `receive` took yet; handlers see every message.
- In Rust, a handler and a predicate share their state through an `s::Var` (I-4): `c.handle("approve", approvers.handler(|a, name: String| a.push(name)))?` then `c.wait_until(approvers.check(|a| a.len() >= 2)).await?`.
- Names starting with `substantial.` are reserved: binding, receiving or sending one, from a workflow as from a client, raises `InvalidArgument`. The ends a run awaits with `c.result` (I-11) come as such messages, which no handler sees.

## I-10 Undo, rollback and sagas

```python
@s.step(undo=refund)
async def charge(card: str, amount: int) -> str:     # refund(payment_id, card, amount)
    return await psp.charge(card, amount, idempotency_key=s.step_key())

@s.workflow
async def checkout(c: s.Context, card: str, amount: int, items: list[str]) -> str:
    async with c.saga():                             # on an error: undo what ran inside, raise again
        payment = await charge(card, amount)
        await reserve(items)
        await ship(items)

    if await fraud_check(payment):
        await c.rollback("fraud")                    # undo every step so far, end the run
    return payment
```

- `undo=` says how a step is undone: the undo receives the step's value then its arguments, so a step that returns nothing can be undone too. In Rust, an inline step's undo receives the value only and captures what else it needs, `c.step("charge", ..).undo(|id: String| async move { .. })`. It runs as the step `<step>.undo`, with the step's retry and timeout, once.
- `c.rollback(reason)` runs the undos of every step so far, most recent first, then ends the run failed with `RolledBack(reason)`. It does not return.
- `async with c.saga():` runs, on an error inside it, the undos of the steps inside it only, most recent first, then raises the error again. Those steps are then off the stack: a later rollback does not undo them twice.
- An undo with no attempt left stops the rollback: the run fails with `UndoFailed`, naming the step.
- A run whose failure ran undos, a rollback's or a saga's, cannot be `retry`d (I-17): its compensated steps would come back with their recorded results and step keys. Start it under a new id.
- A rollback undoes the steps of its run; earlier runs of a chain are not undone.

## I-11 Starting other workflows

```python
check = await c.start(verify, {"archive": archive})   # a child of this run
await check.send("hurry")
ok = await check.result()                              # raises RunFailed if it failed or was cancelled
await check.cancel("not needed any more")              # the child ends cancelled
```

```ts
const check = await c.start(verify, { archive });
await check.cancel("not needed any more");
```

```rust
let check = c.start(&verify, VerifyInput { archive }).await?;
check.cancel("not needed any more").await?;
```

**A child whose workflow no worker serves waits silently**: no error and no log, the child stays `pending` and its parent suspended on `result()`. Give a worker every workflow a run may start.

- A workflow started from a workflow is a child of the run: the same `c.start` as `sub.start`, the parent recorded in the child's `start`. Its id is derived from the parent's, or given with `id=`; in Rust `c.start(&verify, input).id(..).await?` and `check.send(name, value).id(..).await?`, as `sub.start(..).id(..)` and `run.send(..).id(..)`. `check.chain` (Rust `check.chain()`) is the child's chain id, as `c.chain` is the parent's.
- A child may be a workflow of another language, started through a stub (I-5).
- `c.start(w, input, budget=)` gives the child limits of its own, else it gets its workflow's (I-28); a child's total cost reaches its parent with its end (I-27).
- An `id=` already taken by a run that is not this child, or by a run collected less than 7 days ago (I-17), makes the child end failed with `RunExists`: its `result()` raises `RunFailed`, and the other run is left untouched. Starting two children with the same id from one run raises `InvalidArgument`.
- A child not awaited goes on after its parent ends. A parent that is cancelled cancels its children still running.
- **`child.cancel(reason)`** cancels a child this run started, as a client's `run.cancel` does (I-13): the child raises `Cancelled` where it waits, its undos run, it ends cancelled, and `child.result()` then raises `RunFailed` with the status `cancelled` and the reason. The call returns once the message is sent, without waiting for the child's end. It is recorded like a workflow's send (I-9): every replay gives the same outcome, and the child is cancelled once. A child that ended raises `RunEnded`, a collected one `RunNotFound`; a child whose given `id` another run had taken is never cancelled, its call raising `RunEnded` with the status `failed`. A non-string reason raises `InvalidArgument`.
- `check.done` says whether the child ended, for a `wait_until` over several things; `check.done()` in Rust. It turns true when the run consumes the child's end, at the same place on every replay.

**Awaiting any chain.** `await c.result(chain, timeout=None)` waits for the end of a chain this run did not start, a run a client started or another workflow's child, as `child.result()` waits for a child's:

```python
@s.workflow
async def ship(c: s.Context, order: str) -> str:
    paid = await c.result(f"payment-{order}", timeout=s.days(3))   # the payment run's value
    return await send_parcel(order, paid["amount"])
```

```ts
const paid = await c.result<{ amount: number }>(`payment-${input.order}`, { timeout: s.days(3) });
```

```rust
let paid: Paid = c.result(&format!("payment-{}", input.order)).timeout(s::days(3)).await?;
```

- It returns the chain's value; a chain that ended failed or cancelled raises `RunFailed` with its status, error and reason, as `child.result()` does; a chain that does not exist, or was collected, raises `RunNotFound` (with `collected` and `until`); `timeout=` raises `WaitTimeout` once it passed first, and the call can be made again. A chain that continued is followed to its last run. Waiting on its own chain raises `InvalidArgument`. TypeScript takes `{ timeout, decode }`, Rust `c.result::<T>(chain).timeout(d)`.
- The run registers under the awaited chain, then reads its end: an end written before is answered at once, and one written after tells the run, whose next wakeup resumes the code, as a child's end does. Both are recorded: a replay never reads them again. It costs the waiting run a create and three reads, and every run's end one listing of its watchers ([Architecture](architecture.md#workflow-verbs)).
- **Wanting a value before the other run ends** is not a `result`: the other run sends it, to `c.chain` passed in its input or a message, and the run receives it (`c.receive`), the reply pattern of an actor ([Integration](integration.md#several-pods)).

**Recursion** is a workflow starting itself. Each call is a run of its own, whose history holds its own work and its children's outcomes, never the whole tree; a parent waiting for its children is suspended and costs no process.

```python
@s.workflow
async def backup_dir(c: s.Context, path: str) -> int:
    entries = await list_dir(path)
    await upload_files([e.path for e in entries if not e.is_dir])
    children = [await c.start(backup_dir, {"path": e.path}) for e in entries if e.is_dir]
    return 1 + sum([await child.result() for child in children])
```

A **tail call**, a workflow whose last act is to run itself again, is `c.continue_as_new`: the run ends and the next run of its chain starts with a new input, so it can go on forever.

## I-12 Loops and compression

A run's history grows with every step, and every wakeup reads and replays it. `iterate` keeps it short: at a boundary of the loop, once the history passes the worker's threshold, the run compresses itself. It continues as a new run of its chain carrying the loop's state, and the new run starts the loop from that state with an empty history.

```python
@dataclass
class Walk:
    after: str | None = None
    files: int = 0

@s.workflow
async def backup_bucket(c: s.Context, source: str, target: str) -> int:
    async for w in c.iterate(Walk()):
        page = await list_page(source, after=w.after)        # 1,000 keys
        if not page:
            break
        await copy_files(page, target)
        w.after, w.files = page[-1], w.files + len(page)
    return w.files
```

- The state is a JSON object, anything else raises `InvalidArgument`: in Python a dataclass or a `dict`. `iterate` yields the same object at each iteration, and `break` ends the loop. After the loop, it holds its last value.
- **Only the state crosses a compression**, in every host. A counter or a list kept beside the loop starts again from its value before the loop in the next run: what the loop needs belongs in the state. A state that does not serialize to JSON fails the run with `NotCompressible`, while the full history still exists; in Python, keep the `str()` of a `datetime` or a `UUID` (I-7). A worker with `continue_as_new_after=0` compresses at every iteration (I-19): test a loop with it, so a slip shows before production.
- **A handler's list does not cross a compression.** Messages reach a handler while an iteration runs, during its steps too, after the code read the list: move the list into the state as the iteration's last statement (`state.queue += ops; ops.clear()`), or the messages it holds are lost when the run continues ([Integration](integration.md#a-durable-debounce)).
- Code before the loop runs again in each run of the chain, with the chain's input.
- A loop that never ends is a cron: `c.schedule()` is one, with its config, missed fires and clock changes handled (I-31).
- `await c.continue_as_new(input, workflow=None)` compresses by hand: it ends the run and starts the next of its chain with this input, the same workflow or another. The same workflow keeps the run's version; another runs with the version it was declared with, or none.
- Messages the run has not consumed are carried to the next run, and a message that reaches the run as it continues is forwarded there: none is lost, but order is not kept across the continuation (I-9). A client handle follows the chain: `send` and `result` reach its last run.

## I-13 Cancellation

```python
try:
    await c.wait_until(lambda: bool(done))
except s.Cancelled:
    await c.rollback("cancelled")
```

- `run.cancel(reason)` from a client: the run raises `Cancelled` at every call it is blocked on. A run that never waits, a loop of steps, sees it after its next step is recorded.
- A step attempt already running is not interrupted: its outcome is recorded and returned. A step waiting for its retry raises `Cancelled`. Undos are never cancelled: a rollback or a saga goes on to its end.
- The code may catch it and still run steps or a rollback; the run ends cancelled whatever it does after.
- `await c.cancel(reason)` ends the run cancelled from inside; it does not return.
- A parent cancels a child it started with `await child.cancel(reason)` (I-11), as a client does with `run.cancel`; the outcome is recorded, so a replay never sends it twice.
- A saga that sees `Cancelled` undoes its steps, as for any error.

## I-14 Logs and progress

```python
c.log.info("archived %d files", len(files))     # muted during replay, written once
c.publish("progress", {"done": 12, "total": 40})
```

- `c.log` is the host's logger (`logging` in Python, `tracing` in Rust, the console in TypeScript), muted while a replay walks the recorded history. Logs never enter the history.
- To see logs: Python writes `c.log` to the logger `substantial.workflow` and its own messages to `substantial`, and `logging` shows only warnings until configured (`logging.basicConfig(level=logging.INFO)`). Rust prints nothing, its own errors included, without a `tracing` subscriber: install one, such as `tracing_subscriber::fmt::init()` of the `tracing-subscriber` crate. TypeScript prints to the console.
- A worker logs one warning when it ends a run failed or cancelled, `run <chain> failed: <type>: <message>` or `run <chain> cancelled: <reason>`, once per run, not per replay.
- `c.publish(key, value)` makes a value readable by clients while the run goes on: `run.read(key)` returns the last one published by any run of the chain, or `None`. In Rust, `run.read::<T>(key).await?` returns an `Option<T>`.

## I-15 Large values

Every value is stored in the history. A value whose JSON encoding passes 64 MiB (67,108,864 bytes) is refused with `ValueTooLarge`, its size and the limit in its message:

- in a client, by `start` or `send`, before any store operation;
- in workflow code, a step's value fails the step at once, as a permanent error: `StepFailed`, whose `cause` is `ValueTooLarge`;
- a child's input, a message, a published value or a loop state raises `ValueTooLarge` at its call, and a workflow's result over the cap fails the run with it.

The host checks each value before it reaches the core, and the core checks it again: a call refused records nothing.

The cap bounds each value, not a run's history, which every replay reads whole: many large values in one run, without `iterate`, can exhaust a worker's memory. Keep large data out of values: store it in a step and return its key.

## I-16 Creating and opening a namespace

```python
sub = await s.Substantial.create("h3://s3.example.ch/backups/nightly?region=ch-1",
                                 credentials="env")
sub = await s.Substantial.open("h3://s3.example.ch/backups/nightly?region=ch-1",
                               credentials="env")
```

- `create` makes the namespace, or opens it if it exists. `open` raises `LayoutError` on a namespace that does not exist, so a typo never makes an empty one.
- **A namespace of an older layout**, whose `substantial.json` holds `partitions`, raises `LayoutError` at `create` and `open`, naming its layout and this one. Nothing migrates it: let the version that created it drain it, until its runs have ended, and start this version on a new namespace.
- **Due runs are listed by workflow.** A run's wakeups are kept under its workflow's name, and every worker serving the workflow lists them at each scan, at most `due_page` keys a listing; a claim decides which worker replays a run (I-18). What the listings cost on S3 is in [Architecture](architecture.md#monthly-cost-on-s3).
- Both run the probe of I-26: a store that accepts two creates of one key raises `LayoutError`.
- **Ended runs are collected.** A run is kept while its chain runs and for at least its `retention` after the chain's last run ends, 30 days unless the worker or the workflow says otherwise (I-5, I-18). Then a worker serving its workflow deletes it, no sooner than 2 hours after the run ended: a `retention` of 1 hour collects at about 2 hours. A run is kept longer while a child it started is kept. The first run of a chain that continued keeps its `start` and `result` while the chain lives.
- `retention` is at least 1 hour and at most 100 years (36,500 days). Every ended run is collected once it passes: no setting keeps runs for good, so a namespace stops growing. No retention means the worker's.
- A worker serving the workflow's name collects its runs, whatever their version: keep a worker serving a name while it has ended runs, or they stay. A collection whose checks cannot pass yet, a child still kept or a parent still starting it, tries again a day later.
- A collection comes due without waking anyone: the first scan of a worker serving the workflow after that instant collects the run, up to `max_idle` later (30 s in the `service` preset). The end of a tombstone is served the same way.
- The id of a collected chain stays taken for 7 days, its tombstone (I-17), then starts a new run.
- These hold while no client or worker stalls more than 7 days between reading an id and writing under it, and while workers serve due entries within 7 days.
- Never delete keys under a namespace, by hand or with a lifecycle rule: runs still going read keys of runs that ended, and deleting them by age can make a child run twice.

A store is named by its URL only:

| URL | Store | The namespace is | Waiting listings |
|---|---|---|---|
| `memory://<ns>` | memory, for tests | `<ns>` | no |
| `file:<path>`, `file:///<path>` | file system | the directory | no |
| `s3://<endpoint>/<bucket>/<ns>?region=<r>` | S3 with conditional writes | the prefix | no |
| `h3://<endpoint>/<bucket>/<ns>?region=<r>` | an S3-compatible store whose listings can wait | the prefix | yes |
| `redis://<host>:<port>/<ns>?db=<n>&replicas=<n>` | one Redis or Valkey server | the hash tag `{<ns>}` | yes |
| `redis+sentinel://<host>:<port>,<host>:<port>/<ns>?master=<name>&db=<n>&replicas=all` | Redis or Valkey with Sentinel | the hash tag `{<ns>}` | yes |
| `redis+cluster://<host>:<port>,<host>:<port>/<ns>?replicas=all` | a Redis or Valkey cluster | the hash tag `{<ns>}` | yes |

- An unknown scheme raises `ConfigError` with the supported ones.
- A `file:` path is used as given: one starting with `~` raises `ConfigError`, as nothing expands it to a home directory; give an absolute or relative path.
- Every part shown is required: the endpoint and the region too, on AWS as anywhere else (`s3://s3.eu-central-1.amazonaws.com/<bucket>/<ns>?region=eu-central-1`). `<endpoint>` is a host, with a port if needed, reached over HTTPS; `s3+http://` and `h3+http://` name plain HTTP, for a local server in development. In Python, the endpoint's certificate is verified against the authorities botocore ships with (certifi's when it is installed), as the AWS SDK for Python does, so neither the system's authorities nor `AWS_CA_BUNDLE` are read. The region is named as AWS names them, 1 to 63 lowercase letters, digits and hyphens, neither starting nor ending with a hyphen; another raises `ConfigError` at open.
- `s3://` and `h3://` open the same adapter on the same S3 API: `h3://` only declares that the store's listings can wait, returning once their page changes or after at most 20 s. Workers then long-poll instead of listing every `max_idle`, and `result` sees a run's end as soon as it is written.
- Redis stores need, in Python, the `redis` extra (`pip install "substantial[redis]"`), in Rust, the `redis` feature (`cargo add substantial --features redis`), and in TypeScript, `@redis/client` (`npm install @redis/client`); without it, a Redis URL raises `ConfigError` naming what to install. They need Redis or Valkey 7.2 or later with AOF on and `maxmemory-policy noeviction` (I-26). Every parameter shown is required, and an unknown one raises `ConfigError`. `<host>:<port>` names the server, the sentinels asked for the primary `master`, or the cluster nodes asked for the primary of the namespace's slot. `replicas` is how many replicas fsync a create before it is acknowledged: a number or `all` on one server, and only `all` on Sentinel and Cluster, so a promoted replica holds every acknowledged create; there, a primary with no replica online acknowledges no create. `<ns>` is made of `[A-Za-z0-9._-]` components separated by `/`. `rediss://`, `rediss+sentinel://` and `rediss+cluster://` reach every server over TLS, verified against the default certificate authorities of the host, and no other can be given: in Python, those of OpenSSL's default locations, or of `SSL_CERT_FILE` and `SSL_CERT_DIR`; in Rust, the operating system's, or those of `SSL_CERT_FILE` and `SSL_CERT_DIR`; in TypeScript, Node's, its bundled Mozilla list unless `--use-system-ca`, plus `NODE_EXTRA_CA_CERTS`, or Deno's. Sentinel and Cluster need at least 2 replicas per primary for any operation to go on after one failover.
- `credentials`, required by `s3://` and `h3://`, and refused by `memory` and `file`, is one of:
  - `"env"`, the store's standard variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` when it is set; a missing one raises `ConfigError` at open;
  - explicit keys, read once;
  - a provider, a function, sync or async, returning keys with an optional `expires_at`. It is called at open, and again before any request made less than 5 minutes before the last keys' `expires_at`; keys without one are never refreshed. A provider that raises fails that request as a store failure, and the next request calls it again; one that has not answered after 60 s, the timeout of a store request, fails that attempt, which is retried like a lost connection. Use it for roles, web identity or any temporary keys.
- The URL and `credentials` alone define an S3 store: no other AWS setting is read, neither a profile nor `~/.aws/config` nor variables such as `AWS_USE_FIPS_ENDPOINT`.

```python
s.Credentials(access_key_id="...", secret_access_key="...", session_token=None)

async def role_keys() -> s.Credentials:        # or a plain def, run in a thread
    k = await fetch_role_keys()
    return s.Credentials(access_key_id=k.id, secret_access_key=k.secret,
                         session_token=k.token, expires_at=k.expiration)    # an aware datetime

sub = await s.Substantial.open(url, credentials=role_keys)
```

- On a Redis store, `credentials` is optional: none for a server without `AUTH` (Rust `Credentials::None`), `"env"` for `REDIS_PASSWORD`, and `REDIS_USERNAME` when it is set, a missing or empty `REDIS_PASSWORD` raising `ConfigError` at open, a password, or a provider returning one with an optional `expires_at`. A password is `s.Password(password, username=None)` in Python, `Credentials::Password { password, username }` in Rust and `new s.Password({ password, username })` in TypeScript, the default user without a username. A provider returns `s.Password` in Python, sync or async, `s::Password { password, username, expires_at }` in Rust, async, given as `Credentials::password_provider(f)`, and `{ password, username?, expiresAt? }` in TypeScript, sync or async. It is called at the first connection, and again by any connection opened less than 5 minutes before the last password's `expires_at`; connections already open keep theirs. A provider that raises fails that operation as a store failure, without retries; one that has not answered after 60 s fails that attempt, which is retried. Sentinels are asked for the primary without a password: sentinels that require one are not supported yet.

```python
sub = await s.Substantial.open("redis+sentinel://10.0.0.1:26379,10.0.0.2:26379/shop?master=main&db=0&replicas=all",
                               credentials=s.Password("...", username="shop"))
```

```rust
let sub = s::Substantial::open("redis+sentinel://10.0.0.1:26379,10.0.0.2:26379/shop?master=main&db=0&replicas=all")
    .credentials(s::Credentials::Password { password: "...".into(), username: Some("shop".into()) })
    .await?;
```

```ts
const sub = await s.Substantial.open(
  "redis+sentinel://10.0.0.1:26379,10.0.0.2:26379/shop?master=main&db=0&replicas=all",
  { credentials: new s.Password({ password: "...", username: "shop" }) },
);
```

```ts
new s.Credentials({ accessKeyId: "...", secretAccessKey: "...", sessionToken: undefined })

const sub = await s.Substantial.open(url, {
  credentials: async () => {
    const k = await fetchRoleKeys();
    return { accessKeyId: k.id, secretAccessKey: k.secret, sessionToken: k.token, expiresAt: k.expiration };
  },
});
```

```rust
s::Credentials::S3 { access_key_id: "...".into(), secret_access_key: "...".into(), session_token: None }

let sub = s::Substantial::open(url)
    .credentials(s::Credentials::provider(|| async {
        let k = fetch_role_keys().await?;
        anyhow::Ok(s::Keys {
            access_key_id: k.id,
            secret_access_key: k.secret,
            session_token: Some(k.token),
            expires_at: Some(k.expiration),      // a chrono::DateTime<Utc>
        })
    }))
    .await?;
```

- `prices=` gives the store's prices, so the cost of each run's requests is counted in money (I-27), and `observe=` a function called with each operation, step attempt, replay and scan (I-29); TypeScript `{ prices, observe }`, Rust `.prices(p)` and `.observe(f)`. Both are optional.
- `await sub.close()` closes the store, its connections included. `async with s.Substantial.create(url) as sub:` closes it when the block ends, as does `async with sub:` on one already open; `create` and `open` return an awaitable that is also an async context manager. In TypeScript, `await using sub = await s.Substantial.create(url)` closes it at the end of the scope. Rust has no `close`: dropping the `Substantial` frees it.

## I-17 Starting and following runs

```python
run = await sub.start(backup, {"source": "/data", "target": "s3://archive"}, id="backup-2026-10-31")
await run.send("approve", "alice", id="approve-alice")
result = await run.result(timeout=s.hours(2))   # raises RunFailed, or WaitTimeout after two hours
result = await run.result(max_interval=s.minutes(5))   # polls at most every 5 minutes, not 30 s
status = await run.status()     # pending, running, suspended, completed, failed, cancelled
progress = await run.read("progress")
await run.cancel("not needed")
events = await run.history()    # the events of the last run, each with its segment and time
await run.retry()               # a failed run again, its recorded steps kept; retry(budget=) sets its budget

run = sub.run(backup, "backup-2026-10-31")      # a handle on an existing chain
async for info in sub.runs(backup):             # the live runs of a workflow, every version
    print(info.run, info.version, info.waiting_on, info.due_at)
```

- **Names.** Run and message ids, workflow names and published keys go into store keys: they match `[a-z0-9._-]{1,100}`, `.` and `..` excepted, and names starting with `substantial.` are reserved. Step and message names match `[A-Za-z0-9._-]{1,128}`. Anything else raises `InvalidArgument`, whose message states the rule. Many external ids are case-sensitive, and lowercasing `evt_1AbC` would merge it with `evt_1abc`: hash an external id into a run or message id, as `f"evt-{hashlib.sha256(event_id.encode()).hexdigest()[:32]}"`.
- `start` without `id` gets a ULID from the host, in lowercase. The host's ULIDs are monotonic in the process: one made in the same millisecond as the last, or earlier after the clock went back, is the last plus one. Each `send` and `cancel` draws one too, so a client's messages to a run are consumed in the order sent, ids given or not, except across a continue-as-new (I-9). With an `id` already started for the same workflow, version and input, it returns that run while that run is kept, and completes a start that stopped halfway; with other ones, it raises `RunExists`. From the run's collection and for 7 days after, `start(id)` raises `RunExists` whatever the input, saying the run was collected and until when the id is taken. After that, the id starts a new run. Every host encodes an input as canonical JSON (RFC 8785): keys sorted, numbers written as the doubles they are, so keys in another order, `1.0` for `1`, or the same input from another host is the same input, and a Python worker reads `1.0` back as `1`. An input JSON cannot hold exactly raises `InvalidArgument`: a NaN or an infinity in every host, an integer past 2^53 - 1 in Python and Rust (a TypeScript number is already a double, and a `bigint` is refused), and in TypeScript a string with a lone surrogate. Children's inputs and `continue_as_new`'s are encoded the same way. Start with the fields a redelivery keeps: a delivery counter or a timestamp in the input raises `RunExists` at the first redelivery.
- `status` is `pending` while no worker has taken the run: no replay committed and no claim held. `running` while a worker holds a live claim, `suspended` while the run waits on time or messages, then the run's end. A run whose first replay only waits, for a first message and no timer, commits an empty segment, so an idle actor a worker took reads `suspended`, and one that stays `pending` is not served (I-18). Between two commits of a running replay it may read `suspended` for a moment. A run that continues as new reads as its own state, `running` or `suspended`, until its result exists, then the chain reads as its next run.
- A chain is followed from its first run's `result`: a chain that never continued is read without a listing.
- `result` reads the chain's end, then polls the `result` key of the last run it found, one key per poll, until it exists or the timeout passes, the interval doubling from 10 ms up to 30 s, or up to `max_interval` when given (TypeScript `{ maxInterval }`, Rust `.max_interval(d)`), a positive duration: a longer one costs fewer requests on a long wait and sees the end later ([Architecture](architecture.md#clients)); when that run continued, the next poll follows the chain from it. On a store whose listings wait it holds a waiting listing of that `result` key instead of polling. Every 30 minutes it follows the chain again from its first run, so a wait on a chain that gets collected raises `RunNotFound`.
- A handle is typed by its workflow's result: `run.result()` gives the value the workflow returns, as a `Run<O>` in Rust and TypeScript and an `s.Run[R]` to a Python type checker, which also types a child's `result()` (`s.Child[R]`).
- A handle names a chain, `run.chain` (Rust `run.chain()`): `send`, `result`, `status`, `read` and `cancel` act on the last run, and `result` waits through continuations.
- `send` to a run that ended raises `RunEnded`, to a chain that does not exist `RunNotFound`, from a client as from a workflow (I-9). `send` without `id` gets a ULID.
- A collected chain raises `RunNotFound` from `status`, `result`, `send` and `cancel`, saying it was collected, for 7 days; `read` returns `None`. An ended run never reads as running.
- `result` needs no worker in the process. A worker of this `Substantial` that dies fails the `result` waits in progress with its error (I-18); later waits go on, for a worker elsewhere.
- **`retry` runs a failed chain again, its recorded steps kept.** After fixing the code or once the outage is over, `await run.retry()` starts a new run of the chain that replays the failed run's history without its failure: steps that succeeded return their recorded values without running, the step that failed runs again from its first attempt with a fresh retry budget, and the code goes on. `s.step_key()` stays the failed run's, so a step whose effect was half done keeps its idempotency key. The handle follows the chain to the retry: `result`, `status`, `send` and `cancel` act on it, and `history` reads both. Only a chain whose last run failed without running undos is retried: one that completed, was cancelled or has not ended raises `InvalidArgument`, and so does one whose failure ran undos, a `rollback`'s or a saga's (I-10): the retry would give back the recorded results of steps whose effects were undone, and running them again would reuse their `s.step_key()`, so start such a run under a new id instead. A collected chain raises `RunNotFound`. The retry runs the failed run's workflow version, under the code registered for it now: a history the code no longer matches fails with `NonDeterminism`. Retries of one failed run, concurrent or repeated, start one run, and a retry that fails can be retried in turn. The retry has no parent: a parent already received the child's failure. Starting the same `id` with the same input still returns the failed chain, retried or not; a run started under a new id (`order-42-r1`) runs every step again, with new `s.step_key()`s. For failures the code can wait out, see the catch-and-resume pattern of I-6.
- **Budgets.** `start(..., budget=)` gives the run limits on its cost, over its workflow's (I-28); the budget is part of the start, so the same id with another raises `RunExists`. `run.retry(budget=)` (TypeScript `run.retry({ budget })`, Rust `run.retry().budget(..)`) sets the retry's, else keeps the failed run's; the cost already spent stays the chain's.
- **A workflow awaits a chain** with `c.result(chain)` (I-11), a chain it did not start included, without polling.
- **`runs` lists the live runs of a workflow**, every version: pending, waiting on a timer or a retry's backoff, overdue, or waiting on messages only; ended runs are not. Each is a `RunInfo` (Rust `s::RunInfo`): `chain`, `run` (the chain's run that is live), `version` (none for a run without one), `due_at`, when it next wakes, none when it waits on messages only, and `waiting_on`: `"start"` (started, never run), `"time"` (a timer or a step's retry backoff), `"inbox"` (a message arrived and is due, consumed at its next wakeup) or `"message"` (idle until a message comes, no timer); TypeScript `dueAt` and `waitingOn`, Rust a `WaitingOn` enum. Python and TypeScript iterate with `async for` and `for await`, Rust consumes a `Stream` of `Result<RunInfo>`. A run is read from its workflow's queue, never from the histories: it is listed once, by its earliest wakeup; a run waiting on messages is found by a marker its first such wait created, kept until its end even if it later waits on a timer too. A run that just ended may still be listed for a moment, as may one whose wakeup a crashed worker left, until the next scan. Ended runs, failed ones included, are not listed: keep the ids you start to find those.
- **`history` gives the events of the chain's last run**, the ones its replays read, for inspection: a list of `HistoryEvent` with `run` (the run whose segment recorded it: the last run, or a failed run it retries), `segment`, `at` (when that segment was committed), `kind` and `data`, the event's fields as JSON with times in RFC 3339 and failures as `{"type", "message", "retryable", "details"}`. The kinds are `step` (`name`, `occurrence`, `attempt`, `value`), `step_retry` (with `error` and `at`, when the next attempt starts), `step_failed` (with `error`), `timer` (`occurrence`, `until`), `fired`, `message` (`id`, `name`, `value`, or `ended` for a child's end, `at`), `child` (`occurrence`, `run`, `workflow`, `version`, `input`), `sent` (`occurrence`, `run`, `id`, `name`, `value`, and `to` or `refused`), `published` (`key`, `value`), `budget_exceeded` (`meter`, `limit`, `used`, I-28), and the end: `completed` (`value`), `failed` (`error`), `cancelled` (`reason`) or `continued` (`run`, `workflow`, `version`, `input`, `state`). A value of bytes reads `{"$bytes": "<base64>"}`. A chain that continued shows its last run only. A collected chain raises `RunNotFound`, as `status` does.

## I-18 Workers

```python
async with sub.worker([example], preset="script"):                             # inside a script
    ...

await sub.worker([backup, verify], preset="service", concurrency=32).serve()   # a long-lived process
```

A worker runs the workflows it is given. A preset is a named set of every setting, the only defaults there are; one of the seven public settings given next to it overrides it, and any other name is refused with `ConfigError: unknown settings [...]`.

- **A run whose workflow no worker serves waits silently**, a child (I-11) as a run started by a client: it stays `pending`, with no error and no log. Give a worker every workflow a run may start.
- **A run started or messaged from another process waits up to `max_idle`** on a store whose listings do not wait (`file:`, `s3://`): 30 s in the `service` preset, 1 s in `script`. A start or send in the worker's own process wakes it at once, to list that run's workflow alone, and on `h3://` the worker's waiting listing returns as soon as the start or message lands, even behind runs the worker skips (another version, another worker's claim). Lower `max_idle` for a lower latency, at the cost of more listings.
- **Idle scans are spread out.** Each worker draws a phase below `max_idle` when it starts: its first idle wait is that phase, every later one `max_idle`, so workers started together do not list in step, and an idle worker still lists once per `max_idle`. Prefer fewer workers with more `concurrency`: each worker lists every workflow it serves at each scan, and every worker that knows a run's timer scans when it comes due, though one replays the run ([Architecture](architecture.md#several-workers)).
- **Every step attempt has a timeout.** A worker renews its claim while it lives, not while the step makes progress, so a hung step would keep the run on that worker. Its attempt fails with `StepTimeout` after the step's own `timeout`, else its workflow's `step_timeout`, else the worker's, 5 minutes in both presets: give longer steps a `timeout` of their own. A thread cannot be stopped: a Python synchronous step or a Rust `step_blocking` past its timeout may still finish, its result discarded.
- **The runs of a worker that crashed are taken over** within `claim_ttl` plus `claim_skew` of its last claim or renewal, 35 s in both presets, by the workers serving them.
- **A worker that dies is loud.** When its loop dies, from a bug or a trapped core, it logs the error at once, and the `result` waits in progress through the same `Substantial` fail with it. Later waits are not failed: `result` needs no worker in the process.
- **Health.** `worker.alive` is whether its loop runs, started and neither left nor dead, and `worker.error` the error that killed it, else `None`: what a readiness probe reads. TypeScript has `worker.alive` and `worker.error`, Rust `worker.is_alive()` and `worker.error()`.

| Setting | `script` | `service` | Meaning |
|---|---|---|---|
| `concurrency` | 1 | 16 | runs replayed at once |
| `max_idle` | 1 s | 30 s, 5 min on a store whose listings wait | longest time between two listings; the first idle wait is the worker's phase, below it |
| `shutdown_grace` | 5 s | 25 s | steps in flight may end within it when the worker leaves |
| `claim_ttl` | 30 s | 30 s | a claim not renewed for this long may be taken over |
| `continue_as_new_after` | 1,000 segments or 8 MiB | 1,000 segments or 8 MiB | compression threshold, whichever the history passes first |
| `step_timeout` | 5 min | 5 min | the timeout of every step attempt and undo whose step and workflow set none (I-6) |
| `retention` | 30 days | 30 days | how long ended runs are kept after their chain ends, at least 1 hour and at most 100 years (36,500 days), for the workflows that set none (I-5, I-16) |

`retention` is bounded at 100 years so that a collection's time stays a 4-digit year in its due key and within every host's clock; a longer duration raises `ConfigError`.

`continue_as_new_after` takes a `s.ContinueAsNewAfter(segments, bytes)`, or `0` to compress at every boundary. In TypeScript it is a plain object `{ segments, bytes }`, and the settings are `camelCase` beside the preset: `sub.worker([backup, verify], { preset: "service", concurrency: 32 })`.

The presets fix every other value, the same in both, and no worker can change them:

| Value | Preset | Meaning |
|---|---|---|
| `commit_every` | 0 s | each step is recorded as it returns |
| `claim_skew` | 5 s | clock difference tolerated between workers |
| `wait` | 20 s | the longest wait of a waiting listing |
| `due_page` | 1000 | due entries listed per workflow and page |
| `due_pages` | 10 | pages per listing round, when the first pages hold only entries the worker skips (another version, another worker's claim) |
| `snapshot_every` | 64 segments | a snapshot of the history every this many segments |
| `dedup_window` | 7 days | how long a consumed message id is kept at least, from its consume time and across continuations: a resend with the same id within it is delivered once. A run's history keeps every id the run consumed, so a resend after it is delivered again only once the run has continued as new since (I-19), the next run carrying only the ids within the window |
| `tombstone` | 7 days | how long the id of a collected chain stays taken (I-17) |
| `retention_min` | 1 hour | the shortest `retention` |
| `recheck` | 1 day | when a collection whose checks failed tries again (I-16) |
| store retries | 5 attempts, 100 ms to 5 s | an adapter's retries of a transient failure (I-26) |

**A worker woken on a schedule**, such as one a Kubernetes CronJob runs ([Integration](integration.md#a-worker-run-by-a-kubernetes-cronjob)), drains instead of serving:

```python
await sub.worker([nightly, report], preset="script").drain(within=s.minutes(2))
```

```ts
await sub.worker([nightly, report], { preset: "script" }).drain({ within: s.minutes(2) });
```

```rust
sub.worker((nightly, report)).preset(s::Preset::Script).drain(s::minutes(2)).await?;
```

- `drain(within=)` serves every run due now or within `within` of the call: short sleeps, retry backoffs and timeouts that end by then, the children and messages its own runs create, and the runs of a crashed worker whose claim expires by then. It returns once nothing it knows of is due by then and nothing is in flight, stopping as `shutdown()` does, and raises the worker's error if the worker died. It starts the worker if it was not; on a started one (Rust `worker.drain(d)`), it drains from its next scan.
- A run due after the horizon, a collection not yet due, and work another process creates meanwhile are left to the next drain. A draining worker makes no idle listing and holds no waiting listing: between two drains, a namespace costs nothing ([Architecture](architecture.md#a-worker-run-on-a-schedule)).
- `within` is a duration, not negative, and required: anything else raises `InvalidArgument`.
- **Step timeouts against the job's lifetime.** A step still running when the job ends is abandoned (I-6) and runs again at the next drain, and one that runs longer than the job's lifetime never ends, at every drain. The worker cannot know the job's lifetime: keep every step's timeout, its own, its workflow's `step_timeout` and the worker's, below `within` plus `shutdown_grace`, or give the job a longer lifetime.

Leaving `async with` (TypeScript `await using`), `worker.shutdown()`, or a SIGTERM or Ctrl-C (SIGINT) to `serve()` starts no new replay and lets the steps in flight end within `shutdown_grace`, in every host, then interrupts those still running (I-6); Rust on Windows listens to Ctrl-C only. A run the worker reaches meanwhile is left with its claims deleted and its wakeup kept, for another worker. Outside `serve()` no host handles signals: Ctrl-C ends a Rust or TypeScript script at once, without the grace, while Python's `asyncio.run` cancels the script, which leaves `async with` and shuts the worker down.

In Rust, `sub.worker((backup, verify)).preset(s::Preset::Service).set("concurrency", 32)` overrides a public setting by name, its value written with a unit (`"100ms"`, `"30s"`, `"5min"`), `continue_as_new_after` as `"1000,8MiB"` or `"0"`, and `retention` as `"30d"`; `start()` or `serve()` refuses a worker without a preset, an unknown setting or a value that does not parse.

## I-19 Testing

`s.testing.Env` is a `Substantial` on a namespace in memory and a virtual clock, whose workers run in the test's process and move only when the test says so: a month of timers in a fraction of a second, a crash, a paused pod, a store outage or Chaos in one call. See [Testing workflows](testing.md).

- `Env` takes the `prices` and `observe` options of `Substantial.create` (I-27, I-29).
- **One event loop.** In Python, the environment belongs to the event loop that opened it: a call from another loop, such as an app a thread-portal test client runs, raises `InvalidArgument` naming both loops, instead of hanging; in Rust, the same for another tokio runtime. Run the app on the test's loop ([Testing](testing.md#testing-an-asgi-app)). TypeScript has one event loop.
- `worker.drain(within=)` on an environment's worker moves the virtual clock from one due instant to the next until the worker stopped.
- Outside it, a test runs on the real clock: `memory://<ns>` holds a namespace in the process, shared by name, so give each test its own.
- `continue_as_new_after=0` compresses at every `iterate` boundary, so a loop that is not compressible fails in the test.

## I-20 Errors

The same kinds in every host: Python and TypeScript classes, variants of `substantial::Error` in Rust. In Python and TypeScript every error of this table derives from `SubstantialError`, control flow excepted: `Cancelled`, `RolledBack` and `ContinuedAsNew` derive from `Control` in TypeScript (`isControl(e)`) and from `BaseException` in Python. A failure, as a history records it, is a `Failure`: `type` (Rust `kind`), `message`, `retryable` and `details`, the JSON object the core recorded with it, or `None` (TypeScript `undefined`): a plain `dict` in Python, a `Record<string, unknown>` in TypeScript, an `Option<serde_json::Value>` in Rust. `BudgetExceeded` holds `meter`, `limit` and `used`, integers (a `bigint` in TypeScript); `StepFailed` and `RunFailed` hold what the table says.

| Error | Raised by | Meaning |
|---|---|---|
| `StepFailed` | a step | no attempt left, or a permanent error; holds `failure` (Rust `error`), of type `StepFailed`, whose message names the step, its attempts and the last error, and `cause`, the last attempt's `Failure`: `StepTimeout` when it timed out, `ValueTooLarge` when its value passed 64 MiB, else the type of the error the step raised (`Error` in Rust), `retryable` false for a permanent error |
| `Cancelled` | calls blocked when the run is cancelled | control flow |
| `RolledBack` | `rollback` | the undos ran; the run ends failed with this reason; control flow |
| `UndoFailed` | `rollback`, `saga` | an undo had no attempt left; holds `step`, the undo's step `<step>.undo`, and `failure` (Rust `error`), its last attempt's `Failure` |
| `RunFailed` | `run.result()`, a child's `result()`, `c.result(chain)` | the run ended failed or cancelled; holds `run`, `status`, `error`, a `Failure`, and `cause`. For a run a step failed, `error` is the step's `StepFailed` and `cause` its `StepFailed.cause`, the last attempt's `Failure`, so a client or a parent matches `cause.type` (Rust `cause.kind`) without reading messages; for any other end `cause` is `None` (TypeScript `undefined`). In Rust `error` is a `Box<Failure>` and `cause` an `Option<Box<Failure>>`, as `StepFailed`'s `cause` is a `Box<Failure>` |
| `BudgetExceeded` | calls in a workflow past its budget | a limit of the run's budget was passed (I-28): `meter`, `limit` and `used`; raised at the calls the run waits on and at every later step, child start and send; the run ends failed with this type |
| `NonDeterminism` | a replay | the code made a call the history does not answer; the run fails, and its `RunFailed` adds that an incompatible change needs a new version (`version=`) |
| `NotCompressible` | `iterate` | the loop's state does not serialize to JSON at a compression; the run fails. Rust has no such variant: a state serde cannot encode is a `Json` error, and fails the run the same way |
| `ValueTooLarge` | `start`, `send`, a workflow's calls that take a value | a value whose JSON encoding passes 64 MiB (I-15); its message, and in Rust its fields `size` and `limit`, give both; a step's value over it fails the step with `StepFailed`, its `cause` `ValueTooLarge` |
| `NotInWorkflow` | a declared step, `s.step_key()`, `s.charge` | called outside a workflow, or outside a step; Rust's `s::step_key()` and `s::charge` panic there instead, as a misuse the code cannot handle |
| `RunExists` | `start` | the id is taken by another workflow or input, or by a run collected less than 7 days ago: then `collected` is true and `until` says when the id is free (Rust `RunExists { id, collected, until }`); for a child, the error of its end (I-11) |
| `InvalidArgument` | `start`, `send`, `cancel`, `status`, `result`, `read`, `history`, `runs`, `retry`, `worker`, `drain`, `schedule`, `s.Cron`, `s.charge`, a workflow's calls, declarations (`s.Retry`, checked by Rust at the step's call; `@s.workflow`, `@s.step`), durations | a name or id outside the naming rules (I-17), a reserved name, an input or a loop state that is not an object, a step of a saga that ended, an option or duration of the wrong type or range, a `retry` of a chain whose last run did not fail, or whose failure ran undos, or retried already with another budget; a budget's malformed meter or limit, an `s.charge` amount out of range (I-27, I-28); a cron or a time zone that is not valid, a step named `schedule.next` (I-31); a `c.result` of the run's own chain; raised in a workflow, it fails the run |
| `RunEnded` | `send`, `cancel`, a workflow's `send`, `child.cancel` | the chain's last run ended, its message naming how (`completed`, `failed`, `cancelled`), Rust `RunEnded { run, status }`; a workflow's send records it, so every replay raises it again |
| `RunNotFound` | `send`, a workflow's `send`, `child.cancel`, `c.result`, `cancel`, `status`, `result`, `history`, `retry` | no chain has this id; a chain collected less than 7 days ago has `collected` true and `until`, as `RunExists` (Rust `RunNotFound { chain, collected, until }`) |
| `WaitTimeout` | `run.result(timeout=)`, `c.receive(timeout=)`, `c.result(timeout=)` | the timeout passed first; raised in a workflow and not caught, it fails the run |
| `StoreError` | adapters | the store failed after the adapter's retries; in a worker, the wakeup stays and the run is tried again |
| `LayoutError` | `create`, `open` | no namespace, a namespace of an older or unknown layout, a store that failed the probe |
| `ConfigError` | `create`, `open`, `worker`, `Credentials`, `ContinueAsNewAfter`, `Prices`, a credentials provider's result, `sub.schedule` | a missing, unknown or invalid setting: no preset, an unknown URL scheme, missing credentials or credentials given to a store that takes none, a setting that is not one of the seven public ones, a value of the wrong type, a `retention` under 1 hour, versions of a workflow giving different retentions, an unknown price table or an invalid price; a schedule's revision lower than the one applied, or the same with another config (I-31) |

In Rust a failure's type is the field `kind` of `Failure`, `type` being a keyword. A workflow fails with its own error by returning `Err(s::Error::failed("Refused", "not today"))`, or through `?` on an `anyhow::Error`: its message and causes become the failure's, with the type `Error`, and an `anyhow::Error` that holds a `substantial::Error` gives it back, control flow included. `substantial::Error` also has variants for the host's own failures:

| Variant | Meaning |
|---|---|
| `Failed` | a workflow's own failure, or a call resolved with an error other than a step's |
| `Core` | a core error other than non-determinism and layout: `INVALID`, `ENDED`, `CORRUPT` |
| `Trap` | the core trapped, a bug of the core; its instance is dropped |
| `Module` | the core module could not be loaded: checksum, compilation, missing export |
| `Protocol` | the core answered what the host does not expect at that point |
| `Json` | a value that does not serialize or deserialize |
| `Io` | the host's own I/O outside a store, such as listening for signals |

A worker leaves a run of a version it does not serve, or without a version when it serves only versions of its name, to other workers.

A suspension is not an error, and only Python's code sees it. When a replay ends, Python cancels the code's task: `asyncio.CancelledError`, a `BaseException`, is raised at the `await` the code is blocked on, so `finally` blocks and `async with` exits run at every suspension. Code that catches it and goes on gets a `ProtocolError` at its next call: let it through, and never catch `BaseException` in a workflow. `asyncio.gather(..., return_exceptions=True)` lets it through: the host cancels every task of the code, the gather's included, and a gather that is cancelled raises it rather than returning it. TypeScript leaves the code's promises pending, and Rust drops its future: nothing reaches the code.

## I-21 Actions in each host

| Action | Python | TypeScript | Rust |
|---|---|---|---|
| workflow | `@s.workflow`, `@s.workflow(name=, version=, budget=, retention=, step_timeout=)` | `s.workflow("name", fn, { version, budget, retention, stepTimeout })` | `#[s::workflow]`, `#[s::workflow(name = .., version = .., budget = .., retention = .., step_timeout = ..)]` |
| declared step | `@s.step`, `@s.step(name=, retry=, timeout=, undo=)` | `s.step("name", fn, { retry, timeout, undo, decode })` | `#[s::step]`, `#[s::step(name = .., retry = .., timeout = .., undo = f)]` |
| inline step | `await c.step("name", fn, *args, retry=, timeout=, undo=)` | `await c.step("name", fn, { retry, timeout, undo, decode })` | `c.step("name", move \|\| async move { .. }).await?`, `c.step_blocking("name", move \|\| ..).await?` |
| parallel | `await asyncio.gather(a(), b())` | `await Promise.all([a(), b()])` | `tokio::try_join!(a(), b())?` |
| permanent error | `raise s.Permanent(...)` | `throw new s.Permanent(...)` | `Err(s::Permanent::new(e).into())` |
| step key | `s.step_key()` | `s.stepKey()` | `s::step_key()` |
| charge | `s.charge("USD", nano)` | `s.charge("USD", nano)`, a `bigint` | `s::charge("USD", nano)?` |
| cost | `await c.cost()` | `await c.cost()` | `c.cost().await?` |
| abandoned attempt | `asyncio.CancelledError` at its `await` | `s.stepSignal()` aborted | the step's future dropped at its `.await` |
| now, random, uuid | `await c.now()`, `await c.random()`, `await c.uuid()` | `await c.now()`, `await c.random()`, `await c.uuid()` | `c.now().await?`, `c.random().await?`, `c.uuid().await?` (a `String`) |
| send | `await c.send(chain, "note", value, id=)` | `await c.send(chain, "note", value, { id })` | `c.send(chain, "note", value).id(..).await?` |
| sleep | `await c.sleep(s.hours(1))`, `await c.sleep_until(t)` | `await c.sleep(s.hours(1))`, `await c.sleepUntil(t)` | `c.sleep(s::hours(1)).await?`, `c.sleep_until(t).await?` |
| receive | `await c.receive("by")`, `await c.receive("by", timeout=)` | `await c.receive<T>("by", { decode, timeout })` | `c.receive::<T>("by").await?`, `c.receive::<T>("by").timeout(d).await?` |
| handle | `c.handle("note", cb)`, `c.handle({"add": f, "remove": g})` | `c.handle<T>("note", cb, { decode })`, `c.handle({ add, remove })` | `c.handle("note", move \|v: T\| ..)?`, `c.handle("note", var.handler(\|v, m: T\| ..))?`, `c.handle_all(s::Handlers::new().on("add", f).on("remove", g))?` |
| chain id | `c.chain` | `c.chain` | `c.chain()` |
| wait until | `await c.wait_until(lambda: .., timeout=)` returns a bool | `await c.waitUntil(() => .., { timeout })` returns a boolean | `c.wait_until(move \|\| ..).timeout(d).await?`, `c.wait_until(var.check(\|v\| ..))`, returns a `bool` |
| saga | `async with c.saga(): ..` | `await c.saga(async () => { .. })` | `c.saga(\|c\| async move { .. }).await?`, declared steps awaited inside, inline steps called on that `c` |
| rollback | `await c.rollback("reason")` | `await c.rollback("reason")` | `return Err(c.rollback("reason").await)` |
| start | `child = await c.start(verify, {"archive": a}, id=)`, `child.chain`, `await sub.start(...)` | `const child = await c.start(verify, { archive }, { id })`, `child.chain`, `await sub.start(...)` | `let child = c.start(&verify, input).id(..).await?`, `child.chain()`, `child.result().await?`, `child.done()`, `sub.start(w, input).id(..).await?` |
| await a chain | `await c.result(chain, timeout=)` | `await c.result<T>(chain, { timeout, decode })` | `c.result::<T>(chain).timeout(d).await?` |
| cancel a child | `await child.cancel("reason")` | `await child.cancel("reason")` | `child.cancel("reason").await?` |
| iterate | `async for state in c.iterate(State())` | `for await (const state of c.iterate(new State()))` | `c.iterate(State::default(), \|c, state\| async move { Ok(ControlFlow::Continue(state)) }).await?`, `Break(state)` ends |
| continue as new | `await c.continue_as_new(input, workflow=)` | `await c.continueAsNew(input, { workflow })` | `return Err(c.continue_as_new(input).await)`, `continue_as_new_to(&w, input)` |
| cancel | `await c.cancel("reason")` | `await c.cancel("reason")` | `return Err(c.cancel("reason").await)` |
| log | `c.log.info(...)` | `c.log.info(...)` | `c.log.info(...)` |
| publish | `c.publish("progress", value)` | `c.publish("progress", value)` | `c.publish("progress", &value)?` |
| create, open | `await s.Substantial.create(url, credentials=, prices=, observe=)`, `open(url, credentials=, prices=, observe=)`, `await sub.close()`, `async with s.Substantial.create(..) as sub` | `await s.Substantial.create(url, { credentials, prices, observe })`, `open(url, { credentials, prices, observe })`, `await sub.close()`, `await using sub = ..` | `s::Substantial::create(url).credentials(c).prices(p).observe(f).await?`, `open(url)..`, no `close` |
| handle | `run = sub.run(w, chain)`, `run.chain` | `const run = sub.run(w, chain)`, `run.chain` | `let run = sub.run(&w, chain)`, `run.chain()` |
| client send, cancel | `await run.send("note", value, id=)`, `await run.cancel("reason")` | `await run.send("note", value, { id })`, `await run.cancel("reason")` | `run.send("note", value).id(..).await?`, `run.cancel("reason").await?` |
| status, read | `await run.status()`, `await run.read("progress")` | `await run.status()`, `await run.read<T>("progress")` | `run.status().await?`, `run.read::<T>("progress").await?` |
| live runs | `async for info in sub.runs(w)` | `for await (const info of sub.runs(w))` | `let mut runs = sub.runs(&w); while let Some(info) = runs.try_next().await? { .. }` |
| history, retry | `await run.history()`, `await run.retry(budget=)` | `await run.history()`, `await run.retry({ budget })` | `run.history().await?`, `run.retry().budget(..).await?` |
| budget | `@s.workflow(budget=)`, `sub.start(.., budget=)`, `c.start(.., budget=)` | `{ budget }` on `s.workflow`, `sub.start`, `c.start` | `#[s::workflow(budget = ..)]`, `.budget(..)` on `sub.start` and `c.start` |
| poll | `await c.poll("name", fn, *args, every=, timeout=, retry=)` | `await c.poll("name", fn, { every, timeout, retry, decode })` | `c.poll("name", move \|\| async move { .. }).every(d).timeout(d).await?` |
| schedule | `async for at in c.schedule()`, `await sub.schedule(w, id, s.Cron(expr, tz), revision=, backfill=)` | `for await (const at of c.schedule())`, `await sub.schedule(w, id, new s.Cron(expr, tz), { revision, backfill })` | `c.schedule(\|c, at\| async move { Ok(ControlFlow::Continue(())) }).await?`, `sub.schedule(w, id, Some(s::Cron::new(expr, tz)?), revision).backfill(n).await?` |
| result | `await run.result(timeout=, max_interval=)` | `await run.result({ timeout, maxInterval })` | `run.result().timeout(d).max_interval(d).await?` |
| drain | `await worker.drain(within=)` | `await worker.drain({ within })` | `worker.drain(d).await?`, `sub.worker(w).preset(p).drain(d).await?` |
| worker | `async with sub.worker([a, b], preset=)`, `.serve()`, `await w.shutdown()`, `w.alive`, `w.error` | `await using w = sub.worker([a, b], { preset })`, `.serve()`, `await w.shutdown()`, `w.alive`, `w.error` | `sub.worker((a, b)).preset(p).start()?`, `.serve().await?`, `.shutdown().await?`, `.is_alive()`, `.error()` |

In Rust, a workflow ends early only by returning `Err` of `rollback`, `continue_as_new` or `cancel`, which never return `Ok`; every other context call propagates with `?`. `publish`, which the core answers at once, is not `async`.

## I-27 Cost and charges

```python
@s.step(retry=s.Retry(max_attempts=3, initial=s.seconds(1), max=s.seconds(10), multiplier=2.0))
async def transcribe(audio: str) -> str:
    result = await speech_api.transcribe(audio, idempotency_key=s.step_key())
    s.charge("USD", result.billed_seconds * 400_000)        # this attempt's spend, in nano-dollars
    return result.text

@s.workflow
async def podcast(c: s.Context, audio: str) -> str:
    text = await transcribe(audio)
    cost = await c.cost()
    c.log.info("%d store requests, %s nano-USD so far", cost.requests, cost.amounts.get("USD", 0))
    return text

sub = await s.Substantial.open(url, credentials="env", prices=s.Prices("aws-s3-standard-us-east-1-2026-11-09"))
```

```ts
const transcribe = s.step("transcribe", async (audio: string) => {
  const result = await speechApi.transcribe(audio, { idempotencyKey: s.stepKey(), signal: s.stepSignal() });
  s.charge("USD", BigInt(result.billedSeconds) * 400_000n);
  return result.text;
});

const cost = await c.cost();   // cost.requests: number, cost.amounts.USD: bigint
const sub = await s.Substantial.open(url, { credentials: "env", prices: new s.Prices("aws-s3-standard-us-east-1-2026-11-09") });
```

```rust
#[s::step]
async fn transcribe(audio: String) -> anyhow::Result<String> {
    let result = speech_api::transcribe(&audio, &s::step_key()).await?;
    s::charge("USD", result.billed_seconds * 400_000)?;
    Ok(result.text)
}

let cost = c.cost().await?;   // cost.requests: u64, cost.amounts["USD"]: u64
let sub = s::Substantial::open(url).credentials(s::Credentials::Env).prices(s::Prices::named("aws-s3-standard-us-east-1-2026-11-09")?).await?;
```

- **`c.cost()`** returns an `s.Cost`, the chain's cost so far: `create`, `get`, `list`, `delete` and `requests` (their sum), the store requests counted, and `amounts`, nano-units by currency; with the breakdown `store` (the chain's own requests and their money), `steps` (each step name's charges) and `children` (the totals of the children whose end the run received), each an `s.Meters` of the same fields but `steps`, a mapping of step names to amounts. The top-level fields add up the three. Like `c.now()` it is recorded: a replay gets the value the first run saw.
- **Store requests** are counted by the core for each run, the adapter's retries included: what its wakeups read and write, its commits and their effects, its claims, a workflow's send's reads. A worker's scans, the collection of ended runs and client calls (`start`, `send`, `result`, ...) belong to no run: count those by the listings of [Architecture](architecture.md) or with `observe` (I-29).
- **A lower bound.** Each history segment records the requests made up to it, those of its own commit included as planned, each once. A request the plan did not foresee, an adapter's retry or a read a race adds during a commit, is recorded by the run's next segment only if the same worker writes it; a wakeup that wrote nothing, a duplicate wakeup, is recorded the same way; the children a cancelled run cancels are not counted, nor a wakeup after the run's end, which only redoes its effects. Otherwise the counts are exact: the rows of [Architecture](architecture.md) are what a run records.
- **Money for the store** needs `prices` on `Substantial.create` or `open`: one currency and integer nano-units per 1,000 create, get, list and delete requests, a request costing its share rounded up. `s.Prices("aws-s3-standard-us-east-1-2026-11-09")` is AWS S3 Standard in US East (N. Virginia) from the AWS price list of that date: USD, create and list 5,000,000, get 400,000, delete 0 (5,000 nano-dollars a PUT or LIST, 400 a GET). Override any field by name, `s.Prices("aws-...", get=500_000)` (TypeScript `new s.Prices("aws-...", { get: 500_000n })`, Rust `s::Prices { get: 500_000, ..s::Prices::named("aws-...")? }`), or give all five, `s.Prices(currency="EUR", create=..., get=..., list=..., delete=...)`. An unknown table, a missing or invalid field raises `ConfigError`; a currency outside ISO 4217 raises it when a worker starts. Without `prices`, requests are counted and no store money is: memory, file and Redis stores simply open without it. Each worker prices the requests it counts with its own prices, so totals never go down when workers' prices differ.
- **Charges.** `s.charge(currency, nano)` inside a step (TypeScript `s.charge`, Rust `s::charge`) adds to its attempt's spend: an ISO 4217 code and an integer amount in nano-units (10^-9 of the currency), 0 to 2^63-1: a Python `int`, a TypeScript `bigint` (a `number` raises `InvalidArgument`), a Rust `u64` (`Err(InvalidArgument)` past 2^63-1). A negative or larger amount raises `InvalidArgument` at the call; a code outside ISO 4217, or a charge that would take the chain's total past 2^63-1, fails the step at once, a permanent `InvalidArgument` (`StepFailed` with that cause). The spend is recorded with the attempt's outcome, failed and timed out attempts' too, and `history` shows it as the event's `charges`. Currencies are never mixed nor converted. Outside a step, `s.charge` raises `NotInWorkflow` (Rust panics, as `s::step_key()` does).
- **Children.** A child's total reaches its parent with its end: the parent's `children`, and its `requests` and `amounts`, include it from the place the run received it.
- **Chains.** A continued run carries its chain's total to the next run, and a retry (I-17) the failed run's: `c.cost()` is the chain's, not the run's.
- **Amounts** are integers in every host: Python `int`, TypeScript `bigint`, Rust `u64`. In the JSON values a history gives, they are decimal strings, so no host rounds them.

## I-28 Budgets

```python
@s.workflow(budget={"requests": 2_000, "USD": 5_000_000_000})      # 2,000 store requests, $5
async def podcast(c: s.Context, audio: str) -> str:
    try:
        return await transcribe(audio)
    except s.BudgetExceeded as e:
        c.publish("stopped", {"meter": e.meter, "used": str(e.used)})
        raise

run = await sub.start(podcast, {"audio": url}, budget={"USD": 20_000_000_000})   # this run's own
await run.retry(budget={"USD": 50_000_000_000})                                   # the cost spent kept
```

```ts
const podcast = s.workflow("podcast", async (c, input: { audio: string }) => { /* ... */ }, {
  budget: { requests: 2_000n, USD: 5_000_000_000n },
});
const run = await sub.start(podcast, { audio: url }, { budget: { USD: 20_000_000_000n } });
await run.retry({ budget: { USD: 50_000_000_000n } });
```

```rust
#[s::workflow(budget = [("requests", 2_000), ("USD", 5_000_000_000)])]
async fn podcast(c: s::Context, input: Podcast) -> s::Result<String> { /* ... */ }

let run = sub.start(&podcast, input).budget([("USD", 20_000_000_000)]).await?;
run.retry().budget([("USD", 50_000_000_000)]).await?;
```

- **A budget** is a mapping of meters to integer limits: `create`, `get`, `list`, `delete` and `requests` (lowercase), the store requests of the chain and of its children; or a currency code (uppercase), its `amounts` total: the store's money, the steps' charges and the children's (I-27). Limits are 0 to 2^63-1 (Python `int`, TypeScript `bigint`, Rust `u64`). A malformed meter or limit, or a code outside ISO 4217, raises `InvalidArgument`.
- **Which budget.** `start`'s own, else its workflow's (`budget=` on `@s.workflow`, TypeScript `{ budget }`, Rust `budget = ..` or `.with_budget(..)`), else none. A child (`c.start(..., budget=)`) gets its own the same way, never its parent's. The budget is part of the start: the same id with another budget raises `RunExists`. A continued run keeps it. `run.retry(budget=)` sets the retry's, else keeps the failed run's; the retry keeps the cost already spent, so a retry for more money gives a higher limit; a second retry of the same failed run with another budget raises `InvalidArgument`.
- **When it is checked.** The core compares the chain's cost with the budget before a step attempt starts, before the run commits its progress or suspends, and when the code starts a child or sends a message, never while a replay walks the recorded history. A limit passed (`used > limit`) is recorded in the history (`budget_exceeded`, with the meter, the limit and what was used), so every replay raises it at the same place whatever its worker counted.
- **What the code sees.** `s.BudgetExceeded` (Rust `Error::BudgetExceeded { meter, limit, used }`) is raised at every call the run waits on, as `Cancelled` is (I-13), and at every later step, `c.start` and `c.send`. Undos still run, `publish` works and steps already running finish, their charges recorded: catch it to roll back or publish what was done. The run then ends failed with the type `BudgetExceeded` whatever the code does: `run.result()` raises `RunFailed` whose `error.type` is `BudgetExceeded` and whose `error.details` hold the meter, the limit and what was used (I-20).
- **How far past.** A check sees what was counted before it: a run may pass a limit by the requests of one commit and its effects, and by what the steps already running charge. Keep a margin for both.
- **Per run, not per workflow.** No limit spans the runs of a workflow: bound a workflow by its per-run budget times the runs you start, and alert on the `StoreOp` and `Attempt` events of `observe` (I-29).

## I-29 Observing

```python
def observe(event: s.StoreOp | s.Attempt | s.Replay | s.Scan) -> None:
    match event:
        case s.Attempt(workflow=w, step=step, outcome="failed"):
            metrics.counter("step_failed", workflow=w, step=step).inc()
        case s.Replay(workflow=w, delay=delay) if delay is not None:
            metrics.histogram("replay_delay_seconds", workflow=w).observe(delay)
        case s.StoreOp(cost=s.Amount(nano=nano)):
            metrics.counter("store_nano_usd").inc(nano)

sub = await s.Substantial.open(url, credentials="env", prices=s.Prices("aws-s3-standard-us-east-1-2026-11-09"), observe=observe)
```

```ts
const sub = await s.Substantial.open(url, {
  observe: (event) => {
    if (event instanceof s.Replay && event.delay !== undefined) replayDelay.observe({ workflow: event.workflow }, event.delay);
  },
});
```

```rust
let sub = s::Substantial::open(url)
    .observe(|event: &s::Observed| {
        if let s::Observed::Scan(scan) = event {
            metrics::gauge!("due", "workflow" => scan.workflow.clone()).set(scan.due as f64);
        }
    })
    .await?;
```

`observe=` on `Substantial.create` and `open` is a plain function the host calls for each event, with no metrics library in any host: forward the events to the one your service uses.

| Event | Fields | When |
|---|---|---|
| `StoreOp` | `op` (`create`, `get`, `list`, `delete`), `seconds`, `attempts`, `outcome` (`created`, `exists`, `found`, `missing`, `listed`, `deleted`, `failed`), `cost` | each store operation of the `Substantial`, a client's and a worker's, once answered: `attempts` the requests its adapter sent, retries included; `cost` an `s.Amount(currency, nano)`, `attempts` times the price of one request of its kind, `None` without `prices` |
| `Attempt` | `workflow`, `step`, `attempt`, `seconds`, `outcome` (`ok`, `retry`, `failed`, `timeout`, `abandoned`) | each step attempt a worker runs, once it ends: `retry` an error with attempts left, `failed` none left or a permanent error, `timeout` past its timeout, `abandoned` interrupted by a shutdown or a lost claim |
| `Replay` | `workflow`, `delay`, `seconds`, `end` (`suspended`, `ended`, `idle`, `dropped`) | each replay a worker runs, once it ends: `delay` the seconds from the wakeup's due time to its start, `None` without one; `idle` found nothing new; `dropped` lost a race, was stopped or left at a shutdown |
| `Scan` | `workflow`, `due` | each listing of a workflow's wakeups by a scan: `due` how many it found due, the backlog |

- **Live work only.** Steps a replay answers from the history are no attempts, and emit nothing; nor do the operations of other processes.
- **Synchronous.** The callback runs on the worker's or the client's path, before the next operation: keep it short, and hand slow work to a queue.
- **An observer that raises** stops the worker with that error, as any fatal error does (I-18: `worker.error`, the `result` waits in progress), and fails the client call it was called for. A Rust observer that panics stops the worker with `Error::Protocol` naming the panic, as any panic of a worker does, and a client call panics.
- Same names and fields in every host: Python dataclasses and TypeScript classes `s.StoreOp`, `s.Attempt`, `s.Replay`, `s.Scan` and `s.Amount`; in Rust the enum `s::Observed` of the structs `s::observe::StoreOp`, `Attempt`, `Replay` and `Scan`. `seconds` and `delay` are floating-point seconds; `nano` is an `int`, a `bigint`, a `u64`.

## I-30 Polling

```python
@s.workflow
async def export(c: s.Context, job: str) -> str:
    url = await c.poll("export_url", provider.export_url, job, every=s.minutes(1), timeout=s.hours(2))
    if url is None:
        await c.rollback("the export was not ready within 2 hours")
    return url
```

```ts
const url = await c.poll("export_url", () => provider.exportUrl(input.job), {
  every: s.minutes(1),
  timeout: s.hours(2),
});
if (url === undefined) await c.rollback("the export was not ready within 2 hours");
```

```rust
let job = input.job.clone();
let url: Option<String> = c
    .poll("export_url", move || { let job = job.clone(); async move { provider::export_url(&job).await } })
    .every(s::minutes(1))
    .timeout(s::hours(2))
    .await?;
```

- `c.poll(name, fn, *args, every=, timeout=, retry=None)` runs `fn(*args)` as the inline step `name`, as `c.step` does (I-6), each try with its `retry`. A value of `None` means not ready: the run sleeps `every`, or the time left if shorter, and tries again. It returns the first value that is not `None`, or `None` once `timeout` has passed. TypeScript takes `c.poll(name, fn, { every, timeout, retry, decode })`, where `fn` returning `null` or `undefined` is not ready and `poll` gives `undefined` after the timeout; Rust takes a closure returning `Result<Option<T>, E>` and gives an `Option<T>`, with `.every(d)`, `.timeout(d)` and `.retry(r)`.
- The deadline is the recorded `c.now()` of the first call plus `timeout`, so a replay keeps it. A try already recorded never runs again.
- `every` is positive and `timeout` required: anything else raises `InvalidArgument` (in Rust, at the `.await`).
- **Each try is a step and a sleep**: one wakeup and one segment of the run's history per try ([Architecture](architecture.md#workflow-verbs)). A poll of hours every minute is fine; for one of days, poll in a child (its history ends with it) or in an `iterate` loop of your own, so that the history stays short (I-12).

## I-31 Schedules

```python
@s.workflow
async def nightly(c: s.Context) -> None:
    async for at in c.schedule():                     # each fire time, a UTC datetime
        day = at.astimezone(ZoneInfo("Europe/Zurich")).date().isoformat()
        run = await c.start(report, {"day": day}, id=f"report-{day}")
        try:
            await run.result()
        except s.RunFailed as e:
            c.log.warning("report of %s failed: %s", day, e)

await sub.schedule(nightly, "nightly", s.Cron("0 3 * * *", "Europe/Zurich"), revision=1)
```

```ts
export const nightly = s.workflow("nightly", async (c) => {
  for await (const at of c.schedule()) {
    const day = at.toISOString().slice(0, 10);
    await c.start(report, { day }, { id: `report-${day}` });
  }
});

await sub.schedule(nightly, "nightly", new s.Cron("0 3 * * *", "Europe/Zurich"), { revision: 1 });
```

```rust
#[s::workflow]
async fn nightly(c: s::Context) -> s::Result<()> {
    c.schedule(|c, at| async move {
        let day = at.format("%Y-%m-%d").to_string();
        c.start(&report, Report { day: day.clone() }).id(&format!("report-{day}")).await?;
        Ok(ControlFlow::Continue(()))
    })
    .await
}

sub.schedule(nightly, "nightly", Some(s::Cron::new("0 3 * * *", "Europe/Zurich")?), 1).await?;
```

- **The loop.** `c.schedule()` yields each fire time of the schedule's cron, the instant it was due, never the time it ran (TypeScript `for await`, a `Date`; Rust a closure as `iterate`'s, a `chrono::DateTime<Utc>`, `Break(())` ends it). It is the workflow's one `iterate` (I-12): the run compresses at its boundaries and lives for years on a short history.
- **The config** comes from `sub.schedule(workflow, id, cron, revision=, backfill=5)` (TypeScript `{ revision, backfill }`, Rust `sub.schedule(w, id, cron, revision).backfill(n)`), which starts the run `id` if needed, with an empty input, and sends it the config as the message `schedule`, with the id `schedule-<revision>`. The run applies a config only if its revision is higher than the one it applied: a pod with an older deploy never undoes a newer one. Give the revision from your deploy, such as a counter in the configuration that changes with the cron.
- `sub.schedule` first reads the config the run published: the same revision and config does nothing, so every pod may call it at startup; a higher revision already applied raises `ConfigError` (a stale pod), and so does the same revision with another config. It returns the run's handle.
- **`s.Cron(expr, tz)`**: five fields, minute, hour, day of month, month, day of week; each a list of `*`, `n` and `a-b`, with `/step`; months `jan` to `dec` and days `sun` to `sat` by name too, Sunday `0` or `7`. When both day fields are restricted, a day matching either fires, as in cron. The time zone is an IANA name, required: `"UTC"` for none. An invalid expression, one that never fires or an unknown zone raises `InvalidArgument`.
- **Clock changes.** A local time a clock change skips fires at the end of the gap, 03:00 for 02:30 in Zurich on the last Sunday of March; a local time it repeats fires at its first occurrence only, so an hourly schedule fires once in the repeated hour. Every host computes the same fire times from the same rules (the vectors of `dev/cross/cron.json`), each from its own copy of the IANA time zone database: Python reads the system's through `zoneinfo` (or the `tzdata` package where the system has none), TypeScript the ICU data built into Node or Deno, Rust the copy built into the `chrono-tz` crate at compile time. Hosts whose copies differ can compute different fire times for a zone whose rules changed recently: keep them up to date, or give such a schedule's workflow to one host.
- `cron=None` disables the schedule (TypeScript `null`, Rust `None`): the run waits on messages alone. A new cron or zone fires from the instant it applies, never before.
- **Missed fires are backfilled.** When the run wakes after fire times passed, every worker down, a deploy, an outage, it yields each one, oldest first, before waiting for the next. At most `backfill` of them fire, the most recent ones: the older ones are skipped and counted. The default is 5 periods; give `backfill=1` for a job where only the latest matters. A worker woken less often than the cron's period, such as one run by a Kubernetes CronJob ([Integration](integration.md#a-worker-run-by-a-kubernetes-cronjob)), needs `backfill` at least the worker's period divided by the cron's.
- The fires to run are recorded in the inline step `schedule.next`, with the next fire time, so a replay yields the same instants whatever its clock or time zone database says: a child named from `at` stays idempotent. The step name `schedule.next` is reserved: a step of the code named so raises `InvalidArgument`.
- **What it published.** `run.read("schedule")` returns `revision`, `cron`, `tz`, `backfill`, `last` (the last fired instant), `recent` (the last 10 fired, oldest first), `skipped` (`count`, `oldest` and `newest` skipped instant) and `next`, the instants as `2026-11-16T02:00:00.000Z`; it is published when a config applies, `next` included, and at each fire. A `schedule` message from anything else than `sub.schedule` must hold exactly these config fields, or the run fails with `InvalidArgument`.
- **Cost.** A fire is one wakeup at its timer: the plan's step, the fire's published value and the body's own work ([Architecture](architecture.md#workflow-verbs)).

## I-22 For implementers

### I-23 The core

The core, shipped compiled as `substantial.wasm`: no I/O, no clock and no randomness of its own. Hosts pass the time, random bytes and the answers of the store. It holds three kinds of machine, each driven by one request message in and one response message out (`proto/substantial/boundary/v1`):

| Machine | Opened with | Requests | Responses |
|---|---|---|---|
| run, one per replay | `RunInput`: start, snapshot, segments, inbox, settings, seed, the namespace's prefix | `call`, `step_ended`, `satisfied`, `stored`, `next` | `called`, `next`, `done` |
| agent, one per worker | `AgentConfig` | `next`, `feed`, `commit`, `replay_ended`, `wake`, `wake_workflow`, `shutdown`, `drain` | `command` (`scanned` among them), `done` |
| plan, one per client action | `PlanInput`: `send`, `create`, `open`, `start`, `result`, `status`, `cancel`, `read`, `runs`, `history`, `retry` | `next`, `feed` | `step` (a `StoreOp`, or the outcome: `sent`, `opened`, `started`, `result`, `status`, `read`, `runs`, `history`, `retried`), `done` |

The `runs` plan answers in pages: a `runs` step holds a `page` of `RunInfo`, and the `next` after it goes on, until `end` (or `failed`). Every other plan's outcome is its last step. A `history` event's `data` is the event's fields as JSON, rendered by the core so every host gives the same. A run machine's `RunInput.start.line`, set on a retry, is the run the host builds step keys from instead of the run's own id. A run the code ends by letting a step's `StepFailed` through ends with the failure the core resolved the step with, its details `{"step", "attempts", "error"}` unchanged: `retry` finds the step to run again by them.

Every response may instead be `error`, a `CoreError` (`NON_DETERMINISM`, `INVALID`, `ENDED`, `CORRUPT`, `LAYOUT`, `VALUE_TOO_LARGE`). `VALUE_TOO_LARGE` refuses a value of the code past 64 MiB (I-15), its message `<what> is <size> bytes, over the limit of 67108864 bytes`; the call records nothing.

Randomness comes from the host. A worker gives its agent 32 bytes of its OS CSPRNG (`AgentConfig.seed`), from which each replay gets a seed of its own, for its run and segment (`RunInput.seed`, 32 bytes), and the worker's phase, its first idle wait, from the seed alone. `random` and `uuid` draw from a ChaCha20 generator seeded with it, live only: a replay returns the recorded values. A uuid is a version 4 UUID, 122 of its bits random. The ULIDs of `start` and `send` come from the host.

The run machine. The host's scheduler makes the code's calls in order (`Call`: steps, sleeps, waits, receives, binds, starts, sends, cancels of a child (`Call.cancel`, handled as a send whose name is `substantial.cancel`, toward a child of the run only), awaits of a chain (`Call.chain_result`: the host performs the create of the watcher key among the `store` operations, and the end comes as a `substantial.watched` message the core consumes itself), publishes, sagas, rollbacks, `iterate`, the clock, random and uuid builtins, the end), runs the code until it is quiescent, then asks for `next`:

| `Next` | The host |
|---|---|
| `resolve` | resolves the call with a value, an error, `Cancelled` or a timeout |
| `deliver` | gives the message to the bound handlers; a pending receive gets it by a later `resolve` |
| `child_ended` | marks the child it names as done; its result calls resolve after it |
| `execute` | runs the step attempt, then sends `step_ended` |
| `store` | a read of a workflow's send, a get or a list of keys of the chain it sends to, the namespace's prefix included: performs the `StoreOp` on the namespace's store, with the adapter's retries, and sends `stored` with its `OpResult` before the next `next`; a store that still fails drops the replay, never `stored` with `failed` |
| `commit` | hands the commit (progress, suspend or end) to the agent machine |
| `wait_steps` | waits for a step in flight, or until `until`, the earliest timer or retry |
| `idle` | ends the replay, with the next wakeup if any |

The agent machine answers `next` with an `AgentCommand`:

| `AgentCommand` | The host |
|---|---|
| `store` | performs the `StoreOp` (create, get, list, delete) and sends `feed` with its `OpResult`; several may be in flight |
| `replay` | runs the code with a run machine opened from the `RunInput` |
| `committed` | tells the replay to go on, or to drop and replay again when `won` is false |
| `stop_replay` | stops a replay whose claim was taken over |
| `wait_until` | waits until that time, a store answer or a local wake |
| `scanned` | nothing: an observer's `Scan` event, then `next` again |
| `stop` | the worker has shut down |

Every `called` and `next` says with `live` whether the replay has walked the whole recorded history; before the first, the code is live exactly when the history is empty. The host writes the code's logs only while it is live. A step call's `called` holds its occurrence, which the host puts in the step key. After `deliver` and `child_ended` alike, the host evaluates the `wait_until` predicates before the next `next`.

A workflow's send records its outcome in its `Sent` event of the history (`proto/substantial/layout/v1`): `to`, a `Delivery` naming the run the message goes to and its workflow, which names the message's due entry; or `refused`, the `Failure` the send raised, `RunNotFound` (details `{"run"}`, and `"collected": true` and `"until"` for a chain collected) or `RunEnded` (details `{"run", "last", "status"}`), nothing sent. The failures the core makes hold the one they wrap in their details: `StepFailed` `{"step", "attempts", "error"}`, the last attempt's failure as `{"type", "message", "retryable", "details"}`; a child's `RunFailed` `{"run", "status", "error", "reason"}`.

Costs and budgets (I-27, I-28) are the core's: every `OpResult` fed to an agent (`feed`) or a run machine (`stored`) says in `attempts` how many requests the adapter sent for it, its retries and an ambiguous create's read-back included, at least 1 (plans ignore it); `RunSettings.prices`, absent without prices, prices them. Each segment records the chain's `Cost`; the run machine gets it in `RunInput.cost` and, after each progress commit that won, in `committed.cost`, which the host passes to `arrived` with the messages, none or some, before the replay goes on. `Call.cost` is the builtin `c.cost()`, resolved with the cost as JSON (counts as numbers, amounts as decimal strings). `StepEnded.charges` are the attempt's `s.charge` calls in order; the core checks their currencies against ISO 4217 and sums them. `StartPlan.budget`, `Call.Start.budget` and `RetryPlan.budget` (absent: keep the failed run's) carry a budget, which the core checks. A call the budget refuses answers `called.failed`, a `Failure` of type `BudgetExceeded`, and blocked calls resolve with that error. For `observe` (I-29), `Replay.due_at` is the earliest due entry a replay serves, and the agent answers `scanned` (a workflow and how many entries its listing found due) once it has nothing else to do; the host calls `next` again.

The agent serves each workflow of `AgentConfig.workflows` for its versions, and for its runs without a version when `unversioned`, which is required. `Served.retention` is the workflow's `retention`, absent for the worker's; `RunSettings.retention` is required, a duration of at least `retention_min_ms`, and the end segment records it. `AgentConfig.tombstone_ms`, `retention_min_ms` and `recheck_ms` carry the preset's values.

`wake` asks the agent for a scan of every workflow it serves; `wake_workflow`, for a scan of one workflow's due entries, which a `start` or `send` of the process created an entry under: the host knows the workflow of its `start`, and a `send` or `cancel` plan answers `accepted` with the run and its workflow. A workflow the worker does not serve is ignored.

`drain` (a time, the horizon: the host's now plus `within`) has the agent scan every workflow at once, take only entries due by the horizon, keep no idle timer and no waiting listing, and answer `stop` once no run is in hand, no scan is in flight or asked for, and no wakeup it listed or created, nor the end of a skipped run's claim, comes by the horizon. The host runs its loop as usual until `stop`.

A client's `send` and `cancel` plans take `ord`, a ULID the host draws from its monotonic generator per call, which orders the message's inbox key `runs/<run>/inbox/<ord>-<id>`; a send without `id` uses it as its id. A plan's `no_chain` and `run_exists` outcomes hold `collected_until` when the chain was collected.

A replay dropped, after `committed` with `won` false or `stop_replay`, is reported with `replay_ended` (`dropped`), as one that ended is (`idle`). `step_ended` and `satisfied` for a call that is not a step or a wait answer `INVALID`; for one already resolved, they are ignored. A child result for a run that is not a child, a saga end for a saga not open, a step of a saga not open, and an `iterate` or a continuation whose state is not an object answer `INVALID` too.

The host loop, in Rust:

```rust
loop {
    match agent.next(clock.now())? {
        AgentCommand::Store { id, op } => store_ops.push(perform(store.clone(), id, op)),
        AgentCommand::Replay { run, input } => replays.spawn(replay(workflows.clone(), run, input)),
        AgentCommand::Committed { run, n, won } => replays.tell(run, Committed { n, won }),
        AgentCommand::StopReplay { run } => replays.stop(run),
        AgentCommand::WaitUntil { at } => tokio::select! {
            Some((id, result)) = store_ops.next() => agent.feed(id, result)?,
            Some(event) = replays.next() => match event {
                ReplayEvent::Commit(run, commit) => agent.commit(run, commit)?,
                ReplayEvent::Ended(run, end) => agent.replay_ended(run, end)?,
            },
            _ = local.notified() => agent.wake()?,
            _ = sleep_until(at) => {}
        },
        AgentCommand::Stop => break,
    }
}
```

### I-24 The scheduler

Each host runs workflow code on an executor of its own, never on the runtime that runs steps:

| Host | Executor | Quiescent when |
|---|---|---|
| Rust | the workflow future polled by the host | a poll returns `Pending` with no wake pending |
| Python | a dedicated asyncio loop per replay | its ready queue is empty |
| TypeScript | promises resolved from the host | the microtask queue is drained |

The host delivers one `Next` at a time, runs the code to quiescence, and only then calls `next` again. That one rule makes branches deterministic.

### I-25 The WASM boundary

Every host loads the same module, `wasm/substantial.wasm` (its checksum in `wasm/SHA256SUMS`), Rust through wasmtime. Every call is one message in and one message out, protobuf encoded (`proto/substantial/boundary/v1`); the hosts use the code generated from it.

| Export | Does |
|---|---|
| `memory` | the module's memory, where requests and responses sit |
| `sub_alloc(len) -> ptr` | memory for a request of `len` bytes |
| `sub_free(ptr, len)` | frees a response, or a request never passed to the module |
| `sub_new(kind, ptr, len) -> machine` | a machine of `MachineKind` run (1), agent (2) or plan (3), from its input |
| `sub_call(machine, ptr, len) -> u64` | one request, its response as `ptr << 32 \| len` |
| `sub_refused(machine) -> u64` | the error `sub_new` refused the machine's input with, as `ptr << 32 \| len`, or 0 for a machine made |
| `sub_drop(machine)` | frees a machine |

- **Ownership.** `sub_new` and `sub_call` take the request buffer and free it, whatever the outcome. A response belongs to the host, which frees it with `sub_free` once read.
- **Errors.** Every response carries `CoreError error = 1`, so an error decodes the same whatever the kind. `sub_new` never returns 0: given an unknown kind, or an input that does not decode or is invalid, it returns a machine that answers every call with that error, and must still be dropped. A host asks `sub_refused` right after `sub_new`, so an input the core refuses fails where it is given: a worker whose config the agent refuses, such as prices in a currency outside ISO 4217, raises `ConfigError` when it starts. A request that does not decode is answered with an error and leaves the machine as it was; so does a call on a machine that does not exist.
- **Traps.** A trap is a bug of the core: the host drops the instance and its machines.

The module imports nothing: no WASI, no clock, no file.

### I-26 Store adapters

Users name a store by its URL (I-16). Inside each host, every adapter implements the same interface, which is not part of the public API:

```rust
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StoreFailure>> + Send + 'a>>;

pub trait Store: Send + Sync { // boxed futures, so a worker holds an `Arc<dyn Store>`
    fn capabilities(&self) -> Capabilities; // { wait: bool }
    fn create<'a>(&'a self, key: &'a str, bytes: Bytes) -> StoreFuture<'a, Create>; // Created | Exists
    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Bytes>>;
    fn list<'a>(&'a self, prefix: &'a str, start_after: Option<&'a str>, max_keys: u32, wait: Option<ListWait>) -> StoreFuture<'a, Listing>;
    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()>;
}
```

```python
class Store(Protocol):
    capabilities: Capabilities  # wait: bool
    async def open(self) -> None: ...   # connects, ConfigError on a store it cannot use
    async def create(self, key: str, data: bytes) -> Literal["created", "exists"]: ...
    async def get(self, key: str) -> bytes | None: ...
    async def list(self, prefix: str, *, start_after: str | None, max_keys: int, wait: ListWait | None) -> Listing: ...
    async def delete(self, key: str) -> None: ...
    async def close(self) -> None: ...
```

```ts
interface Store {
  readonly capabilities: { wait: boolean };
  create(key: string, data: Uint8Array): Promise<"created" | "exists">;
  get(key: string): Promise<Uint8Array | undefined>;
  list(prefix: string, options: { startAfter?: string; maxKeys: number; wait?: { etag?: string; seconds: number } }): Promise<{ keys: string[]; etag?: string }>;
  delete(key: string): Promise<void>;
  close(): Promise<void>;
}
```

The operations each verb performs on the store, their round trips and what they cost on S3 are in [Architecture](architecture.md).

An adapter is scoped to its namespace: keys are relative to it, so the same keys work under a bucket, a prefix or a directory. An adapter retries a transient failure itself, 5 attempts in all with a backoff doubling from 100 ms to 5 s, the same in every host, and reports with each answer how many requests it sent, which a run's cost counts (I-27). An operation that still fails after them raises `StoreFailure`; `close` (Python and TypeScript) releases the adapter's connections.

Each URL scheme opens its adapter (`MemoryStore`, `FsStore`, `S3Store`, `S3Store` with `wait = true` for `h3://`, and `RedisStore` for every Redis scheme), its query parameters as fields. `Substantial.create` and `open` call the adapter's `open` first, in every host, so a store that fails the checks of its adapter raises `ConfigError`. Adapters resolve ambiguous creates with a `get`, as the store contract requires.

#### The store contract

Four operations, the only ones any part of Substantial performs on a store:

| Operation | Meaning |
|---|---|
| `create(key, bytes) -> created \| exists` | atomic and exclusive: of concurrent creates of a key, exactly one gets `created`, and an acknowledged `created` survives a crash of the store |
| `get(key) -> bytes \| missing` | the bytes of a created key |
| `list(prefix, start_after, max_keys, wait) -> keys, etag` | keys under `prefix` and after `start_after`, in byte order of the full key, at most `max_keys`, including every key whose create was acknowledged before the call started; `etag` and `wait` only on stores that declare them |
| `delete(key)` | removes a key; deleting a missing key succeeds |

How each store implements them:

| | File system | S3 | Waiting S3 (`h3://`) | Redis |
|---|---|---|---|---|
| `create` | temporary file in `<root>/.tmp/`, fsync, `link()` to the key (fails if it exists), fsync the directory | `PutObject`, `If-None-Match: *`; 412 is `exists`, 409 is retried | as S3 | the script `create.lua`: `SET {ns}:o:<key> NX`, then `ZADD {ns}:i 0 <key>` and a publish of the key on `{ns}:c`, or `INCR {ns}:f` when the key exists; then `WAITAOF 1 <replicas> 5000` on the same connection |
| `get` | read | `GetObject` | as S3 | `GET {ns}:o:<key>` and `INCR {ns}:f`, then `WAITAOF` |
| `list` | walk the deepest directory holding the prefix, keep the keys in range, sort by full key | `ListObjectsV2`, `prefix`, `start-after`, `max-keys` up to 1,000 | as S3; a waiting one is one request of at most 1,000 keys with the wait headers | `ZRANGE {ns}:i [<prefix> (<prefix>\xff BYLEX LIMIT 0 <max_keys>`, the lower bound `(<start_after>` when it follows the prefix, and `INCR {ns}:f`, then `WAITAOF` |
| `delete` | unlink | `DeleteObject` | as S3 | the script `delete.lua`: `DEL`, `ZREM` and a publish of the key when it existed, `INCR {ns}:f` when it was missing; then `WAITAOF` |
| `wait` | no | no | yes | yes |

Rules for every store:

- **No overwrite.** Substantial never writes an existing key. An adapter that cannot refuse a second create of a key is not a store.
- **Hints may come back.** Due entries, rest markers (`queues/<workflow>/rest/<run>`, the runs waiting on messages only) and claims are hints: one may be created again after it was deleted, the claim of a paused worker even after its run was collected. So may three history keys: a consumed `inbox` key, created again by an ambiguous send's retry or by a sender's redone effects, which the run consumes without delivery (I-9); an `index` entry, created again empty; and the `start` of a creator that met a collected id and withdraws it, left inert, completed by a later `start` with the same input. The others (`log`, `snap`, `effects`, `pub`, `result`, `gone`) never come back, except under an id started again after its tombstone. A consumed `inbox` key a crashed worker left is deleted by the run's next replay, by a run that continued when it forwards its inbox, or by the run's collection.
- **A short page ends a listing.** A `list` answers `max_keys` keys whenever at least that many follow `start_after`: a store that answers fewer before the end hides keys.
- **A create lands in time, or never.** A create, its retries included, is applied within a bounded time of being sent or not at all. S3 does not promise it, since a request the adapter gave up on may still be applied; the bound is the operator's, at least the adapter's 5 attempts of 60 s each plus their backoff.
- **Ambiguous outcomes.** A create that fails without an answer (a timeout, a 5xx, a lost connection) is followed by a `get`: the same bytes mean `created`, other bytes `exists`, `missing` means create again. An `exists` after an ambiguous attempt of the same create is checked the same way, since that attempt may have landed late. A worker never assumes it won or lost.
- **Retries.** The adapter retries transient failures with exponential backoff, 5 attempts in all, then answers `StoreFailure`; a worker's agent reissues the operation up to 5 times at once, then drops the replay and the wakeup stays.
- **Declared capabilities.** An adapter declares whether its `list` can wait; the URL scheme names the adapter, nothing is detected.
- **Probe at start.** `create` creates `probe/exclusive` before the namespace marker: in a new namespace twice, requiring `created` then `exists`; in an existing one, and at `open`, once, requiring `exists`. The key is kept. A store that accepts two creates of it raises `LayoutError`. This catches S3-compatible stores that ignore `If-None-Match`.
- **Failover.** A store whose replicas copy writes asynchronously may lose an acknowledged create when one is promoted, and a second create of the same key then succeeds: two segments `n`, two histories. Only stores that never lose an acknowledged write are supported.

Per store:

- **File system.** `O_EXCL` is not reliable on NFS; the adapter uses `link()`. Keys are lowercase, so a file system that folds case (macOS by default) holds them as they are, and no component passes 255 bytes. A key is never the directory of another, so the layout never needs both `a` and `a/b`. Parent directories are created and fsynced before the link, and never removed, so no `rmdir` races a `link`. Key components are checked: no `.` or `..`, at most 255 bytes. For development and CI, and for one machine.
- **S3.** Conditional writes are required: AWS, MinIO, Ceph RGW from 8.1 (9.0 for versioned buckets), R2. AWS answers 409 `ConditionalRequestConflict` to concurrent conditional writes of one key: the adapter retries, it is not `exists`; 429 is retried too, and 408 is ambiguous like a 5xx. A missing key is `NoSuchKey` only: a missing bucket is a store failure. Requests time out after 5 s to connect and 60 s per attempt, so a hung request is an ambiguous outcome. On a versioned bucket a deleted hint is a delete marker, and creating it again succeeds. The probe checks each namespace. The adapters use the `aws-sdk-s3` crate, `@aws-sdk/client-s3`, and in Python aiohttp, its requests signed by botocore.
- **Redis.** Redis or Valkey 7.2 or later, for `WAITAOF`. `{ns}` is the hash tag of every key, so a namespace lives on one slot, one shard of a cluster: spread load over namespaces. The scripts are the files of `redis/` at the repository root, shipped in each package, loaded with `SCRIPT LOAD` by every connection pool to a primary and run with `EVALSHA`. The adapters use redis-py, `@redis/client` and the `redis` crate, each operation on a connection of its own, as `WAITAOF` blocks the connection it is sent on. Requests time out after 5 s to connect and 60 s per attempt, finding the primary included, and the client's own retries are off. Everything is in memory, about 2 to 3 KiB per run of three steps and a sleep, kept until its collection (I-16): size the servers for it.
  - **Checked at open.** AOF off (`appendonly no`), a `maxmemory-policy` other than `noeviction`, a server without `WAITAOF`, a cluster node named by a URL that is not `redis+cluster://` or the reverse, and a replica named by `redis://` raise `ConfigError`. Set `appendfsync always` on every server: with `everysec`, `WAITAOF` makes a primary fsync at once, but a replica fsyncs and acknowledges once a second, so a create on Sentinel or Cluster waits up to 1 s ([measured](redis.md)).
  - **Durability.** A create is acknowledged once `WAITAOF` reports the primary and `replicas` replicas fsynced. `all` is the number of replicas online on the primary, counted at each connection to it, every 10 s, and after each `WAITAOF` short of it. `WAITAOF` is a round trip of its own: sent in one pipeline with the write, replicas acknowledge their fsync at their next second, measured on Valkey 8.1 and 9.1 and Redis 7.4.
  - **Ambiguous outcomes.** A `WAITAOF` short of the count, a timeout or a lost connection is an ambiguous create. The `get` that resolves it sends `GET` and `INCR {ns}:f` together, then `WAITAOF` on the same connection: the AOF and the replication stream are one ordered log, so a fence fsynced everywhere means the bytes the `GET` found are too, and `created` is answered only then. The script fences an `exists` the same way, so no answer rests on a create a failover may still lose.
  - **Reads and deletes.** What a client saw never changes in a failover: the store contract has a create land in time or never, and a client returning a `result` a failover then loses would see the run end again, maybe otherwise; a collection deletes history keys, which must not come back once seen missing. So every `get`, `list`, waiting ones included, and `delete` sends `INCR {ns}:f` with its command, `delete.lua` when the key is missing, then `WAITAOF` on the same connection, and answers once the fence is fsynced on the primary and the replicas; one short of them is a transient failure. On one server with `appendfsync always`, Valkey 8.1 writes and fsyncs the AOF before it sends any reply of the same event loop (`beforeSleep` in `server.c`, `flushAppendOnlyFile` in `aof.c`), so a read there cannot see a write a crash then loses; it can with `everysec`, with `no-appendfsync-on-rewrite yes` during a rewrite, and through any failover. The fence covers every case, at a round trip and an fsync per operation ([measured](redis.md)).
  - **Failover.** Sentinel and Redis Cluster promote the replica with the most data; with `replicas=all` it holds every acknowledged create. On Sentinel and Cluster, a primary with no replica online acknowledges no create, its creates failing as transient: a primary just promoted, its replicas still syncing, may itself be replaced by one of them, which would lack what it acknowledged alone. Reads and deletes fail the same way. So a shard with one replica takes no create after its failover until a replica joins the promoted one: give Sentinel and Cluster at least 2 replicas per primary.
  - **Residual risk.** A replica offline when a create was acknowledged was not waited for. A double failure loses that create: the replica offline, then the primary lost while every replica holding the create is unavailable, and that replica promoted. Keep a replica disconnected for long from being promoted. Sentinel has no setting of its own for it: it skips a replica whose link to the primary has been down longer than 10 times `down-after-milliseconds` plus the time the primary has been down (`sentinelSelectReplica` in `sentinel.c`), so `down-after-milliseconds` bounds that window; 5000 ms, a 50 s window, is a fair trade against failovers started by a slow primary. Set `replica-priority 0` on a replica known to be behind, as one back from maintenance, until it has synced. On Cluster, keep `cluster-replica-validity-factor` above 0, which would let a replica of any age be promoted; 1 makes a replica whose data is older than `cluster-node-timeout` plus `repl-ping-replica-period` ineligible. After a failover, the adapter asks the sentinels or the cluster nodes for the new primary at its next attempt.
  - **Waiting listings.** Each adapter holds one subscription to `{ns}:c` (`SSUBSCRIBE` on Cluster), demultiplexed by prefix. A waiting listing registers before it lists, answers at once when the page's etag, a hash of its keys and of whether more follow, differs from the one given, and otherwise waits for a key under its prefix or the end of its wait, then lists again. A lost subscription wakes every waiting listing, which lists again once subscribed anew; one silent for 10 s gets a `PING`, and is lost after 20 s.
  - **Permissions.** A user restricted by ACL needs, on the keys `{ns}:*` and the channel `{ns}:c`: `EVALSHA`, `SCRIPT LOAD`, `GET`, `INCR`, `ZRANGE`, `WAITAOF`, `INFO`, `PING`, `SUBSCRIBE` or `SSUBSCRIBE`, and the scripts' `SET`, `ZADD`, `DEL`, `ZREM`, `PUBLISH` or `SPUBLISH`; on a cluster, `CLUSTER SLOTS`.
- **Waiting S3.** An S3-compatible store whose listings can wait, with every rule of S3 above. A waiting listing sends `x-substantial-wait-seconds` (0 to 20) and the previous `x-substantial-wait-etag`, both signed, and takes its etag from `x-substantial-listing-etag`; an answer without it is a store failure, the URL naming a server whose listings cannot wait. A wait fits the 60 s of an attempt.

## Later

Not in any host:

- a budget across the runs of a workflow; until then, a per-run budget times the runs, and an alert on the events of `observe` (I-28);
- ended runs in `sub.runs`, the failed ones to retry among them, or a monitoring tool for stuck, failed and unserved runs; until then, keep the ids you start;
- collecting the covered history segments of a live run; until then, they stay until the run is collected (I-16);
- `sub.repair`, rebuilding the hints of a namespace;
- values stored beside the history;
- SQL stores.
