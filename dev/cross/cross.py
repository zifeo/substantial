"""The Python side of the cross-language run, driven by dev/cross/run.py.

cross.py worker <url>    serves the workflows until SIGTERM
cross.py watcher <url>   serves this language's `watch-<language>` until SIGTERM
cross.py client <url>    starts a run with a budget, waits until it waits on messages alone,
                         messages it and prints its result and the kinds of its history as JSON,
                         with the failure of a run of `over` that passes its budget
cross.py start <url>     starts the run `same-input`, its input written its own way, and prints
                         its chain and status as JSON
"""

import asyncio
import json
import sys
from datetime import timedelta

from substantial import (
    ConfigError,
    Context,
    Cron,
    RunFailed,
    Substantial,
    Workflow,
    charge,
    minutes,
    step,
    workflow,
)

LANGUAGE = "python"


@step
def greet(name: str) -> str:
    return f"hello {name}"


@step
def repeat(text: str, times: int) -> str:
    charge("USD", 1500000)
    return text * times


@workflow(name="cross", version=1)
async def cross(c: Context, name: str, delay_ms: int) -> dict:
    greeting = await greet(name)
    await c.sleep(timedelta(milliseconds=delay_ms))
    suffix = await c.receive("suffix")
    repeated = await repeat(suffix["text"], suffix["times"])
    cost = await c.cost()
    return {
        "greeting": greeting,
        "suffix": repeated,
        "worker": LANGUAGE,
        "charged": str(cost.steps["repeat"]["USD"]),
        "counted": cost.requests > 0,
    }


@workflow(name="over", version=1)
async def over(c: Context, text: str) -> str:
    """Charges more than a budget of USD 1000000 allows, so its next step is refused."""
    repeated = await repeat(text, 1)
    return await greet(repeated)


async def overrun(sub: Substantial) -> dict:
    """Starts `cross-over` with a budget it passes; its failure as its result gives it, its
    history ending failed."""
    run = await sub.start(over, {"text": "!"}, id="cross-over", budget={"USD": 1000000})
    try:
        await run.result(timeout=minutes(1))
    except RunFailed as e:
        failure = e.error
    else:
        raise RuntimeError("cross-over completed within a budget it passes")
    ended = (await run.history())[-1]
    if ended.kind != "failed":
        raise RuntimeError(f"cross-over failed, its history ending with {ended.kind}")
    if failure.details is None:
        raise RuntimeError("cross-over failed without details")
    fields = {k: failure.details[k] for k in ("meter", "limit", "used")}
    return {"type": failure.type, **fields}


@workflow(name="ticks", version=1)
async def ticks(c: Context) -> None:
    async for _ in c.schedule():
        pass


NIGHTLY = Cron("0 3 * * *", "Europe/Zurich")


async def scheduled(sub: Substantial) -> dict:
    """Configures `cross-ticks`, served by another host, until it publishes the config; the same
    call again does nothing, an older revision is refused."""
    run = await sub.schedule(ticks, "cross-ticks", NIGHTLY, revision=2, backfill=3)
    while (published := await run.read("schedule")) is None or published["revision"] != 2:
        await asyncio.sleep(0.05)
    await sub.schedule(ticks, "cross-ticks", NIGHTLY, revision=2, backfill=3)
    try:
        await sub.schedule(ticks, "cross-ticks", NIGHTLY, revision=1)
        stale = "accepted"
    except ConfigError:
        stale = "ConfigError"
    keys = ("revision", "cron", "tz", "backfill", "last", "recent")
    return {**{k: published[k] for k in keys}, "stale": stale}


@workflow(name="awaited", version=1)
async def awaited(c: Context) -> dict:
    return {"worker": LANGUAGE, "go": await c.receive("go")}


def watcher(language: str) -> Workflow:
    """`watch-<language>`, which awaits the end of a chain it did not start: served by that
    language's worker, a stub elsewhere."""

    @workflow(name=f"watch-{language}", version=1)
    async def watch(c: Context, chain: str) -> dict:
        return {"watcher": LANGUAGE, "result": await c.result(chain)}

    return watch


async def watched(sub: Substantial, language: str) -> dict:
    """Starts `cross-awaited`, served by the worker, and `cross-watch`, served by the watcher of
    `language`, which awaits it; once the watcher waits, lets `cross-awaited` end."""
    run = await sub.start(awaited, id="cross-awaited")
    watch = await sub.start(watcher(language), {"chain": run.chain}, id="cross-watch")
    while await watch.status() != "suspended":
        await asyncio.sleep(0.05)
    await run.send("go", "now")
    return await watch.result(timeout=minutes(1))


async def waits_on_messages(sub: Substantial) -> None:
    """Until `cross-1`, served by another host, is listed waiting on messages alone."""
    while True:
        async for i in sub.runs(cross):
            if (i.run, i.waiting_on) == ("cross-1", "message"):
                return
        await asyncio.sleep(0.05)


async def main(mode: str, url: str, watching: str | None) -> None:
    credentials = "env" if url.startswith("s3") else None
    sub = await Substantial.create(url, credentials=credentials)
    if mode == "worker":
        await sub.worker([cross, over, ticks, awaited], preset="script").serve()
    elif mode == "watcher":
        await sub.worker([watcher(LANGUAGE)], preset="script").serve()
    elif mode == "start":
        run = await sub.start(cross, {"delay_ms": 200.0, "name": "ada"}, id="same-input")
        print(json.dumps({"chain": run.chain, "status": str(await run.status())}))
    else:
        budget = {"requests": 100000, "USD": 10000000}
        run = await sub.start(cross, {"name": "ada", "delay_ms": 200}, id="cross-1", budget=budget)
        await waits_on_messages(sub)
        await run.send("suffix", {"text": "!", "times": 3}, id="suffix-1")
        result = await run.result(timeout=minutes(1))
        status = await run.status()
        history = [e.kind for e in await run.history()]
        line = {
            "result": result,
            "status": str(status),
            "history": history,
            "over": await overrun(sub),
            "schedule": await scheduled(sub),
            "watched": await watched(sub, str(watching)),
            "client": LANGUAGE,
        }
        print(json.dumps(line))
    await sub.close()


if __name__ == "__main__":
    mode, args = sys.argv[1] if len(sys.argv) > 1 else "", sys.argv[2:]
    if mode not in ("worker", "watcher", "client", "start") or len(args) != 1 + (mode == "client"):
        sys.exit("usage: cross.py worker|watcher|start <url>, cross.py client <url> <watcher>")
    asyncio.run(main(mode, args[0], args[1] if mode == "client" else None))
