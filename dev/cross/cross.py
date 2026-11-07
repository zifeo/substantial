"""The Python side of the cross-language run, driven by dev/cross/run.py.

cross.py worker <url>    serves the workflow until SIGTERM
cross.py client <url>    starts a run, messages it and prints its result as JSON
"""

import asyncio
import json
import sys
from datetime import timedelta

from substantial import Context, Substantial, minutes, step, workflow

LANGUAGE = "python"


@step
def greet(name: str) -> str:
    return f"hello {name}"


@step
def repeat(text: str, times: int) -> str:
    return text * times


@workflow(name="cross", version=1)
async def cross(c: Context, name: str, delay_ms: int) -> dict:
    greeting = await greet(name)
    await c.sleep(timedelta(milliseconds=delay_ms))
    suffix = await c.receive("suffix")
    return {
        "greeting": greeting,
        "suffix": await repeat(suffix["text"], suffix["times"]),
        "worker": LANGUAGE,
    }


async def main(mode: str, url: str) -> None:
    credentials = "env" if url.startswith("s3") else None
    sub = await Substantial.create(url, partitions=1, credentials=credentials)
    if mode == "worker":
        await sub.worker([cross], preset="script").serve()
    else:
        run = await sub.start(cross, {"name": "ada", "delay_ms": 200}, id="cross-1")
        await run.send("suffix", {"text": "!", "times": 3}, id="suffix-1")
        result = await run.result(timeout=minutes(1))
        status = await run.status()
        print(json.dumps({"result": result, "status": str(status), "client": LANGUAGE}))
    await sub.close()


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] not in ("worker", "client"):
        sys.exit("usage: cross.py worker|client <url>")
    asyncio.run(main(sys.argv[1], sys.argv[2]))
