"""The loop of one replay's code (I-24), stepped by the host until its ready queue is empty."""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Coroutine
from typing import Any


class Executor:
    """The loop of one replay's code, stepped by the host, never run on its own."""

    def __init__(self) -> None:
        # A selector loop by name: the stepping below relies on asyncio's own loop, never on a
        # loop a policy may substitute.
        self.loop = asyncio.SelectorEventLoop()

    def spawn(self, coro: Coroutine[Any, Any, Any], context: contextvars.Context) -> asyncio.Task:
        return self.loop.create_task(coro, context=context)

    async def run(self) -> None:
        """Runs the code until it is quiescent: its ready queue is empty."""
        # From Python 3.14 the running task is kept per thread, and no task of this loop may
        # start while one of the host's runs: the stepping happens in a plain callback of the
        # host's loop, between its tasks.
        host = asyncio.get_running_loop()
        done = host.create_future()

        def step() -> None:
            try:
                self._run()
            except BaseException as e:
                if not done.done():
                    done.set_exception(e)
                return
            if not done.done():
                done.set_result(None)

        host.call_soon(step)
        await done

    def _run(self) -> None:
        # asyncio refuses to run a loop inside another, so the host's loop is set aside while
        # this one steps.
        outer = asyncio._get_running_loop()
        asyncio._set_running_loop(None)
        try:
            while self.loop._ready:  # type: ignore[attr-defined]
                self.loop.call_soon(self.loop.stop)
                self.loop.run_forever()
        finally:
            asyncio._set_running_loop(outer)

    async def close(self) -> None:
        for task in asyncio.all_tasks(self.loop):
            task.cancel()
        try:
            await self.run()
        finally:
            self.loop.close()
