import asyncio
from contextlib import suppress
from substantial.backends.backend import Backend
import uvloop

from substantial.workflows.run import Run


# move to backend?
lease_seconds = 10
renew_seconds = 8
pool_interval = 1


class Agent:
    def __init__(self, backend: Backend, queue: str):
        self.backend = backend
        self.queue = queue

    def run_sync(self):
        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            runner.run(self.run())

    async def run(self):
        while True:
            await self.poll_with_lease()
            await asyncio.sleep(pool_interval)

    async def poll_with_lease(self):
        active_leases = await self.backend.active_leases(lease_seconds)
        next_run = await self.backend.next_run(self.queue, active_leases)
        print("run_id", next_run)

        if next_run is None:
            print("no runs")
            await asyncio.sleep(1)
            return

        run_id, schedule = next_run

        async def heartbeat():
            while True:
                await asyncio.sleep(renew_seconds)
                renewed = await self.backend.renew_lease(run_id, lease_seconds)
                if not renewed:
                    return

        leased = await self.backend.acquire_lease(run_id, lease_seconds)
        if not leased:
            return

        renew_task = asyncio.create_task(heartbeat())
        run = Run(run_id, self.queue, self.backend)
        process_task = asyncio.create_task(run.replay(schedule))

        done, pending = await asyncio.wait(
            [
                renew_task,
                process_task,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        done = done.pop()
        pending = pending.pop()

        pending.cancel()
        with suppress(asyncio.CancelledError):
            await pending

        await self.backend.remove_lease(run_id, lease_seconds)
