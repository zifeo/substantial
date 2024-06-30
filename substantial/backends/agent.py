import asyncio
from contextlib import suppress
from substantial.backends.backend import Backend


class Agent:
    def __init__(self, backend: Backend, queue: str):
        self.backend = backend
        self.queue = queue

    async def pool(self):
        lease_seconds = 10
        renew_seconds = 8

        while True:
            active_leases = await self.backend.active_leases(lease_seconds)
            run_id = await self.backend.next_run(self.queue, active_leases)
            print("run_id", run_id)

            if run_id is None:
                await asyncio.sleep(1)
                continue

            leased = await self.backend.acquire_lease(run_id, lease_seconds)
            if not leased:
                continue

            async def heartbeat():
                while True:
                    await asyncio.sleep(renew_seconds)
                    renewed = await self.backend.renew_lease(run_id, lease_seconds)
                    if not renewed:
                        return

            renew_task = asyncio.create_task(heartbeat())
            process_task = asyncio.create_task(self.process(run_id))

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

            exception = done.exception()
            if exception is None:
                print("done")
            else:
                print("error", exception)

    async def process(self, run_id: str):
        try:
            pass

        except Exception as e:
            print(e)

        finally:
            # write log and events
            pass
