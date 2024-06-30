import asyncio
from contextlib import suppress
from datetime import datetime, timedelta
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

            async def heartbeat():
                while True:
                    await asyncio.sleep(renew_seconds)
                    renewed = await self.backend.renew_lease(run_id, lease_seconds)
                    if not renewed:
                        return

            leased = await self.backend.acquire_lease(run_id, lease_seconds)
            if not leased:
                continue

            renew_task = asyncio.create_task(heartbeat())
            schedule = datetime.now()
            process_task = asyncio.create_task(self.process(run_id, schedule))

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

    async def process(self, run_id: str, schedule: datetime):
        try:
            events = await self.backend.read_events(run_id)
            log = ""

            print("events", events)

            await self.backend.schedule_run(
                self.queue, run_id, schedule + timedelta(seconds=10)
            )

            await self.backend.unschedule_run(self.queue, run_id, schedule)

            pass

        except Exception as e:
            print(e)

        finally:
            await self.backend.write_events(run_id, events)
            await self.backend.append_metadata(run_id, schedule, log)
            pass
