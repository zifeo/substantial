from datetime import datetime


class Backend:
    # run related

    async def read_events(self, run_id: str):
        raise NotImplementedError()

    async def write_events(self, run_id: str, content: str):
        raise NotImplementedError()

    async def read_all_metadata(self, run_id: str):
        raise NotImplementedError()

    async def append_metadata(self, run_id: str, schedule: datetime, content: str):
        raise NotImplementedError()

    async def add_schedule(
        self, queue: str, run_id: str, schedule: datetime, content: str
    ):
        raise NotImplementedError()

    async def read_schedule(self, queue: str, run_id: str, schedule: datetime) -> str:
        raise NotImplementedError()

    async def close_schedule(self, queue: str, run_id: str, schedule: datetime):
        raise NotImplementedError()

    # agent related

    async def next_run(self, queue: str, excludes: list[str]):
        raise NotImplementedError()

    async def active_leases(self, lease_seconds: int):
        raise NotImplementedError()

    async def acquire_lease(self, run_id: str, lease_seconds: int) -> bool:
        raise NotImplementedError()

    async def renew_lease(self, run_id: str, lease_seconds: int) -> bool:
        raise NotImplementedError()

    async def remove_lease(self, run_id: str, lease_seconds: int):
        raise NotImplementedError()
