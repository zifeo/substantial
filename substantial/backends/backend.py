from datetime import datetime
from typing import List, Tuple, Union
from substantial.protos.events import Event, Records
from substantial.protos.metadata import Metadata


class Backend:
    # run related

    async def read_events(self, run_id: str) -> Union[Records, None]:
        raise NotImplementedError()

    async def write_events(self, run_id: str, content: Records) -> None:
        raise NotImplementedError()

    async def read_all_metadata(self, run_id: str) -> List[Metadata]:
        raise NotImplementedError()

    async def append_metadata(
        self, run_id: str, schedule: datetime, content: str
    ) -> None:
        raise NotImplementedError()

    async def add_schedule(
        self, queue: str, run_id: str, schedule: datetime, content: Union[Event, None]
    ) -> None:
        raise NotImplementedError()

    async def read_schedule(
        self, queue: str, run_id: str, schedule: datetime
    ) -> Union[Event, None]:
        raise NotImplementedError()

    async def close_schedule(self, queue: str, run_id: str, schedule: datetime) -> None:
        raise NotImplementedError()

    # agent related

    async def next_run(
        self, queue: str, excludes: list[str]
    ) -> Union[Tuple[str, datetime], None]:
        raise NotImplementedError()

    async def active_leases(self, lease_seconds: int) -> List[str]:
        raise NotImplementedError()

    async def acquire_lease(self, run_id: str, lease_seconds: int) -> bool:
        raise NotImplementedError()

    async def renew_lease(self, run_id: str, lease_seconds: int) -> bool:
        raise NotImplementedError()

    async def remove_lease(self, run_id: str, lease_seconds: int) -> None:
        raise NotImplementedError()
