from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Union
from uuid import uuid4
from substantial.backends.backend import Backend
from substantial.protos.metadata import Metadata
from substantial.protos.events import Event, Records


class FSBackend(Backend):
    """
    This backend is for testing purposes only as it is not scalable:
      - there is no file order in POSIX
      - all the runs need to be loaded in memory
      - not all required operations are atomic or using compare-and-swap
    """

    def __init__(self, root: str):
        self.root = Path(root)
        for d in ["runs", "schedules", "leases"]:
            (self.root / d).mkdir(parents=True, exist_ok=True)

    async def read_events(self, run_id: str) -> Union[Records, None]:
        f = self.root / "runs" / run_id / "events"
        if not f.exists():
            return None
        return Records().from_json(f.read_text())

    async def write_events(self, run_id: str, content: Records) -> None:
        f = self.root / "runs" / run_id / "events"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(content.to_json(indent=4))

    async def read_all_metadata(self, run_id: str) -> List[str]:
        f = self.root / "runs" / run_id / "logs"
        ret = []
        for log in f.iterdir():
            # could be polymorphic
            ret.append(log.read_text())
        return ret

    async def append_metadata(self, run_id: str, schedule: datetime, content: str):
        f = self.root / "runs" / run_id / "logs" / schedule.isoformat()
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(content)

    async def next_run(self, queue: str, excludes: list[str])  -> Union[Tuple[str, datetime], None]:
        f = self.root / "schedules" / queue
        excludes_set = set(excludes) # Note: lease related

        for schedule in sorted(f.iterdir()):
            for run_id in schedule.iterdir():
                if run_id.name not in excludes_set:
                    return run_id.name, datetime.fromisoformat(schedule.name)

        return None

    async def add_schedule(
        self, queue: str, run_id: str, schedule: datetime, content: Union[Event, None]
    ) -> None:
        q = self.root / "schedules" / queue

        waiting_schedule = None
        if q.exists():
            # Note: new schedule should always overwrite replays (but not scheduled events)
            # This is for keeping the run consistent,
            # Case 1: ======= r1 == ev1 == r2 ====== r3 ========>
            #                              x r4
            # Result: ============== ev1 = r4 ====== r3 ========>
            # Rationale being that the scheduled replays are often triggered by Interrupts and such
            # for more sane runs, fuse planned replays (induced by add_schedule calls)?

            for sched in sorted(q.iterdir()):
                for planned in sched.iterdir():
                    planned_date = datetime.fromisoformat(sched.name)
                    if planned.name == run_id and planned_date <= schedule:
                        event = await self.read_schedule(queue, run_id, planned_date)
                        if event is None: # event => None == scheduled replays..
                            await self.close_schedule(queue, run_id, planned_date)

        f1 = q / schedule.isoformat() / run_id
        f1.parent.mkdir(parents=True, exist_ok=False)
        f1.write_text(
            "" if content is None
            else content.to_json()
        )

    async def read_schedule(self, queue: str, run_id: str, schedule: datetime) -> Union[Event, None]:
        f = self.root / "schedules" / queue / schedule.isoformat() / run_id
        if not f.exists():
            raise Exception(f"run not found: {f}")
        ret = f.read_text()
        return None if ret == "" else Event().from_json(ret)

    async def close_schedule(self, queue: str, run_id: str, schedule: datetime) -> None:
        f = self.root / "schedules" / queue / schedule.isoformat() / run_id
        if not f.exists():
            # raise Exception(f"run not found: {f}")
            return
        print(f"closed {f}")
        f.unlink()

    async def active_leases(self, lease_seconds: int) -> List[str]:
        f = self.root / "leases"
        ret = []
        for lease in f.iterdir():
            if not lease_held(lease, lease_seconds):
                continue
            ret.append(lease.name)
        return ret

    async def acquire_lease(self, run_id: str, lease_seconds: int) -> bool:
        f = self.root / "leases" / run_id
        return leasing_cas(f, "acquire", lease_seconds)

    async def renew_lease(self, run_id: str, lease_seconds: int) -> bool:
        f = self.root / "leases" / run_id

        if not f.exists():
            raise Exception(f"lease not found: {f}")

        return leasing_cas(f, "renew", lease_seconds)

    async def remove_lease(self, run_id: str, lease_seconds: int):
        f = self.root / "leases" / run_id

        if not f.exists():
            raise Exception(f"lease not found: {f}")

        still_holding = leasing_cas(f, "remove", lease_seconds)
        if still_holding:
            f.unlink()


def leasing_cas(f: Path, suffix: str, lease_seconds: int) -> bool:
    if lease_held(f, lease_seconds):
        return False

    return cas(f, suffix)


def lease_held(f: Path, lease_seconds: int) -> bool:
    return (
        f.exists()
        and datetime.fromtimestamp(f.stat().st_mtime) + timedelta(seconds=lease_seconds)
        > datetime.now()
    )


def cas(f: Path, suffix: str) -> bool:
    nonce = str(uuid4())
    witness = f.parent / f"{f.name}.{suffix}"
    witness.write_text(nonce)

    try:
        witness.rename(f)
        return f.read_text() == nonce
    except FileExistsError:
        # another rename has already happened
        witness.unlink()
        return False
