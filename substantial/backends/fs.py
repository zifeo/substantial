from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4
from substantial.backends.backend import Backend


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

    def read_events(self, run_id: str):
        f = self.root / "runs" / run_id / "events"
        if not f.exists():
            return None

        return f.read_text()

    def write_events(self, run_id: str, content: str):
        f = self.root / "runs" / run_id / "events"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(content)

    def read_all_metadata(self, run_id: str):
        f = self.root / "runs" / run_id / "logs"
        ret = []
        for log in f.iterdir():
            ret.append(log.read_text())
        return ret

    def append_metadata(self, run_id: str, schedule: datetime, content: str):
        f = self.root / "runs" / run_id / "logs" / schedule.isoformat()
        f.parent.mkdir(parents=True, exist_ok=False)
        f.write_text(content)

    def next_run(self, queue: str, excludes: list[str]):
        f = self.root / "schedules" / queue
        excludes_set = set(excludes)

        for schedule in sorted(f.iterdir()):
            if schedule.stem not in excludes_set:
                run_id = schedule.stem
                schedule = schedule.parent.stem
                return run_id, datetime.fromisoformat(schedule)

        return None

    def add_schedule(self, queue: str, run_id: str, schedule: datetime, content: str):
        f1 = self.root / "schedules" / queue / schedule.isoformat() / run_id
        f1.parent.mkdir(parents=True, exist_ok=False)
        f1.write_text(content)

    def read_schedule(self, queue: str, run_id: str, schedule: datetime):
        f = self.root / "schedules" / queue / schedule.isoformat() / run_id
        if not f.exists():
            raise Exception(f"run not found: {f}")
        ret = f.read_text()
        return None if ret == "" else ret

    def close_schedule(self, queue: str, run_id: str, schedule: datetime):
        f = self.root / "schedules" / queue / schedule.isoformat() / run_id
        if not f.exists():
            raise Exception(f"run not found: {f}")
        f.unlink()

    def active_leases(self, lease_seconds: int):
        f = self.root / "leases"
        ret = []
        for lease in f.iterdir():
            if not lease_held(lease, lease_seconds):
                continue
            ret.append(lease.name)
        return ret

    def acquire_lease(self, run_id: str, lease_seconds: int) -> bool:
        f = self.root / "leases" / run_id
        return leasing_cas(f, "acquire", lease_seconds)

    def renew_lease(self, run_id: str, lease_seconds: int) -> bool:
        f = self.root / "leases" / run_id

        if not f.exists():
            raise Exception(f"lease not found: {f}")

        return leasing_cas(f, "renew", lease_seconds)

    def remove_lease(self, run_id: str, lease_seconds: int):
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
    witness = f.parent / f"{f.stem}.{suffix}"
    witness.write_text(nonce)

    try:
        witness.rename(f)
        return f.read_text() == nonce
    except FileExistsError:
        # another rename has already happened
        witness.unlink()
        return False
