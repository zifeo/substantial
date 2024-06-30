from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4
from substantial.backends.backend import Backend


class FSBackend(Backend):
    """
    This backend is for testing purposes only.
    As there is no order defined in the filesystem, all the runs need to be loaded in memory.
    """

    def __init__(self, root: str):
        self.root = Path(root)
        for d in ["runs", "schedules", "leases"]:
            (self.root / d).mkdir(parents=True, exist_ok=True)

    def read_events(self, run_id: str):
        f = self.root / "runs" / run_id / "events"
        if not f.exists():
            raise Exception(f"events file not found: {f}")

        return f.read_text()

    def write_events(self, run_id: str, content: str):
        f = self.root / "runs" / run_id / "events"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(content)

    def read_logs(self, run_id: str):
        f = self.root / "runs" / run_id / "logs"
        ret = []
        for log in f.iterdir():
            ret.append(log.read_text())
        return ret

    def append_log(self, run_id: str, schedule: datetime, content: str):
        f = self.root / "runs" / run_id / "logs" / schedule.isoformat()
        f.parent.mkdir(parents=True, exist_ok=False)
        f.write_text(content)

    def next_run(self, queue: str, excludes: list[str]):
        f = self.root / "schedules" / queue
        excludes_set = set(excludes)

        for schedule in sorted(f.iterdir()):
            if schedule.name not in excludes_set:
                return schedule.name

        return None

    def schedule_run(self, queue: str, run_id: str, schedule: datetime):
        f = self.root / "schedules" / queue / schedule.isoformat() / run_id
        f.parent.mkdir(parents=True, exist_ok=False)
        f.touch()

    def unschedule_run(self, queue: str, run_id: str, schedule: datetime):
        f = self.root / "schedules" / queue / schedule.isoformat() / run_id
        if not f.exists():
            raise Exception(f"run not found: {f}")
        f.unlink()

    def active_leases(self, lease_seconds: int):
        f = self.root / "leases"
        now = datetime.now()
        ret = []
        for lease in f.iterdir():
            if (
                datetime.fromtimestamp(lease.stat().st_mtime)
                + timedelta(seconds=lease_seconds)
                < now
            ):
                # lease is expired
                continue
            ret.append(lease.name)
        return ret

    def acquire_lease(self, run_id: str, lease_seconds: int) -> bool:
        f = self.root / "leases" / run_id
        return cas(f, "acquire", lease_seconds)

    def renew_lease(self, run_id: str, lease_seconds: int) -> bool:
        f = self.root / "leases" / run_id

        if not f.exists():
            raise Exception(f"lease not found: {f}")

        return cas(f, "renew", lease_seconds)

    def remove_lease(self, run_id: str, lease_seconds: int):
        f = self.root / "leases" / run_id

        if not f.exists():
            raise Exception(f"lease not found: {f}")

        still_holding = cas(f, "remove", lease_seconds)
        if still_holding:
            f.unlink()


def cas(f: Path, suffix: str, lease_seconds: int) -> bool:
    if (
        f.exists()
        and datetime.fromtimestamp(f.stat().st_mtime) + timedelta(seconds=lease_seconds)
        > datetime.now()
    ):
        # lease is already acquired
        return False

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
