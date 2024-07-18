from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Union
from uuid import uuid4

import redis
from substantial.backends.backend import Backend
from substantial.protos.metadata import Metadata
from substantial.protos.events import Event, Records


class RedisBackend(Backend):
    def __init__(self, host: str, port: int, **kwargs):
        self.redis = redis.Redis(host=host, port=port, decode_responses=True, **kwargs)

    async def read_events(self, run_id: str) -> Union[Records, None]:
        key = "_".join(["runs", run_id, "events"])
        val = self.redis.get(key)
        return None if val is None else Records().from_json(val)

    async def write_events(self, run_id: str, content: Records) -> None:
        key = "_".join(["runs", run_id, "events"])
        self.redis.set(key, content.to_json(indent=4))

    async def read_all_metadata(self, run_id: str) -> List[str]:
        base_key = "_".join(["runs", run_id, "logs"]) # queue
        ret = []
        for schedule in self.redis.lrange(base_key, 0, -1):
            log = self.redis.get("_".join[run_id, schedule])
            # could be polymorphic
            ret.append(log)
        return ret

    async def append_metadata(self, run_id: str, schedule: datetime, content: str):
        base_key = "_".join(["runs", run_id, "logs"]) # queue
        sched_key = "_".join[run_id, schedule.isoformat()]
        # TODO: make this transactional?
        self.redis.lpush(base_key, sched_key)
        self.redis.set(sched_key, content)

    async def next_run(self, queue: str, excludes: list[str])  -> Union[Tuple[str, datetime], None]:
        q_key = "_".join(["schedules", queue])  # priority queue
        excludes_set = set(excludes)

        for schedule in self.redis.zrange(q_key, 0, -1):
            for run_id in self.redis.zrange(schedule, 0, -1):
                if run_id not in excludes_set:
                    return run_id, datetime.fromisoformat(schedule)

        return None

    async def add_schedule(
        self, queue: str, run_id: str, schedule: datetime, content: Union[Event, None]
    ) -> None:
        q_key = "_".join(["schedules", queue])  # priority queue

        # avoid having a bunch of replays happening
        for sched in self.redis.zrange(q_key, 0, -1):
            for planned_key in self.redis.zrange(sched, 0, -1):
                _dt, planned_id = planned_key.split("_") 
                planned_date = datetime.fromisoformat(sched)
                if planned_id == run_id and planned_date <= schedule:
                    event = await self.read_schedule(queue, run_id, planned_date)
                    if event is None: # event => None == scheduled replays..
                        await self.close_schedule(queue, run_id, planned_date)

        sched_key = schedule.isoformat()
        # TODO: batch as one trip + make transactional
        self.redis.zadd(q_key, { sched_key: 0 })
        self.redis.zadd(sched_key, { run_id: schedule.timestamp() })
        self.redis.set(
            "_".join([sched_key, run_id]), 
            "" if content is None else content.to_json()
        )

    async def read_schedule(self, queue: str, run_id: str, schedule: datetime) -> Union[Event, None]:
        q_key = "_".join(["schedules", queue])
        ret = self.redis.get("_".join([q_key, schedule, run_id]))
        if ret is None:
            raise Exception(f"run not found: {run_id}")
        return None if ret == "" else Event().from_json(ret)

    async def close_schedule(self, queue: str, run_id: str, schedule: datetime) -> None:
        q_key = "_".join(["schedules", queue])
        key = "_".join([schedule.isoformat(), run_id])

        # TODO: make transactional
        self.redis.zrem(q_key, key)
        self.redis.delete(key)

        print(f"closed {key} refered within queue {q_key}")

    # TODO
    async def active_leases(self, lease_seconds: int) -> List[str]:
        f = Path("logs") / "leases"
        ret = []
        for lease in f.iterdir():
            if not lease_held(lease, lease_seconds):
                continue
            ret.append(lease.name)
        return ret

    async def acquire_lease(self, run_id: str, lease_seconds: int) -> bool:
        f = Path("logs") / "leases" / run_id
        return leasing_cas(f, "acquire", lease_seconds)

    async def renew_lease(self, run_id: str, lease_seconds: int) -> bool:
        f = Path("logs") / "leases" / run_id

        if not f.exists():
            raise Exception(f"lease not found: {f}")

        return leasing_cas(f, "renew", lease_seconds)

    async def remove_lease(self, run_id: str, lease_seconds: int):
        f = Path("logs") / "leases" / run_id

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
