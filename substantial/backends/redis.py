from datetime import datetime, timedelta
from typing import List, Tuple, Union

import redis
from substantial.backends.backend import Backend
from substantial.protos.events import Event, Records


class RedisBackend(Backend):
    def __init__(self, host: str, port: int, **kwargs):
        self.redis = redis.Redis(host=host, port=port, decode_responses=True, **kwargs)
        self.separator = ":/"
        self.base_prefix = "substantial"
        # self.redis.flushall()

    # Utils
    def _key(self, *parts: str) -> str:
        invalid_chunks = [part for part in parts if self.separator in part]
        if len(invalid_chunks) > 0:
            raise ValueError(
                f"Fatal: parts {invalid_chunks} cannot contain separator '{self.separator}'"
            )
        return f"{self.base_prefix}{self.separator}{self.separator.join(parts)}"

    def _parts(self, key: str) -> List[str]:
        if not key.startswith(self.base_prefix):
            raise ValueError(
                f"Invalid key '{key}': required prefix '{self.base_prefix}' not present"
            )

        offset = len(self.base_prefix + self.separator)
        return key[offset:].split(self.separator)

    # Backend

    async def read_events(self, run_id: str) -> Union[Records, None]:
        event_key = self._key("runs", run_id, "events")
        val = self.redis.get(event_key)

        return None if val is None else Records().from_json(val)

    async def write_events(self, run_id: str, content: Records) -> None:
        key = self._key("runs", run_id, "events")
        self.redis.set(key, content.to_json(indent=4))

    async def flush(self) -> None:
        self.redis.flush()

    async def read_all_metadata(self, run_id: str) -> List[str]:
        log_key = self._key("runs", run_id, "logs")
        sched_keys = self.redis.lrange(log_key, 0, -1)
        logs = self.redis.mget(sched_keys)
        return logs

    async def append_metadata(self, run_id: str, schedule: datetime, content: str):
        """
        Mental model:
            * runs:/{run_id}:/logs => List keys {run_id}:/{schedule_isoformat}
                - Deref key => metadata content
        """

        log_key = self._key("runs", run_id, "logs")  # queue
        sched_key = self._key(run_id, schedule.isoformat())

        self.redis.register_script("""
            local log_key = KEYS[1]
            local sched_key = KEYS[2]
            local content = ARGV[1]

            redis.call("LPUSH", log_key, sched_key)
            redis.call("SET", sched_key, content)
        """)(keys=[log_key, sched_key], args=[content])

    async def read_workflow_links(self, workflow_name: str) -> List[str]:
        links_key = self._key("links", "runs", workflow_name)
        return self.redis.zrange(links_key, 0, -1)

    async def write_workflow_link(self, workflow_name: str, run_id: str) -> None:
        """
        Mental model:
            * links:/runs:/{workflow_name} => List run_id
                - Used by read_workflow_link
        """

        links_key = self._key("links", "runs", workflow_name)
        self.redis.zadd(links_key, {run_id: 0})

    async def next_run(
        self, queue: str, excludes: list[str]
    ) -> Union[Tuple[str, datetime], None]:
        q_key = self._key("schedules", queue)  # priority queue

        lua_ret = self.redis.register_script("""
            local q_key = KEYS[1]
            local excludes = ARGV
            local schedule_refs = redis.call("ZRANGE", q_key, 0, -1)

            for _, schedule_ref in ipairs(schedule_refs) do
                local run_ids = redis.call("ZRANGE", schedule_ref, 0, -1)
                for _, run_id in ipairs(run_ids) do
                    local is_excluded = false
                    for k = 1, #excludes do
                        if run_id == excludes[k] then
                            is_excluded = true
                            break
                        end
                    end

                    if not is_excluded then
                        return {run_id, schedule_ref}
                    end
                end
            end

            return nil
        """)(keys=[q_key], args=excludes)

        if lua_ret is not None:
            run_id, sched_ref = lua_ret
            schedule = self._parts(sched_ref)[-1]
            return run_id, datetime.fromisoformat(schedule)

        return None

    async def add_schedule(
        self, queue: str, run_id: str, schedule: datetime, content: Union[Event, None]
    ) -> None:
        """
        Mental model:
            * schedules:/{queue} => List sched_ref (ref_:/{run_id}:/{schedule_isoformat})
                - sched_ref => Ordered list of run_id
                - Used by next_run
                - Freed by close_schedule
            * {schedule_isoformat}/{run_id} => schedule payload (e.g. send, start, stop)
                - Used by read_schedule
                - Freed by close_schedule
        """

        q_key = self._key("schedules", queue)  # priority queue

        non_prefixed_sched_ref = schedule.isoformat()
        sched_score = schedule.timestamp()
        sched_key = self._key(non_prefixed_sched_ref, run_id)
        sched_ref = self._key("ref_", run_id, non_prefixed_sched_ref)

        self.redis.register_script("""
            local q_key = KEYS[1]
            local sched_ref = KEYS[2]
            local sched_key = KEYS[3]
            local sched_score = tonumber(ARGV[1])
            local run_id = ARGV[2]
            local content = ARGV[3]

            redis.call("ZADD", q_key, 0, sched_ref)
            redis.call("ZADD", sched_ref, sched_score, run_id)
            redis.call("SET", sched_key, content)
        """)(
            keys=[q_key, sched_ref, sched_key],
            args=[sched_score, run_id, "" if content is None else content.to_json()],
        )

    async def read_schedule(
        self, queue: str, run_id: str, schedule: datetime
    ) -> Union[Event, None]:
        sched_key = self._key(schedule.isoformat(), run_id)
        ret = self.redis.get(sched_key)
        if ret is None:
            raise Exception(f"schedule not found: {sched_key}")
        return None if ret == "" else Event().from_json(ret)

    async def close_schedule(self, queue: str, run_id: str, schedule: datetime) -> None:
        q_key = self._key("schedules", queue)
        non_prefixed_sched_ref = schedule.isoformat()
        sched_key = self._key(non_prefixed_sched_ref, run_id)
        sched_ref = self._key("ref_", run_id, non_prefixed_sched_ref)

        self.redis.register_script("""
            local q_key = KEYS[1]
            local sched_ref = KEYS[2]
            local sched_key = KEYS[3]
            local run_id = ARGV[1]

            redis.call("ZREM", q_key, sched_ref)
            redis.call("ZREM", sched_ref, run_id)
            redis.call("DEL", sched_key)
        """)(keys=[q_key, sched_ref, sched_key], args=[run_id])

        print(f"closed {run_id}")

    # Agent related
    async def active_leases(self, _lease_seconds: int) -> List[str]:
        all_leases_key = self._key("leases")

        lua_ret = self.redis.register_script("""
            local all_leases_key = KEYS[1]

            local lease_refs = redis.call("ZRANGE", all_leases_key, 0, -1)
            local results = {}
            for i, lease_ref in ipairs(lease_refs) do
                local exp_time = redis.call("GET", lease_ref)
                table.insert(results, lease_ref)
                table.insert(results, exp_time)
            end

            return results
        """)(keys=[all_leases_key])

        assert len(lua_ret) % 2 == 0

        active_lease_ids = []
        while len(lua_ret) >= 2:
            lease_ref, exp_time = lua_ret.pop(0), lua_ret.pop(0)
            exp_time = datetime.fromisoformat(exp_time)
            if exp_time > datetime.now():
                run_id = self._parts(lease_ref)[-1]
                active_lease_ids.append(run_id)

        return active_lease_ids

    async def acquire_lease(self, run_id: str, lease_seconds: int) -> bool:
        """
        Mental model:
            * leases => Ordered lease refs (format: lease:/{run_id})
                - Deref lease ref => lease expiration (isoformat date string)
                - Could be overwritten by renew_lease (must exist before renew)
                - Freed by remove_lease
        """

        all_leases_key = self._key("leases")
        lease_ref = self._key("lease", run_id)

        exp_time_str = self.redis.register_script("""
            local all_leases_key = KEYS[1]
            local lease_ref = KEYS[2]
            if redis.call("EXISTS", lease_ref) == 1 then
                if redis.call("ZRANK", all_leases_key, lease_ref) == nil then
                    error("Invalid state: integrity failure, lease ref " .. lease_ref .. " is not an element of " .. all_leases_key)
                end
                return redis.call("GET", lease_ref)                    
            else
                return nil
            end
        """)(keys=[all_leases_key, lease_ref])

        not_held = True
        if exp_time_str is not None:
            not_held = False
            exp_time = datetime.fromisoformat(exp_time_str)
            if exp_time < datetime.now():
                not_held = True

        if not_held:
            now = datetime.now()
            lease_exp = now + timedelta(seconds=lease_seconds)
            lease_exp = lease_exp.isoformat()
            self.redis.register_script("""
                local all_leases_key = KEYS[1]
                local lease_ref = KEYS[2]
                local lease_exp = ARGV[1]

                redis.call("ZADD", all_leases_key, 0, lease_ref)
                redis.call("SET", lease_ref, lease_exp)
            """)(keys=[all_leases_key, lease_ref], args=[lease_exp])
            return True

        return False

    async def renew_lease(self, run_id: str, lease_seconds: int) -> bool:
        lease_ref = self._key("lease", run_id)
        new_lease_exp = datetime.now() + timedelta(seconds=lease_seconds)

        lua_ret = self.redis.register_script("""
            local lease_ref = KEYS[1]
            local new_lease_exp = ARGV[1]

            if redis.call("EXISTS", lease_ref) == 1 then
                redis.call("SET", lease_ref, new_lease_exp)
                return 1
            else
                return 0
            end
        """)(keys=[lease_ref], args=[new_lease_exp])

        if lua_ret == 0:
            raise Exception(f"lease not found {lease_ref}")

        return True

    async def remove_lease(self, run_id: str, _lease_seconds: int):
        all_leases_key = self._key("leases")
        lease_ref = self._key("lease", run_id)

        self.redis.register_script("""
            local all_leases_key = KEYS[1]
            local lease_ref = KEYS[2]

            redis.call("ZREM", all_leases_key, lease_ref)
            redis.call("DEL", lease_ref)
        """)(keys=[all_leases_key, lease_ref])
