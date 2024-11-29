import random
import uuid
import logging
from typing import TYPE_CHECKING, Any, Callable, List, Optional
from datetime import datetime, timedelta, timezone

from substantial.protos import events, metadata
from substantial.workflows.parser import parse


if TYPE_CHECKING:
    from substantial.workflows.run import Run

from substantial.types import (
    CancelWorkflow,
    DelayMode,
    Interrupt,
    ValueEval,
    RetryStrategy,
)


class Context:
    def __init__(
        self,
        run: "Run",
        metadata: List[metadata.Metadata],
        events: List[events.Event],
    ):
        self.run = run
        # self.metadata = metadata
        # FIXME only events should be required, metadata is only for the run, not the context
        self.events = events
        self.__id = 0
        self.utils = Utils(self)

    def __next_id(self):
        # FIXME: maybe use lambda hash instead? but how portable that would be?
        # Also this relies on the fact that the Context is recreated each replay
        self.__id += 1
        return self.__id

    def source(self, event: events.Event):
        self.events.append(event)

    # low-level

    async def save(
        self,
        f: Callable,
        *,
        compensate_with: Optional[Callable[[], Any]] = None,
        timeout: Optional[timedelta] = None,
        retry_strategy: Optional[RetryStrategy] = None,
        max_compensation_attempts: int = 3,
    ) -> Any:
        timeout_secs = timeout.total_seconds() if timeout is not None else None
        evaluator = ValueEval(
            f,
            timeout_secs,
            retry_strategy,
            compensate_with,
            max_compensation_attempts,
        )
        save_id = self.__next_id()

        saved = None
        save_records = filter(
            lambda e: e.is_set("save") and save_id == e.save.id, self.events
        )

        for record in save_records:
            if record.save.counter == -1:
                saved = record.save
                break
            else:
                latest = None
                while True:
                    # popfront till we get the Save with the highest counter or resolved (-1)
                    peek_next: events.Event | None = next(save_records, None)
                    if peek_next is None:
                        break
                    else:
                        latest = peek_next
                        if peek_next.save.counter == -1:
                            break
                if latest is not None:
                    saved = latest.save

        if saved is None:
            val = await evaluator.exec(self, save_id, None)
            print(f"Computed id#{save_id}, {val}")
            return val
        else:
            if saved.counter != -1:
                # retry mode (after replay)
                print(f"Retry id#{save_id}, counter={saved.counter}")
                val = await evaluator.exec(self, save_id, saved.counter)
                return val
            else:
                # resolved mode
                print(f"Reused {saved.value} for id#{save_id}")
                return parse(saved.value)

    def handle(self, event_name: str, cb: Callable[[Any], Any]):
        for record in self.events:
            if record.is_set("send") and event_name == record.send.name:
                payload = parse(record.send.value)
                ret = cb(payload)
                return ret
        return None

    async def ensure(self, f: Callable[[], bool]):
        """Wait for `condition()` to be True"""
        result = f()
        if not result:
            raise Interrupt("wait => condition is still false")
        return result

    # high-level

    async def sleep(self, duration: timedelta) -> Any:
        sleep_id = self.__next_id()
        sleep_records = list(
            filter(lambda e: e.is_set("sleep") and sleep_id == e.sleep.id, self.events)
        )

        now = datetime.now(tz=timezone.utc)
        if len(sleep_records) == 0:
            self.source(
                events.Event(
                    sleep=events.Sleep(sleep_id, start=now, end=now + duration)
                )
            )
            raise DelayMode(f"Sleep id#{sleep_id} encoutered")

        for record in sleep_records:
            op_end = record.sleep.end.replace(tzinfo=timezone.utc)
            if now >= op_end:
                print(f"Skip sleep id#{sleep_id}")
                return

        raise DelayMode(f"Sleep id#{sleep_id} not ending yet")

    async def receive(self, event_name: str):
        """
        Wait for events emitted from outside the workflow
        """
        proxy = {}
        self.handle(event_name, lambda payload: proxy.update(val=payload))
        await self.ensure(lambda: "val" in proxy)
        return proxy["val"]

    def cancel_run(self):
        """
        Interrupt after the call
        """
        raise CancelWorkflow(self.run)


class Utils:
    def __init__(self, ctx: Context):
        self.__ctx = ctx

    async def now(self, tz: Optional[timezone] = None) -> datetime:
        now = await self.__ctx.save(lambda: datetime.now(tz))
        return now

    async def random(self, a: int, b: int) -> int:
        return await self.__ctx.save(lambda: random.randint(a, b))

    async def uuid4(self) -> uuid.UUID:
        serialized_uuid = await self.__ctx.save(lambda: uuid.uuid4())
        return serialized_uuid

    @staticmethod
    def log(level, msg, *args, **kwargs) -> None:
        logging.log(level, msg, *args, **kwargs)
