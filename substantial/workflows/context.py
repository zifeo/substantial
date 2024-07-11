import asyncio
from dataclasses import dataclass
import json
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union
from datetime import timedelta

from substantial.protos import events, metadata
from betterproto.lib.google.protobuf import Value

if TYPE_CHECKING:
    from substantial.workflows.run import Run

from substantial.types import (
    AppError,
    CancelWorkflow,
    Interrupt,
    ValueEval,
    Empty,
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

    def source(self, event: events.Event):
        self.events.append(event)

    # low-level

    async def save(
        self,
        f: Callable,
        *,
        # compensate_with
        timeout: Optional[timedelta] = None,
        retry_strategy: Optional[RetryStrategy] = None,
    ) -> Any:
        timeout_secs = timeout.total_seconds() if timeout is not None else None
        evaluator = ValueEval(f, timeout_secs, retry_strategy)

        saved = None
        save_records = filter(lambda e: "saved" in e.to_dict(), self.events)
        # FIXME: no reuse happening.. saved is always None
        # print(self.events)
        for record in reversed(list(save_records)):
            if record.save.counter == -1:
                saved = record.save
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
            val = await evaluator.exec(self, None)
            self.source(events.Event(save=events.Save(json.dumps(val), -1)))
            return val
        else:
            if saved.counter != -1:
                # retry mode (after replay)
                val = await evaluator.exec(self, saved.counter)
                self.source(events.Event(save=events.Save(json.dumps(val), -1)))
                return saved.value
            else:
                # resolved mode
                print(f"reused {saved.value.to_json()}")
                return saved.value

    def handle(self, event_name: str, cb: Callable):
        for record in self.events:
            if "send" in record.to_dict() and event_name == record.send.name:
                payload = json.loads(record.send.value)
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

    # async def sleep(self, duration: timedelta) -> Any:
    #     seconds = duration.total_seconds()
    #     if seconds <= 0:
    #         raise AppError(f"Invalid timeout value: {seconds}")

    #     val = self.__unqueue_up_to(LogKind.Sleep)
    #     if val is Empty:
    #         # FIXME this should not sleep but reschedule later
    #         await asyncio.sleep(seconds)
    #         self.source(LogKind.Sleep, None)
    #     else:
    #         self.source(LogKind.Meta, f"{seconds}s sleep already executed")

    async def receive(self, event_name: str):
        """
        Register a new event that can be triggered from outside the workflow
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
