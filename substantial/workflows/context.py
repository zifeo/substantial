import asyncio
from typing import TYPE_CHECKING, Any, Callable, List, Optional
from datetime import timedelta

from substantial.protos import events, metadata

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
        self.metadata = metadata
        # FIXME only events should be required, metadata is only for the run, not the context
        self.events = events

    # def __unqueue_up_to(self, kind: LogKind):
    #    """
    #    unshift, popfront: discard old events till kind is found
    #    """
    #    event = Empty
    #    logs = self.metadata
    #    if kind == LogKind.EventIn or kind == LogKind.EventOut:
    #        logs = self.events
    #    while True:
    #        event = next(logs, Empty)
    #        if event is Empty or event.kind == kind:
    #            if event is not Empty and event.kind == LogKind.Save:
    #                assert isinstance(event.data, SaveData)
    #                if event.data.counter != -1:
    #                    # front value is still in retry mode
    #                    while True:
    #                        # popfront till we get the Save with the highest counter or resolved (-1)
    #                        peek_next = next(logs, Empty)
    #                        if peek_next is Empty:
    #                            break
    #                        elif peek_next.kind == LogKind.Save:
    #                            event = peek_next
    #                            if peek_next.data.counter == -1:
    #                                break
    #                else:
    #                    # front value has been resolved and saved properly
    #                    pass
    #            break
    #    # At this stage `event` should be
    #    # 1. The latest retry Save if kind.Save == Save with highest retry counter
    #    # 2. or the matching kind of Event
    #    # 3. or Empty
    #    return event

    # low-level

    async def save(
        self,
        f: Callable,
        *,
        # compensate_with
        timeout: Optional[timedelta] = None,
        retry_strategy: Optional[RetryStrategy] = None,
    ) -> Any:
        print("SAVE CALLED")
        timeout_secs = timeout.total_seconds() if timeout is not None else None
        evaluator = ValueEval(f, timeout_secs, retry_strategy)
        val = self.__unqueue_up_to(LogKind.Save)
        print("AFTER EVAL")

        if val is Empty:
            val = await evaluator.exec(self, None)
            self.source(LogKind.Save, SaveData(val, -1))
            return val
        else:
            assert isinstance(val.data, SaveData)
            counter = val.data.counter
            payload = val.data.payload
            if counter != -1 and payload is None:
                # retry mode (after replay)
                val = await evaluator.exec(self, counter)
                self.source(LogKind.Save, SaveData(val, -1))
                return payload
            else:
                # resolved mode
                self.source(LogKind.Meta, f"reused {val.data.payload}")
                return payload

    def handle(self, event_name: str, cb: Any) -> None:
        self.source(LogKind.Meta, f"registering... {event_name}")
        self.events[event_name] = cb

    async def ensure(self, f: Callable[[], bool]):
        """Wait for `condition()` to be True"""
        self.source(LogKind.Meta, "waiting...")
        event = self.__unqueue_up_to(LogKind.EventIn)
        if event is not Empty:
            data: EventData = event.data
            callback = self.events.get(data.event_name)
            if callback is not None:
                ret = callback(*tuple(data.args))
                res = self.__unqueue_up_to(LogKind.EventOut)
                if res is not Empty:
                    self.source(LogKind.Meta, f"reused {data.args}")
                else:
                    self.source(LogKind.EventOut, EventData(data.event_name, ret))

        result = f()
        if not result:
            raise Interrupt("wait => condition is still false")
        return result

    # high-level

    async def sleep(self, duration: timedelta) -> Any:
        seconds = duration.total_seconds()
        if seconds <= 0:
            raise AppError(f"Invalid timeout value: {seconds}")

        val = self.__unqueue_up_to(LogKind.Sleep)
        if val is Empty:
            # FIXME this should not sleep but reschedule later
            await asyncio.sleep(seconds)
            self.source(LogKind.Sleep, None)
        else:
            self.source(LogKind.Meta, f"{seconds}s sleep already executed")

    async def receive(self, event_name: str):
        """
        Register a new event that can be triggered from outside the workflow
        """
        proxy = {}
        self.handle(event_name, lambda x: proxy.update(val=x))
        await self.ensure(lambda: "val" in proxy)
        return proxy["val"]

    def cancel_run(self):
        """
        Interrupt after the call
        """
        raise CancelWorkflow(self.run)
