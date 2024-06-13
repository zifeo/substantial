
import asyncio
import inspect
import math

from datetime import datetime
from enum import Enum
import time
from typing import Any, Callable, List, Optional, Union
from pydantic import field_serializer, field_validator
from pydantic.dataclasses import dataclass
import dataclasses

class LogKind(str, Enum):
    Save = "save"
    Sleep = "sleep"
    EventIn = "event_in"
    EventOut = "event_out"
    Meta = "meta"

@dataclass
class EventData:
    event_name: str
    args: Optional[List[Any]]

@dataclass
class SaveData:
    payload: Any
    counter: int

LogData = Optional[Union[str, SaveData, EventData]]

@dataclass
class Log:
    _handle: str
    kind: LogKind
    data: LogData
    at: Optional[datetime] = dataclasses.field(default_factory=lambda: datetime.now())

@dataclass
class Event:
    handle: str
    name: str
    data: Any
    future: Any # asyncio.Future
    at: Optional[datetime] = dataclasses.field(default_factory=lambda: datetime.now())

    @field_serializer("future")
    @classmethod
    def serialize_future(cls, val, _info):
        return None

    @classmethod
    @field_validator("future")
    def validate_future(cls, val, _info):
        return val

class Interrupt(BaseException):
    hint: str
    def __init__(self, hint: Union[str, None] = None) -> None:
        self.hint = hint or ""

class RetryMode(BaseException):
    hint: str
    def __init__(self, hint: Union[str, None] = None) -> None:
        self.hint = hint or ""

class CancelWorkflow(BaseException):
    hint: str
    def __init__(self, hint: Union[str, None] = None) -> None:
        self.hint = hint or ""

class AppError(BaseException):
    pass

@dataclass
class RetryStrategy:
    max_retries: int
    initial_backoff_interval: Union[int, None]
    max_backoff_interval: Union[int, None]

    def __post_init__(self):
        if self.max_retries < 1:
            raise AppError("max_retries < 1")
        low = self.initial_backoff_interval
        high = self.max_backoff_interval
        if low is not None and high is not None:
            if low >= high:
                raise AppError("initial_backoff_interval >= max_backoff_interval")
            if low < 0:
                raise AppError("initial_backoff_interval < 0")
        elif low is not None and high is None:
            self.max_backoff_interval = low + 10
        elif low is None and high is not None:
            self.initial_backoff_interval = max(0, self.max_backoff_interval - 10)

    def linear(self, retries_left: int) -> int:
        """ Scaled timeout in seconds """
        if retries_left <= 0:
            raise Exception("retries_left <= 0")
        dt = self.max_backoff_interval - self.initial_backoff_interval
        return int(((self.max_retries - retries_left) * dt) / self.max_retries)

@dataclass
class ValueEval:
    lambda_fn: Callable[[], Any]
    timeout: Union[int, None]
    retry_strategy: Union[RetryStrategy, None]

    async def exec(self, ctx, counter) -> Any:
        strategy = self.retry_strategy or RetryStrategy(
            max_retries=3,
            initial_backoff_interval=0,
            max_backoff_interval=10
        )

        try:
            ctx.source(LogKind.Meta, inspect.getsource(self.lambda_fn))
            before_spawn = time.time()
            op = self.lambda_fn() # this does not account the case when lambda_fn() is not async

            if inspect.iscoroutine(op):
                after_spawn = time.time()
                elapsed_after_spawn = after_spawn - before_spawn
                timeout = (
                    None if self.timeout is None
                    else max(0.0001, self.timeout - elapsed_after_spawn)
                )
                return await asyncio.wait_for(op, timeout)
            elif not inspect.isfunction(op):
                after_spawn = time.time()
                elapsed_after_spawn = after_spawn - before_spawn
                if self.timeout is not None and elapsed_after_spawn > self.timeout:
                    pass # raise? in a way, op has been resolved at this stage
                return op
            else:
                raise Exception(f"Expected value or coroutine object, got {type(op)} instead")
        except Exception as e:
            counter = counter or 1
            retries_left = strategy.max_retries - counter
            if retries_left > 0:
                ctx.source(LogKind.Save, SaveData(None, counter + 1))
                backoff = strategy.linear(retries_left)
                await asyncio.sleep(backoff)
                raise RetryMode
            else:
                raise e


Empty: Any = object()
