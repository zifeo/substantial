
import asyncio
import inspect
import math

from datetime import datetime
from enum import Enum
from typing import Any, Callable, List, Optional, Union
# from pydantic.dataclasses import dataclass # does not work with futures out of the box
from dataclasses import dataclass
import dataclasses

from pydantic import BaseModel


class LogKind(str, Enum):
    Save = "save"
    Sleep = "sleep"
    EventIn = "event_in"
    EventOut = "event_out"
    Meta = "meta"

@dataclass
class EventData:
    event_name: str
    args: List[Any]

@dataclass
class Log:
    handle: str
    kind: LogKind
    data: Union[Any, None]
    at: Optional[datetime] = dataclasses.field(default_factory=lambda: datetime.now())
    def normalize_data(self):
        if isinstance(self.data, dict) and "event_name" in self.data and "args" in self.data:
            self.data = EventData(self.data["event_name"], self.data["args"])

@dataclass
class Event:
    handle: str
    name: str
    data: Any
    future: asyncio.Future
    at: Optional[datetime] = dataclasses.field(default_factory=lambda: datetime.now())

class Interrupt(BaseException):
    hint: str
    def __init__(self, hint: Union[str, None] = None) -> None:
        self.hint = hint or ""

class CancelWorkflow(BaseException):
    hint: str
    def __init__(self, hint: Union[str, None] = None) -> None:
        self.hint = hint or ""

class AppError(BaseException):
    pass

class ProgramError(BaseException):
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
            self.initial_backoff_interval = math.max(0, self.max_backoff_interval - 10)

    def linear(self, retries_left: int) -> int:
        """ Scaled timeout in seconds """
        if retries_left <= 0:
            raise Exception("retries_left <= 0")
        dt = self.max_backoff_interval - self.initial_backoff_interval
        return int(((self.max_retries - retries_left) * dt) / self.max_retries)

@dataclass
class Activity:
    lambda_fn: Callable[[], Any]
    timeout: Union[int, None]
    retry_strategy: Union[RetryStrategy, None]

    async def exec(self) -> Any:
        strategy = self.retry_strategy or RetryStrategy(
            max_retries=3,
            initial_backoff_interval=0,
            max_backoff_interval=10
        )
        errors = []
        retries_left = strategy.max_retries

        while retries_left > 0:
            try:
                op = self.lambda_fn()
                if inspect.iscoroutine(op):
                    return await asyncio.wait_for(op, self.timeout)
                elif not inspect.isfunction(op):
                    return op
                else:
                    raise ProgramError(f"Expected value or coroutine object, got {type(op)} instead")
            except Exception as e:
                print(f"Retries => {retries_left}, exec timeout {self.timeout}")
                # if isinstance(e, TimeoutError):
                #     print("Timeout")

                if not isinstance(e, ProgramError):
                    errors.append(e)
                    backoff = strategy.linear(retries_left)
                    print(f"backoff {backoff}s: {str(e)}")
                    await asyncio.sleep(backoff)
            finally:
                retries_left -= 1
        raise AppError(e)


Empty: Any = object()
