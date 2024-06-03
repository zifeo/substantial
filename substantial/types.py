
import asyncio
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from enum import Enum
import inspect
import json
import math
from typing import Any, Callable, Union


class LogKind(str, Enum):
    Save = "save"
    Sleep = "sleep"
    EventIn = "event_in"
    EventOut = "event_out"
    Meta = "meta"


@dataclass
class Log:
    handle: str
    kind: LogKind
    data: Union[Any, None]
    at: datetime = datetime.now()

    def from_dict(value: any) -> 'Log':
        ftypes = {f.name: f.type for f in fields(Log)}
        attrs = {}
        for f in value:
            fval = value[f]
            ftype = ftypes[f]
            if ftype is datetime:
                attrs[f] = datetime.strptime(fval, "%Y-%m-%d %H:%M:%S.%f")
            else:
                attrs[f] = fval
        return Log(**attrs)
    
    def from_json(value: str) -> 'Log':
        d = json.loads(value)
        return Log.from_dict(d)

    def to_json(self):
        d = asdict(self)
        d["at"] = d["at"].strftime("%Y-%m-%d %H:%M:%S.%f")
        return json.dumps(d)


@dataclass
class Event:
    handle: str
    name: str
    data: Any
    future: asyncio.Future
    at: datetime = datetime.now()

class Interrupt(BaseException):
    hint = ""
    def __init__(self, hint: Union[str, None] = None) -> None:
        self.hint = hint or ""

class CancelWorkflow(BaseException):
    pass

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
            self.initial_backoff_interval = math.max(0, self.max_backoff_interval - 10)

    def linear(self, retries_left: int) -> int:
        """ Scaled timeout in seconds """
        if retries_left <= 0:
            raise Exception("retries_left <= 0")
        dt = self.max_backoff_interval - self.initial_backoff_interval
        return int(((self.max_retries - retries_left) * dt) / self.max_retries)

@dataclass
class Activity:
    fn: Callable[[], Any]
    timeout: Union[int, None]
    retry_strategy: Union[RetryStrategy, None]

    async def exec(self, ctx) -> Any:
        strategy = self.retry_strategy or RetryStrategy(
            max_retries=3,
            initial_backoff_interval=0,
            max_backoff_interval=10
        )
        errors = []
        retries_left = strategy.max_retries

        while retries_left > 0:
            try:
                if ctx.cancelled:
                    raise CancelWorkflow
                val = self.fn()
                if inspect.iscoroutine(val):
                    return await asyncio.wait_for(val, self.timeout)
                return val
            except Exception as e:
                if isinstance(e, CancelWorkflow):
                    raise e

                print(f"Retries => {retries_left}, exec timeout {self.timeout}")
                if isinstance(e, TimeoutError):
                    print("Timeout")

                errors.append(e)
                backoff = strategy.linear(retries_left)
                print(f"backoff {backoff}s: {str(e)}")
                await asyncio.sleep(backoff)
            finally:
                retries_left -= 1
        # raise Interrupt # replay
        raise AppError(e)


Empty: Any = object()
