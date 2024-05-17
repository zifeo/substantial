
import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any


class LogKind(str, Enum):
    Save = "save"
    EventIn = "event_in"
    EventOut = "event_out"
    Meta = "meta"


@dataclass
class Log:
    handle: str
    kind: LogKind
    data: Any
    at: datetime = datetime.now()


@dataclass
class Event:
    handle: str
    name: str
    data: Any
    future: asyncio.Future
    at: datetime = datetime.now()


@dataclass
class State:
    is_cancelled: bool

    def cancel(self):
        self.is_cancelled = True


class Interrupt(BaseException):
    pass


Empty: Any = object()
