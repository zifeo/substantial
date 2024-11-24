import uuid
import orjson as json

from datetime import datetime
from typing import Any


def parse(obj: Any) -> Any:
    value = json.loads(obj)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass

        try:
            return uuid.UUID(value)
        except ValueError:
            pass
    return value
