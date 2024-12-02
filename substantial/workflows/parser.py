import uuid
import orjson as json

from datetime import datetime
from typing import Any, Type, Optional


def parse(obj: Any, expected_type: Optional[Type] = None) -> Any:
    value = json.loads(obj)
    if expected_type:
        if expected_type == datetime and isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                pass
        elif expected_type == uuid.UUID and isinstance(value, str):
            try:
                return uuid.UUID(value)
            except ValueError:
                pass
    return value
