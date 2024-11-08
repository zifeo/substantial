from datetime import datetime, timezone
import random
import uuid
import logging


class Utils:
    @staticmethod
    def now() -> str:
        return datetime.now(tz=timezone.utc).isoformat()

    @staticmethod
    def random(a: int, b: int) -> int:
        return random.randint(a, b)

    @staticmethod
    def uuid4() -> uuid.UUID:
        return uuid.uuid4()

    @staticmethod
    def log(level, msg, *args, **kwargs) -> None:
        logging.log(level, msg, *args, **kwargs)
