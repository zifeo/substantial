import random
import uuid
import logging
from datetime import datetime, timezone

from .context import Context


class Utils:
    def __init__(self, ctx: Context):
        self.ctx = ctx

    def now(self) -> str:
        return self.ctx.save(lambda: datetime.now(tz=timezone.utc).isoformat())

    def random(self, a: int, b: int) -> int:
        return self.ctx.save(lambda: random.randint(a, b))

    def uuid4(self) -> uuid.UUID:
        return self.ctx.save(lambda: uuid.uuid4())

    @staticmethod
    def log(level, msg, *args, **kwargs) -> None:
        logging.log(level, msg, *args, **kwargs)
