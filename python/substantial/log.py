"""Logs of workflow code (I-14): `logging`, muted while a replay walks the recorded history."""

from __future__ import annotations

import logging
from typing import Any

from substantial.scheduler import Shared

logger = logging.getLogger("substantial.workflow")


class Log:
    """The logger of a run, `c.log`: the methods of a `logging.Logger`, the run in `extra`."""

    def __init__(self, shared: Shared) -> None:
        self._shared = shared

    def log(self, level: int, msg: object, *args: Any, **kwargs: Any) -> None:
        if self._shared.live:
            kwargs.setdefault("extra", {})["run"] = self._shared.run
            kwargs["stacklevel"] = kwargs.get("stacklevel", 1) + 2
            logger.log(level, msg, *args, **kwargs)

    def debug(self, msg: object, *args: Any, **kwargs: Any) -> None:
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: object, *args: Any, **kwargs: Any) -> None:
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: object, *args: Any, **kwargs: Any) -> None:
        self.log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: object, *args: Any, **kwargs: Any) -> None:
        self.log(logging.ERROR, msg, *args, **kwargs)

    def exception(self, msg: object, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("exc_info", True)
        self.log(logging.ERROR, msg, *args, **kwargs)

    def critical(self, msg: object, *args: Any, **kwargs: Any) -> None:
        self.log(logging.CRITICAL, msg, *args, **kwargs)
