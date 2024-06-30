from typing import Any, Callable
import inspect


class Workflow:
    def __init__(
        self,
        workflow_name: str,
        f: Callable[..., Any],
    ):
        self.id = workflow_name
        self.f = f
        args = inspect.signature(f).parameters
        print(args)
        # FIXME


def workflow(
    # global workflow settings
    # timeout
    # default_retry_strategy
):
    def wrapper(f):
        return Workflow(f.__name__, f)

    return wrapper
