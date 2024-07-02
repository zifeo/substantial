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
        # FIXME check that context is the first argument and preprocess params for Conductor.start to check that they match

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.f(*args, **kwds)


def workflow(
    # global workflow settings
    # timeout
    # default_retry_strategy
):
    def wrapper(f):
        return Workflow(f.__name__, f)

    return wrapper
