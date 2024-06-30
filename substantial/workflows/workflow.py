from typing import Any, Callable, Optional
from uuid import uuid4


from substantial.workflows.run import Run


class Workflow:
    def __init__(
        self,
        workflow_name: str,
        f: Callable[..., Any],
        restore_source_id: Optional[str] = None,
    ):
        self.id = workflow_name
        # FIXME
        self.f = f

        # FIXME
        self.restore_source_id = restore_source_id

    def __call__(self):
        run_id = str(uuid4())
        return Run(self, run_id, self.restore_source_id)


def workflow(
    # global workflow settings
    # timeout
    # default_retry_strategy
):
    def wrapper(f):
        return Workflow(f.__name__, f)

    return wrapper
