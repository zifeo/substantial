from typing import TYPE_CHECKING, Optional

from substantial.workflows.context import Context
from substantial.workflows.ref import Ref

if TYPE_CHECKING:
    from substantial.conductor import SubstantialConductor
    from substantial.workflows.workflow import Workflow

from substantial.log_recorder import Recorder
from substantial.types import (
    Interrupt,
    LogKind,
)


class Run:
    def __init__(
        self,
        workflow: "Workflow",
        run_id: str,
        restore_source_id: Optional[str] = None,
    ):
        self.run_id = run_id
        self.workflow = workflow

        self.replayed = False
        # self.restore_source_id = "example"
        self.restore_source_id = restore_source_id

    @property
    def ref(self) -> Ref:
        return Ref(self.workflow.id, self.run_id)

    async def replay(self, conductor: "SubstantialConductor"):
        print("----------------- replay -----------------")
        if not self.replayed and self.restore_source_id is not None:
            log_path = Recorder.get_log_path(self.restore_source_id)
            Recorder.recover_from_file(log_path, self.ref)
            self.replayed = True

        run_logs = conductor.get_run_logs(self.ref)
        events_logs = conductor.get_event_logs(self.ref)

        ctx = Context(self.ref, conductor.log, run_logs, events_logs)
        ctx.source(LogKind.Meta, "replaying ...")
        try:
            ret = await self.workflow.f(ctx)
            self.replayed = True
        except Interrupt:
            ctx.source(LogKind.Meta, "waiting for condition...")
            raise
        except Exception as e:
            ctx.source(LogKind.Meta, f"error: {e}")
            raise
        finally:
            ctx.source(LogKind.Meta, "replayed")

        return ret
