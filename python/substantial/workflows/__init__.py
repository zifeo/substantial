from typing import Optional
from substantial.workflows.workflow import Workflow


class Store:
    known_workflows = {}

    @staticmethod
    def register(workflow: Workflow):
        Store.known_workflows[workflow.id] = workflow

    @staticmethod
    def from_run(run_id: str) -> Optional[Workflow]:
        workflow_id = run_id.split("-")[0]
        return Store.known_workflows.get(workflow_id)
