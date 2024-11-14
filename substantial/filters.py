from dataclasses import dataclass
from datetime import datetime
import json
from typing import TYPE_CHECKING, Dict, List, Union
from substantial.workflows.workflow import Workflow

if TYPE_CHECKING:
    from substantial.conductor import Conductor


@dataclass
class Ok:
    value: any


@dataclass
class Err:
    value: any


Result = Union[Ok, Err, None]


@dataclass
class SearchResult:
    run_id: str
    result: Result
    started_at: Union[datetime, None]
    ended_at: Union[datetime, None]


def unlift(v: any) -> any:
    if isinstance(v, Ok) or isinstance(v, Err):
        return v.value
    return v


class WorkflowFilter:
    def __init__(self, conductor: "Conductor"):
        self.conductor = conductor

    async def related_runs(self, workflow: str | Workflow):
        name = workflow
        if isinstance(workflow, Workflow):
            name = workflow.id
        return await self.conductor.backend.read_workflow_links(name)

    async def list_results(self, workflow: str | Workflow) -> List[SearchResult]:
        run_ids = await self.related_runs(workflow)
        results = []

        for run_id in run_ids:
            record = await self.conductor.backend.read_events(run_id)
            if record is None or len(record.events) == 0:
                results.append(SearchResult(run_id, None, None, None))
                continue

            started_at = None
            for record in record.events:
                if record.is_set("start"):
                    started_at = record.at

                if record.is_set("stop"):
                    if record.stop.is_set("err"):
                        results.append(
                            SearchResult(
                                run_id,
                                Err(json.loads(record.stop.err)),
                                started_at,
                                record.at,
                            )
                        )
                    else:
                        results.append(
                            SearchResult(
                                run_id,
                                Ok(json.loads(record.stop.ok)),
                                started_at,
                                record.at,
                            )
                        )
                    break

        return results

    async def search(
        self, workflow: str | Workflow, query: Dict[str, any]
    ) -> List[SearchResult]:
        results = await self.list_results(workflow)
        filtered = []

        for sresult in results:
            if eval_condition(sresult.result, query):
                filtered.append(sresult)

        return filtered


def same(a: Result, b: Result):
    if not (isinstance(a, Ok) or isinstance(a, Err) or a is None):
        raise ValueError(f"{a} term is not of type Ok, Err or None")

    if not (isinstance(b, Ok) or isinstance(b, Err) or b is None):
        raise ValueError(f"{b} term is not of type Ok, Err or None")

    if not isinstance(a, type(b)):
        return False

    return isinstance(unlift(a), type(unlift(b)))


def eval_condition(result: Result, filter: Dict[str, any]) -> bool:
    for op, value in filter.items():
        # operators
        if op == "and":
            if isinstance(value, list):
                if not all(eval_condition(result, sub_f) for sub_f in value):
                    return False
            else:
                raise ValueError(f"and expects a list, got {type(value)} instead")
        elif op == "or":
            if isinstance(value, list):
                if not any(eval_condition(result, sub_f) for sub_f in value):
                    return False
            else:
                raise ValueError(f"or expects a list, got {type(value)} instead")
        elif op == "not":
            if isinstance(value, list):
                raise ValueError("not expects a dict, got a list instead")

            if eval_condition(result, value):
                return False
        # special
        # TODO: started_at, ended_at (lte?, gte?, eq, ..)
        # term
        elif op == "eq":
            if not (same(result, value) and unlift(result) == unlift(value)):
                return False
        elif op == "gt":
            if not (same(result, value) and unlift(result) > unlift(value)):
                return False
        elif op == "gte":
            if not (same(result, value) and unlift(result) >= unlift(value)):
                return False
        elif op == "lt":
            if not (same(result, value) and unlift(result) < unlift(value)):
                return False
        elif op == "lte":
            if not (same(result, value) and unlift(value) <= unlift(value)):
                return False
        elif op == "in":
            if not (
                same(result, value)
                and isinstance(unlift(result), str)
                and unlift(value) in unlift(result)
            ):
                return False
        else:
            raise ValueError(f"Unknown operator: {op}")

    return True


# l = [Ok(1), Ok(2), Ok(3), Ok(4), Ok("A"), Err("error")]

# apply_filter(l, {
#     "or": [
#         { "eq": Ok(1) },
#         { "gte": Ok(4) },
#         { "not": { "not": {"eq": Ok(2) } } }, # same as { "eq": Ok(2) }
#         { "in": Err("err") }
#     ]
# })

# [Ok(value=1), Ok(value=2), Ok(value=3)]
