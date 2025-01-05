from dataclasses import dataclass
from datetime import datetime
import orjson as json
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
            if eval_expr(sresult, query):
                filtered.append(sresult)

        return filtered


def unlift_r(v: any) -> any:
    if isinstance(v, Ok) or isinstance(v, Err):
        return v.value
    return v


def is_result(v: any):
    return isinstance(v, Ok) or isinstance(v, Err) or v is None


def same(a: Result, b: Result):
    if not is_result(a):
        raise ValueError(f"{a} term is not of type Ok, Err or None")

    if not is_result(b):
        raise ValueError(f"{b} term is not of type Ok, Err or None")

    if not isinstance(a, type(b)):
        # Err != Ok
        return False

    return isinstance(unlift_r(a), type(unlift_r(b)))


def eval_expr(s_result: SearchResult, filter: Dict[str, any]) -> bool:
    for op, value in filter.items():
        # node operators
        if op == "and" or op == "or":
            if isinstance(value, list):
                if None in value:
                    raise ValueError(f"'{op}' operand cannot be None")

                f = all if op == "and" else any
                if not f(eval_expr(s_result, sub_f) for sub_f in value):
                    return False
            else:
                raise ValueError(f"'{op}' expects a list, got {type(value)} instead")
        elif op == "not":
            if value is None or isinstance(value, list):
                raise ValueError(f"'not' expects a dict, got a {type(value)} instead")
            if eval_expr(s_result, value):
                return False
        # special values
        elif op == "started_at" or op == "ended_at":
            discr = s_result.started_at if op == "started_at" else s_result.ended_at
            term = SearchResult(s_result.run_id, None, None, None)
            if discr is not None:
                term.result = Ok(discr)
            return eval_term(term, filter[op])
        # terminal operators
        else:
            if not eval_term(s_result, filter):
                return False

    return True


def eval_term(s_result: SearchResult, filter: Dict[str, any]) -> bool:
    result = s_result.result
    for op, term in filter.items():
        if not is_result(term):
            # Allow { "op": x } -> { "op": Ok(x) }
            term = Ok(term)

        if op == "eq":
            if not (same(result, term) and unlift_r(result) == unlift_r(term)):
                return False
        elif op == "gt":
            if not (same(result, term) and unlift_r(result) > unlift_r(term)):
                return False
        elif op == "gte":
            if not (same(result, term) and unlift_r(result) >= unlift_r(term)):
                return False
        elif op == "lt":
            if not (same(result, term) and unlift_r(result) < unlift_r(term)):
                return False
        elif op == "lte":
            if not (same(result, term) and unlift_r(term) <= unlift_r(term)):
                return False
        elif op == "in" or op == "contains":
            u_tmp = unlift_r(result)
            if isinstance(u_tmp, datetime):
                result = Ok(str(u_tmp))
            # Examples:
            # r=[1,2] contains t=1 or r=1 in t=[1,2]
            # t='h' in r='hello' or r='hello' contains t='h'
            val, container = (term, result) if op == "contains" else (result, term)
            val, container = unlift_r(val), unlift_r(container)

            if not generic_includes(val, container):
                return False
        else:
            raise ValueError(
                f"Unknown terminal operator: {op}, must be eq, gt, gte, lt, lte, in or contains"
            )

    return True


def generic_includes(val, container):
    try:
        if not (
            isinstance(container, list)
            or isinstance(container, dict)
            or (isinstance(val, str) and isinstance(container, str))
        ):
            return False
        if isinstance(val, dict) and isinstance(container, dict):
            # { x: {a: 3} } in { x: {a: 3}, y: 4 } => True
            for k, v in val.items():
                if k not in container or v != container.get(k):
                    return False
            return True
        return val in container
    except Exception:
        return False
