from datetime import datetime, timedelta
import pytest
from substantial.filters import Err, Ok, SearchResult, eval_expr
from tests.utils import async_test


@pytest.fixture
def search_results():
    raw_res = [
        Ok(1),
        Ok(2),
        Ok(3),
        None,
        Ok(4),
        Err("fatal: example"),
        Ok(5),
        Err("error: example"),
        None,
    ]

    s_results = []
    start = datetime(2024, 1, 1)
    for i, res in enumerate(raw_res):
        if res is None:
            s_results.append(SearchResult(f"fake_uuid#{i}", res, start, None))
        else:
            end = start + timedelta(days=1)
            s_results.append(SearchResult(f"fake_uuid#{i}", res, start, end))
            if i % 2 == 0:
                # new day at every 2 results
                start = end

    return s_results


@pytest.fixture
def fless_than_3():
    return {"lt": Ok(3)}


@pytest.fixture
def fnested():
    return {
        "or": [
            {
                "and": [
                    {"in": Err("fatal")},
                    {"not": {"eq": Err("error: example")}},
                ]
            },
            {"eq": Ok(1)},
            {"gte": 4},  # equiv. Ok(4)
        ]
    }


@pytest.fixture
def after_d():
    return lambda d: {"or": [{"ended_at": {"gte": Ok(d)}}, {"eq": -1}]}


def unlift_s(s: SearchResult):
    return s.result


@async_test
async def test_unfinished(search_results):
    results = filter(lambda r: eval_expr(r, {"eq": None}), search_results)
    results = list(map(unlift_s, results))
    assert results == [None, None]


@async_test
async def test_errors(search_results):
    with pytest.raises(ValueError) as bad_op:
        results = filter(
            lambda r: eval_expr(r, {"bad_op": {"in": "..."}}), search_results
        )
        _ = list(results)

    assert bad_op.value.args[0] == "Unknown terminal operator: bad_op"

    with pytest.raises(ValueError) as bad_input:
        results = filter(lambda r: eval_expr(r, {"and": {"in": "..."}}), search_results)
        _ = list(results)

    assert bad_input.value.args[0] == "'and' expects a list, got <class 'dict'> instead"


@async_test
async def test_simple_filter(search_results, fless_than_3):
    results = filter(lambda r: eval_expr(r, fless_than_3), search_results)
    results = list(map(unlift_s, results))
    assert results == [Ok(1), Ok(2)]


@async_test
async def test_nested_and_order_preserved(search_results, fnested):
    results = filter(lambda r: eval_expr(r, fnested), search_results)
    results = list(map(unlift_s, results))
    assert results == [Ok(1), Ok(4), Err("fatal: example"), Ok(5)]


@async_test
async def test_dates(search_results, after_d):
    results = filter(
        lambda r: eval_expr(r, after_d(datetime(2024, 1, 5))), search_results
    )
    results = list(map(unlift_s, results))
    assert results == [Err(value="fatal: example"), Ok(5), Err("error: example")]

    # string YYYY-MM-DD HH:MM:SS
    results = filter(
        lambda r: eval_expr(r, {"started_at": {"in": "01-01 00:"}}), search_results
    )
    results = list(map(unlift_s, results))
    assert results == [Ok(1)]
