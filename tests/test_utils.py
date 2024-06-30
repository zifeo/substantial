import asyncio
import pytest
from substantial.task_queue import MultithreadedQueue

import time

from tests.utils import LogFilter, StepError, WorkflowTest, make_sync, async_test


@make_sync
async def test_test():
    t = WorkflowTest()
    with pytest.raises(StepError) as info:
        t.step("A").logs_data_equal(LogFilter.Runs, [])
    assert info.value.args[0] == "'A': No workflow has been run prior the call"


duration = 3


def sleep_and_id(v):
    time.sleep(duration)
    return v


def a():
    return sleep_and_id(1)


def b():
    return sleep_and_id(2)


def c():
    return sleep_and_id(3)


@async_test
async def test_parallel_static_calls():
    todos = [a, b, c]
    qcount = 2
    # 0s    3s       6s
    # ai---af,ci-----cf-------->
    # bi-----bf---------------->
    start_time = time.time()
    async with MultithreadedQueue(qcount) as send:
        results = await asyncio.gather(*[send(todo) for todo in todos])
    end_time = time.time()

    assert results == [1, 2, 3]

    diff = end_time - start_time
    assert diff < 6.2


@async_test
async def test_parallel_dynamic_calls():
    # This will only work out of the box with aioprocessing[dill]
    # Otherwise manually dump(here) and load(when running f) with dill
    todos = [lambda: sleep_and_id(i + 1) for i in range(3)]

    start_time = time.time()
    async with MultithreadedQueue(2) as send:
        results = await asyncio.gather(*[send(todo) for todo in todos])

    # arg is frozen right when it's latest(i) + 1
    assert results == [3, 3, 3]

    end_time = time.time()
    diff = end_time - start_time
    assert diff < 6.2


async def d():
    return sleep_and_id(3)


@async_test
async def test_parallel_static_async_hack():
    todos = [make_sync(d), make_sync(d), make_sync(d)]
    start_time = time.time()
    async with MultithreadedQueue(2) as send:
        results = await asyncio.gather(*[send(todo) for todo in todos])
    end_time = time.time()

    assert results == [3, 3, 3]

    diff = end_time - start_time
    assert diff < 6.2
