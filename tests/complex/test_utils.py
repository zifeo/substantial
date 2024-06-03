

import asyncio
import pytest
from substantial.task_queue import MultiTaskQueue

import time

from tests.complex.utils import LogFilter, StepError, WorkflowTest

@pytest.mark.asyncio(scope="module")
async def test_async():
    await asyncio.sleep(1)
    assert 1 + 1 is 2

@pytest.mark.asyncio(scope="module")
async def test_test():
    t = WorkflowTest()
    with pytest.raises(StepError) as info:
        t.step("A").logs_data_equal(LogFilter.runs, [])
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

async def d():
    return sleep_and_id(3)


@pytest.mark.asyncio(scope="module")
async def test_parallel_static_calls():
    todos = [a, b, c]
    qcount = 2
    # 0s    3s       6s    
    # ai---af,ci-----cf--------> 
    # bi-----bf---------------->
    start_time = time.time()
    async with MultiTaskQueue(qcount) as send:
        results = await asyncio.gather(*[send(todo) for todo in todos])
    end_time = time.time()

    assert results == [1, 2, 3]

    diff = end_time - start_time
    expected = duration * qcount
    assert diff - expected < 1




# FIXME: Same as above but dynamically generated tasks, not working, it stucks indefinitely
# if tested separately we get
# Can't pickle local object 'main.<locals>.<listcomp>.<lambda>'
#   File "/usr/lib/python3.11/multiprocessing/reduction.py", line 51, in dumps
#     cls(buf, protocol).dump(obj)
# AttributeError: Can't pickle local object 'main.<locals>.<listcomp>.<lambda>'

# @pytest.mark.asyncio(scope="module")
# async def test_parallel_dynamic_calls():
#     count = 3
#     todos = [lambda: sleep_and_id(i) for i in range(count)]

#     start_time = time.time()
#     async with MultiTaskQueue(2) as send:
#         results = await asyncio.gather(*[send(todo) for todo in todos])

#     assert results == [1, 2, 3]

#     end_time = time.time()
#     diff = end_time - start_time
#     assert diff < (duration * len(todos))





# FIXME: Same as above but using pre-defined coroutines, it stucks indefinitely
# if tested separately we get
#     obj = _ForkingPickler.dumps(obj)
#           ^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/usr/lib/python3.11/multiprocessing/reduction.py", line 51, in dumps
#     cls(buf, protocol).dump(obj)
# TypeError: cannot pickle 'coroutine' object

# @pytest.mark.asyncio(scope="module")
# async def test_parallel_static_async():
#     todos = [d, d, d]
#     qcount = 2
#     start_time = time.time()
#     async with MultiTaskQueue(qcount) as send:
#         results = await asyncio.gather(*[send(todo) for todo in todos])
#     end_time = time.time()

#     assert results == [4, 4, 4]

#     diff = end_time - start_time
#     expected = duration * qcount # should be 6 seconds
#     margin = 1
#     assert diff - expected < margin 