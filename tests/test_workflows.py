import asyncio
from datetime import timedelta
import time
from typing import List
import pytest
from dataclasses import asdict, dataclass
from substantial.task_queue import MultithreadedQueue
from substantial.types import Log, RetryStrategy, SaveData
from tests.utils import EventSend, LogFilter, WorkflowTest, make_sync, async_test

from substantial.workflow import workflow, Context

# Further configuration details:
# https://pytest-asyncio.readthedocs.io/en/latest/how-to-guides/multiple_loops.html

# However, each test are still run sequentially


@pytest.fixture
def t():
    return WorkflowTest()


@async_test
async def test_simple(t: WorkflowTest):
    @workflow()
    async def simple_workflow(c: Context):
        async def async_op(v):
            return f"C {v}"

        r1 = await c.save(lambda: "A")
        r2 = await c.save(lambda: (lambda: f"B {r1}")())
        r3 = await c.save(lambda: async_op(r2))
        return r3

    s = await t.step().exec_workflow(simple_workflow, 3)
    (
        s.logs_data_equal(LogFilter.Runs, ["A", "B A", "C B A"]).logs_data_equal(
            LogFilter.Events, []
        )
    )


@async_test
async def test_events(t: WorkflowTest):
    @dataclass
    class State:
        is_cancelled: bool

        def update(self, *_):
            self.is_cancelled = True

    @workflow()
    async def event_workflow(c: Context):
        r1 = await c.save(lambda: "A")
        payload = await c.event("sayHello")

        s = State(False)
        c.register("cancel", s.update)
        if await c.wait_on(lambda: s.is_cancelled):
            r3 = await c.save(lambda: f"{payload} B {r1}")
        return r3

    s = t.step()
    s = await s.events(
        {1: EventSend("sayHello", "Hello from outside!"), 6: EventSend("cancel")}
    ).exec_workflow(event_workflow, 10)
    s.logs_data_equal(LogFilter.Runs, ["A", "Hello from outside! B A"])
    assert s.workflow_output == "Hello from outside! B A"


@async_test
async def test_multiple_workflows_parallel(t: WorkflowTest):
    @workflow()
    async def first(c: Context):
        v = await c.save(lambda: "first")
        return v

    @workflow()
    async def second(c: Context):
        v = await c.save(lambda: "second 1")
        v = await c.save(lambda: f"{v} 2")
        await c.sleep(timedelta(seconds=1))
        v = await c.save(lambda: f"{v} 3")
        return v

    @workflow()
    async def third(c: Context):
        v = await c.save(lambda: "third")
        return v

    def exec(wf):
        # curryfy is necessary as dill will freeze
        # the arg to latest seen if we iter through `arg in [first, second]` for example
        async def test():
            s = t.step()
            s = await s.exec_workflow(wf, 3)
            return len(s.get_logs(LogFilter.Runs))

        return test

    todos = [make_sync(exec(wf)) for wf in [first, second, third]]
    start_time = time.time()
    async with MultithreadedQueue(2) as send:
        log_sizes = await asyncio.gather(*[send(todo) for todo in todos])
    end_time = time.time()

    duration = end_time - start_time
    assert log_sizes == [1, 4, 1]
    # 0s      3s       6s
    # 1i --- 1f,3i --- 3f --->
    # 2i --- 2f --------->
    assert duration < 6.2


@async_test
async def test_failing_workflow_with_retry(t: WorkflowTest):
    def failing_op():
        raise Exception("UNREACHABLE")

    retries = 3

    @workflow()
    async def failing_workflow(c: Context):
        await c.save(lambda: "A")
        r1 = await c.save(
            lambda: failing_op(),
            retry_strategy=RetryStrategy(
                max_retries=retries, initial_backoff_interval=1, max_backoff_interval=5
            ),
        )
        return r1

    s = t.step()
    s = await s.expects_timeout().exec_workflow(failing_workflow, 5)
    retries_accounting_first_run = retries - 1
    assert len(s.get_logs(LogFilter.Runs)) == (1 + retries_accounting_first_run)


@async_test
async def test_timeout_with_retries(t: WorkflowTest):
    async def do_wait(v):
        async with asyncio.timeout(5):
            pass
        return v

    @workflow()
    async def workflow_that_fails(c: Context, name):
        a = await c.save(lambda: "A")
        return await c.save(
            lambda: do_wait(a),
            timeout=timedelta(seconds=1),
            retry_strategy=RetryStrategy(
                max_retries=4, initial_backoff_interval=1, max_backoff_interval=5
            ),
        )

    s = t.step()
    s = await s.expects_timeout().exec_workflow(workflow_that_fails, 10)
    save_logs: List[Log] = list(
        filter(lambda log: isinstance(log.data, SaveData), s.get_logs(LogFilter.Runs))
    )
    save_datas = [asdict(log.data) for log in save_logs]
    assert save_datas == [
        {"counter": -1, "payload": "A"},  # -1 for resolved
        {"counter": 2, "payload": None},  # 2nd retry (counter: 1 is not recorded)
        {"counter": 3, "payload": None},  # 3rd retry
        {"counter": 4, "payload": None},  # 4th retry
    ]
