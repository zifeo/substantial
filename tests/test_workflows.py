from dataclasses import dataclass
from datetime import timedelta, datetime
import orjson as json
from typing import List
import pytest
from substantial.backends.backend import Backend
from substantial.backends.fs import FSBackend
from substantial.backends.redis import RedisBackend

from substantial.types import RetryStrategy
from substantial.workflows.workflow import workflow
from substantial.workflows.context import Context
from tests.utils import EventSend, WorkflowTest, async_test
from substantial.protos.events import Event, Save, Start, Stop
import betterproto.lib.google.protobuf as protobuf


# Further configuration details:
# https://pytest-asyncio.readthedocs.io/en/latest/how-to-guides/multiple_loops.html

# However, each test are still run sequentially


@pytest.fixture
def t():
    return WorkflowTest()


@async_test
async def test_simple_all_backends(t: WorkflowTest):
    @workflow()
    async def simple_workflow(c: Context):
        async def async_op(v):
            return f"C {v}"

        r1 = await c.save(lambda: "A")
        r2 = await c.save(lambda: (lambda: f"B {r1}")())
        r3 = await c.save(lambda: async_op(r2))
        return r3

    backends = [
        FSBackend("./logs"),
        RedisBackend(host="localhost", port=6380, password="password"),
    ]
    for backend in backends:
        s = await t.step(backend).exec_workflow(simple_workflow, 20)
        assert s.w_output == "C B A"
        assert len(s.w_records.events) > 0
        assert s.w_records.events == [
            Event(
                start=Start(kwargs=protobuf.Struct({})),
                at=s.w_records.events[0].at,
            ),
            Event(save=Save(1, json.dumps("A"), -1)),
            Event(save=Save(2, json.dumps("B A"), -1)),
            Event(save=Save(3, json.dumps("C B A"), -1)),
            Event(stop=Stop(ok=json.dumps("C B A"))),
        ]


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
                max_retries=retries,
                initial_backoff_interval=1,
                max_backoff_interval=5,
            ),
        )
        return r1

    backends = [
        FSBackend("./logs"),
        RedisBackend(host="localhost", port=6380, password="password"),
    ]
    for backend in backends:
        s = t.step(backend)
        with pytest.raises(Exception) as info:
            s = await s.exec_workflow(failing_workflow)

        assert info.value.args[0] == "Exception: UNREACHABLE"
        assert len(s.w_records.events) >= retries + 1
        assert s.w_records.events[-1] == Event(
            stop=Stop(err=json.dumps("Exception: UNREACHABLE"))
        )


@async_test
async def test_events_with_sleep(t: WorkflowTest):
    @dataclass
    class State:
        is_cancelled: bool

        def update(self):
            self.is_cancelled = True

    @workflow()
    async def event_workflow(c: Context):
        r1 = await c.save(lambda: "A")
        payload = await c.receive("sayHello")
        await c.sleep(timedelta(seconds=10))
        s = State(False)
        c.handle("cancel", lambda _: s.update())
        if await c.ensure(lambda: s.is_cancelled):
            r3 = await c.save(lambda: f"{payload} B {r1}")
            return r3

    backends: List[Backend] = [
        FSBackend("./logs"),
        RedisBackend(host="localhost", port=6380, password="password"),
    ]
    for backend in backends:
        s = t.step(backend)
        s = await s.events(
            {
                1: EventSend("sayHello", "Hello from outside!"),
                6: EventSend("cancel"),
            }
        ).exec_workflow(event_workflow)

        assert s.w_output == "Hello from outside! B A"

        related_runs = await backend.read_workflow_links(event_workflow.id)
        assert s.w_run_id in related_runs


@pytest.fixture
def utils_state():
    return {"current_time": None, "rand_value": None, "unique_id": None}


@async_test
async def test_utils_methods(t: WorkflowTest, utils_state):
    @workflow()
    async def utils_workflow(context: Context):
        a = await context.utils.now()
        b = await context.utils.random(1, 10)
        c = await context.utils.uuid4()
        x = await context.save(lambda: "2024-11-29T13:52:08.859245")
        if (
            utils_state["current_time"] is None
            and utils_state["rand_value"] is None
            and utils_state["unique_id"] is None
        ):
            utils_state["current_time"] = a
            utils_state["rand_value"] = b
            utils_state["unique_id"] = c
        else:
            assert a == utils_state["current_time"]
            assert b == utils_state["rand_value"]
            assert c == utils_state["unique_id"]
            assert isinstance(a, datetime)
            assert isinstance(x, str)

        await context.sleep(timedelta(seconds=1))
        return a, b, c, x

    backends = [
        FSBackend("./logs"),
        RedisBackend(host="localhost", port=6380, password="password"),
    ]

    for backend in backends:
        s = await t.step(backend).exec_workflow(utils_workflow)
        now, rand, uuid, x = s.w_output

        assert 1 <= rand <= 10
        assert len(str(uuid)) == 36


@pytest.fixture
def account_balance():
    return {"account": 1000}


@async_test
async def test_banking_compensation(t: WorkflowTest, account_balance):
    def risky_transaction():
        raise Exception("Transaction failed")

    @workflow()
    async def banking_workflow(c: Context):
        def credit_account(value: int) -> int:
            return account_balance["account"] - value

        def debit_account(value: int) -> int:
            return account_balance["account"] + value

        c.save(debit_account(4), compensate_with=credit_account(4))
        c.save(debit_account(10), compensate_with=credit_account(10))
        c.save(
            lambda: [
                debit_account(2),
                risky_transaction(),
            ],
            compensate_with=credit_account(2),
        )  # should fail here and start compensate, and rollback(compensate all preveiws steps in LIFO)
        c.save(debit_account(100), compensate_with=credit_account(100))
        return account_balance

    backends = [
        FSBackend("./logs"),
        RedisBackend(host="localhost", port=6380, password="password"),
    ]

    for backend in backends:
        await t.step(backend).exec_workflow(banking_workflow)
        assert account_balance == {"account": 1000}
