import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import pytest
import uvloop

from substantial.backends.backend import Backend
from substantial.conductor import Conductor
from substantial.workflows.workflow import Workflow
from substantial.protos.events import Event, Records


class StepError(Exception):
    def __init__(self, step: str, message) -> None:
        super().__init__(f"'{step}': {message}")


@dataclass
class EventSend:
    event_name: str
    payload: Optional[Any] = None


class WorkflowTest:
    # test config
    name: str
    event_timeline: Dict[float, EventSend]
    timed_out: bool = False
    expect_timed_out: bool = False
    # workflow
    backend: Optional[Backend] = None
    w_run_id: Optional[str] = None
    w_output: Optional[Any] = None
    w_records: Optional[Records] = None

    def __init__(self) -> None:
        self.event_timeline = dict()

    def error(self, message: str):
        return StepError(self.name, message)

    def step(self, backend: Backend, name: str | None = None):
        """Prepare a new test scope"""
        runner = WorkflowTest()
        runner.backend = backend
        runner.name = name or "<unnamed>"
        return runner

    def events(self, event_timeline: Dict[float, EventSend]):
        self.event_timeline = event_timeline
        return self

    def check_has_run(self):
        assert self.backend is not None
        assert self.w_run_id is not None
        assert self.w_records is not None

    def logs_data_equal(
        self,
        other: List[Event],
    ):
        self.check_has_run()

        proto_events = self.w_records.events
        if len(proto_events) == 0 and len(other) == 0:
            return self

        tf_events = []
        for proto_event in proto_events:
            d = proto_event.to_dict()
            d.pop("at", None)
            tf_events.append(d)

        tf_other = []
        for proto_event in other:
            d = proto_event.to_dict()
            d.pop("at", None)
            tf_other.append(d)

        assert tf_events == tf_other

        return self

    def expects_timeout(self):
        self.expect_timed_out = True
        return self

    async def exec_workflow(
        self, workflow: Workflow, timeout_secs: Union[float, None] = 120
    ):
        assert self.backend is not None

        substantial = Conductor(self.backend)
        substantial.register(workflow)

        w = await substantial.start(workflow)  # the actual workflow run
        self.w_run_id = True

        async def event_timeline():
            time_prev = 0
            for t, event in self.event_timeline.items():
                delta_time = t - time_prev
                time_prev = t
                await asyncio.sleep(delta_time)
                await w.send(event.event_name, event.payload)

        async def resolve_output():
            return [await w.result()]

        # event poller task
        agent_task = substantial.run()

        try:
            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(event_timeline()),
                    asyncio.create_task(resolve_output()),
                ],
                timeout=timeout_secs,
            )

            if len(pending) > 0:
                raise TimeoutError(f"{timeout_secs}s exceeded")

            self.w_records = await self.backend.read_events(w.run_id)
            while len(done) > 0:  # done set has random order
                item = await done.pop()
                if isinstance(item, list):
                    self.w_output = item[0]
                    break

        except TimeoutError:
            self.timed_out = True
            if not self.expect_timed_out:
                raise
        except:
            raise
        finally:
            agent_task.cancel()

        return self


def make_sync(fn: Any) -> Any:
    # Naive impl will run it in the ongoing event loop
    # return asyncio.run(fn())
    def syncified():
        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            ret = runner.run(fn())
        return ret

    return syncified


async_test = pytest.mark.asyncio(scope="function")
