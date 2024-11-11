import asyncio
from datetime import datetime, timedelta
import json
from typing import List

from substantial.backends.backend import Backend
from substantial.protos import events
from substantial.protos import metadata
from substantial.workflows import Store
from substantial.workflows.context import Context


from substantial.types import (
    CancelWorkflow,
    DelayMode,
    Interrupt,
    RetryFail,
    RetryMode,
)

import betterproto.lib.google.protobuf as protobuf


class Run:
    def __init__(
        self,
        run_id: str,
        queue: str,
        backend: Backend,
    ):
        self.run_id = run_id
        self.queue = queue
        self.backend = backend

    async def start(self, kwargs=None):
        if kwargs is None:
            kwargs = {}

        now = datetime.now()
        event = events.Event(
            at=now,
            start=events.Start(
                kwargs=protobuf.Struct(kwargs),
            ),
        )

        # FIXME change in in bytes later
        await self.backend.add_schedule(self.queue, self.run_id, now, event)

    async def send(self, name, value=None):
        now = datetime.now()
        event = events.Event(
            at=now,
            send=events.Send(name=name, value=json.dumps(value)),
        )

        print("send", event)
        await self.backend.add_schedule(self.queue, self.run_id, now, event)

    async def result(self):
        while True:
            events_records = await self.backend.read_events(self.run_id)  # can be none?
            if events_records is None:
                await asyncio.sleep(1)
                continue

            for record in events_records.events:
                if not record.is_set("stop"):
                    continue

                if record.stop.is_set("err"):
                    raise Exception(json.loads(record.stop.err))

                return json.loads(record.stop.ok)

            await asyncio.sleep(1)

    async def replay(self, schedule=None):
        start_at = datetime.now()

        # fetch previous events
        records = await self.backend.read_events(self.run_id)
        events_records = [] if records is None else records.events

        # new on each replay
        metadata_records = [
            metadata.Metadata(at=start_at, info=metadata.Info("replay"))
        ]

        stopped_run = execution_has_stopped(events_records)

        if schedule is not None:
            new_event = await self.backend.read_schedule(
                self.queue, self.run_id, schedule
            )
            if new_event is None:
                await self.backend.close_schedule(self.queue, self.run_id, schedule)
            elif not stopped_run:
                events_records.append(new_event)
        else:
            schedule = start_at

        print("=============================== Replay")
        ctx = Context(self, metadata_records, events_records)

        workflow = Store.from_run(self.run_id)
        if workflow is None:
            raise Exception(f"Unknown workflow: {self.run_id}")

        try:
            if not stopped_run:
                ret = await workflow(ctx, **ctx.events[0].start.kwargs.to_dict())
                # Alt impl: ctx.events.append(..)
                await self.backend.add_schedule(
                    self.queue,
                    self.run_id,
                    schedule + timedelta(seconds=0.5),
                    events.Event(stop=events.Stop(ok=json.dumps(ret))),
                )
        except Interrupt as interrupt:
            # FIXME need to specify the delta
            print(f"Interrupted: {interrupt.hint}")
            await self.backend.add_schedule(
                self.queue, self.run_id, schedule + timedelta(seconds=10), None
            )
        except DelayMode as delay:
            print(f"Delay: {delay.hint}")
            await self.backend.add_schedule(
                self.queue, self.run_id, schedule + timedelta(seconds=0.5), None
            )
        except RetryMode as retry:
            # should be max(0, schedule + retry.delta - **dur_next_lease_avail_if_exp** - **poll_interv**)
            await self.backend.add_schedule(
                self.queue, self.run_id, schedule + retry.delta, None
            )
        except RetryFail as fail:
            await self.backend.add_schedule(
                self.queue,
                self.run_id,
                schedule + timedelta(seconds=0.5),
                events.Event(stop=events.Stop(err=json.dumps(fail.error_message))),
            )
        except CancelWorkflow as cancel:
            # TODO: save cancel events
            print(f"Cancelled workflow: {cancel.hint}")
        except Exception as e:
            metadata_records.append(
                metadata.Metadata(
                    at=datetime.now(),
                    error=metadata.Error(f"error: {e}", "stacktrace", str(type(e))),
                )
            )
            await self.backend.add_schedule(
                self.queue, self.run_id, schedule + timedelta(seconds=0.5), None
            )
            raise

        finally:
            events_save = events.Records(
                run_id=self.run_id,
                events=ctx.events,
            )

            metadata_save = metadata.Records(
                run_id=self.run_id, metadata=metadata_records
            )
            await self.backend.write_events(self.run_id, events_save)
            await self.backend.append_metadata(
                self.run_id, schedule, metadata_save.to_json()
            )
            await self.backend.close_schedule(self.queue, self.run_id, schedule)


def execution_has_stopped(records: List[events.Event]):
    # Expected timeline shape:
    #       == Start == .. == Stop == Start == .. == Stop ==
    # Corrupted:
    #   1. Start before closing old:
    #       == Start == Start .. == Stop == ..
    #   2. Stop before start:
    #       == Stop == ..
    life = 0
    has_stopped = False
    for rec in records:
        if rec.is_set("start"):
            if life >= 1:
                raise Exception(
                    "potentially corrupted logs, another run occured yet previous has not stopped",
                    records,
                )
            life += 1
            has_stopped = False
        elif rec.is_set("stop"):
            if life <= 0:
                raise Exception(
                    "potentially corrupted logs, attempt stopping already closed run, or run with a missing start",
                    records,
                )
            life -= 1
            has_stopped = True
    return has_stopped
