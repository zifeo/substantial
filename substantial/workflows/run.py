import asyncio
from datetime import datetime, timedelta

from substantial.backends.backend import Backend
from substantial.protos import events
from substantial.protos import metadata
from substantial.workflows import Store
from substantial.workflows.context import Context


from substantial.types import (
    CancelWorkflow,
    Interrupt,
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
        await self.backend.add_schedule(self.queue, self.run_id, now, event.to_json())

    async def send(self, name, value=None):
        now = datetime.now()
        event = events.Event(
            at=now,
            send=events.Send(
                name=name,
                value=protobuf.Value(value),
            ),
        )

        print("send", event)
        print("send", event.to_json())
        await self.backend.add_schedule(self.queue, self.run_id, now, event.to_json())

    async def result(self):
        while True:
            events_raw = await self.backend.read_events(self.run_id)
            events_records = events_raw.Records.from_json(events_raw)
            for record in events_records.events:
                if not record.stop:
                    continue

                if record.stop.err:
                    raise Exception(record.stop.err)

                return record.stop.ok

            await asyncio.sleep(1)

    async def replay(self, schedule=None):
        start_at = datetime.now()

        # fetch previous events
        events_raw = await self.backend.read_events(self.run_id)
        events_records = (
            []
            if events_raw is None
            else events_raw.Records.from_json(events_raw).events
        )

        # new on each replay
        metadata_records = [
            metadata.Metadata(at=start_at, info=metadata.Info("replay"))
        ]

        # when there is a new event in schedule
        if schedule is not None:
            new_event = await self.backend.read_schedule(
                self.queue, self.run_id, schedule
            )
            if new_event is not None:
                events_records.append(events.Event().from_json(new_event))
        else:
            schedule = start_at

        ctx = Context(self, metadata_records, events_records)

        workflow = Store.from_run(self.run_id)
        if workflow is None:
            raise Exception(f"Unknown workflow: {self.run_id}")

        try:
            workflow(ctx, **events_records[0].kwargs)
        except Interrupt as interrupt:
            # FIXME need to specify the delta
            print(f"Interrupted: {interrupt.hint}")
            await self.backend.add_schedule(
                self.queue, self.run_id, schedule + timedelta(seconds=10), ""
            )
        except RetryMode:
            print("Retry")
            # FIXME need to specify the delta
            await self.backend.add_schedule(
                self.queue, self.run_id, schedule + timedelta(seconds=10), ""
            )
        except CancelWorkflow as cancel:
            # save cancel events
            print(f"Cancelled workflow: {cancel.hint}")
        except Exception as e:
            metadata_records.append(
                metadata.Metadata(
                    at=datetime.now(),
                    error=metadata.Error(f"error: {e}", "stacktrace", str(type(e))),
                )
            )
            await self.backend.add_schedule(
                self.queue, self.run_id, schedule + timedelta(seconds=10), ""
            )

        finally:
            events_save = events.Records(
                run_id=self.run_id,
                events=events_records,
            )

            metadata_save = metadata.Records(
                run_id=self.run_id, metadata=metadata_records
            )
            await self.backend.write_events(self.run_id, events_save.to_json())
            await self.backend.append_metadata(
                self.run_id, schedule, metadata_save.to_json()
            )
            await self.backend.close_schedule(self.queue, self.run_id, schedule)
