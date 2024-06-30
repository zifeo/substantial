from pydantic.dataclasses import dataclass


@dataclass
class Ref:
    workflow_id: str
    run_id: str

    def __str__(self) -> str:
        return f"{self.workflow_id}-{self.run_id}"

    async def send(self, event_name, *args):
        from substantial.log_recorder import Recorder
        from substantial.types import (
            EventData,
            Log,
            LogKind,
        )

        event_data = EventData(event_name, list(args))
        Recorder.record(self, Log(self, LogKind.EventIn, event_data))
