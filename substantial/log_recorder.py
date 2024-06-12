import json
from typing import List
from pydantic import RootModel
from pydantic.tools import parse_obj_as
import os

from substantial.types import Log, LogKind

class Recorder:
    action_kinds = [LogKind.Save, LogKind.Sleep]
    event_kinds = [LogKind.EventIn, LogKind.EventOut]

    @staticmethod
    def get_log_path(handle: str):
        location = f"logs/{handle}"
        if os.path.exists(location):
            return location
        with open(location, "w+"):
            pass
        return location

    @staticmethod
    def record(handle: str, log: Log):
        action_kinds = [LogKind.Save, LogKind.Sleep]
        event_kinds = [LogKind.EventIn, LogKind.EventOut]
        if log.kind in (action_kinds + event_kinds):
            Recorder.persist(handle, log)
        # else:
        #     print(f"{log.kind} received but not persisted")

    @staticmethod
    def read_logs(handle: str) -> List[Log]:
        filepath = Recorder.get_log_path(handle)
        logs = []
        with open(filepath, "r") as file:
            count = 0
            while line := file.readline():
                log: Log = parse_obj_as(Log, json.loads(line.rstrip()))
                log.normalize_data()
                logs.append(log)
                count += 1
        return logs

    @staticmethod
    def get_recorded_runs(handle: str) -> List[Log]:
        logs = Recorder.read_logs(handle)
        return list(filter(lambda l: l.kind in Recorder.action_kinds, logs))

    @staticmethod
    def get_recorded_events(handle: str) -> List[Log]:
        logs = Recorder.read_logs(handle)
        return list(filter(lambda l: l.kind in Recorder.event_kinds, logs))

    @staticmethod
    def persist(handle: str, log: Log):
        with open(f"logs/{handle}", "a+") as file:
            file.write(f"{RootModel[Log](log).model_dump_json()}\n")

    @staticmethod
    def recover_from_file(filename: str, handle: str):
        if os.path.exists(filename):
            with open(filename, "r") as file:
                count = 0
                print(f"[!] Loading logs from {filename} for {handle}")
                while line := file.readline():
                    log: Log = parse_obj_as(Log, json.loads(line.rstrip()))
                    log.normalize_data()
                    log.handle = handle # force overwrite
                    Recorder.record(handle, log)
                    count += 1
                print(f"Read {count} lines")
