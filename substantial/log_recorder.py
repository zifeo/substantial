from abc import ABC, abstractmethod
from substantial.workflows.ref import Ref
import json
from typing import List
from pydantic import TypeAdapter

import os

from substantial.types import Log, LogKind


class LogSource(ABC):
    """
    Interface that provide ways to read/write into a given log source.
    """

    @staticmethod
    @abstractmethod
    def get_logs(query: str) -> List[Log]:
        """
        Return all logs of any kind from a source
        """
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def get_recorded_runs(ref: str) -> List[Log]:
        """
        Return all logs that is not meta or event from a source
        """
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def get_recorded_events(ref: str) -> List[Log]:
        """
        Return all event logs from a source
        """
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def persist(ref: str, log: Log):
        """
        Write/Serialize log into a source
        """
        raise NotImplementedError()


class Recorder(LogSource):
    """
    `LogSource` implementation that uses files as log source
    """

    action_kinds = [LogKind.Save, LogKind.Sleep]
    event_kinds = [LogKind.EventIn, LogKind.EventOut]

    @staticmethod
    def get_log_path(ref: Ref):
        location = f"logs/{ref}"
        if os.path.exists(location):
            return location
        with open(location, "w+"):
            pass
        return location

    @staticmethod
    def record(ref: Ref, log: Log):
        if log.kind in (Recorder.action_kinds + Recorder.event_kinds):
            Recorder.persist(ref, log)
        else:
            print(f"[!] Received {log.kind} but it was not persisted")

    @staticmethod
    def get_logs(ref: Ref) -> List[Log]:
        filepath = Recorder.get_log_path(ref)
        logs = []
        with open(filepath, "r") as file:
            count = 0
            while line := file.readline():
                dc = json.loads(line.rstrip())
                dc["_ref"] = ref  # row does not have _ref field
                log = TypeAdapter(Log).validate_python(dc)
                logs.append(log)
                count += 1
        return logs

    # Note:
    # In this approach, events and actions writes into the same file hence the filter
    # One might think of calling `get_recorded_runs`/`get_recorded_events` as costly as a web request

    @staticmethod
    def get_recorded_runs(ref: Ref) -> List[Log]:
        logs = Recorder.get_logs(ref)
        return list(filter(lambda log: log.kind in Recorder.action_kinds, logs))

    @staticmethod
    def get_recorded_events(ref: Ref) -> List[Log]:
        logs = Recorder.get_logs(ref)
        return list(filter(lambda log: log.kind in Recorder.event_kinds, logs))

    @staticmethod
    def persist(ref: Ref, log: Log):
        with open(f"logs/{ref}", "a+") as file:
            row = TypeAdapter(Log).dump_json(log, exclude=["_ref"]).decode("utf-8")
            file.write(f"{row}\n")

    @staticmethod
    def recover_from_file(filename: str, ref: Ref):
        """Restore existing logs into a new log file associated with ref"""
        if os.path.exists(filename):
            with open(filename, "r") as file:
                count = 0
                print(f"[!] Loading logs from {filename} for {ref}")
                while line := file.readline():
                    dc = json.loads(line.rstrip())
                    dc["_ref"] = ref
                    log: Log = TypeAdapter(Log).validate_python(dc)
                    Recorder.record(ref, log)
                    count += 1
                print(f"Read {count} lines")
