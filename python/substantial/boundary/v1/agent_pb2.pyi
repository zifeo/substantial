from substantial.boundary.v1 import boundary_pb2 as _boundary_pb2
from substantial.boundary.v1 import run_pb2 as _run_pb2
from substantial.layout.v1 import layout_pb2 as _layout_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Served(_message.Message):
    __slots__ = ("name", "versions", "unversioned")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    UNVERSIONED_FIELD_NUMBER: _ClassVar[int]
    name: str
    versions: _containers.RepeatedScalarFieldContainer[int]
    unversioned: bool
    def __init__(self, name: _Optional[str] = ..., versions: _Optional[_Iterable[int]] = ..., unversioned: _Optional[bool] = ...) -> None: ...

class AgentConfig(_message.Message):
    __slots__ = ("prefix", "partitions", "worker", "workflows", "concurrency", "claim_ttl_ms", "claim_skew_ms", "max_idle_ms", "due_page", "due_pages", "snapshot_every", "op_retries", "seed", "settings", "store_waits", "wait_seconds")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    WORKFLOWS_FIELD_NUMBER: _ClassVar[int]
    CONCURRENCY_FIELD_NUMBER: _ClassVar[int]
    CLAIM_TTL_MS_FIELD_NUMBER: _ClassVar[int]
    CLAIM_SKEW_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_IDLE_MS_FIELD_NUMBER: _ClassVar[int]
    DUE_PAGE_FIELD_NUMBER: _ClassVar[int]
    DUE_PAGES_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_EVERY_FIELD_NUMBER: _ClassVar[int]
    OP_RETRIES_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    STORE_WAITS_FIELD_NUMBER: _ClassVar[int]
    WAIT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    partitions: int
    worker: str
    workflows: _containers.RepeatedCompositeFieldContainer[Served]
    concurrency: int
    claim_ttl_ms: int
    claim_skew_ms: int
    max_idle_ms: int
    due_page: int
    due_pages: int
    snapshot_every: int
    op_retries: int
    seed: bytes
    settings: _run_pb2.RunSettings
    store_waits: bool
    wait_seconds: int
    def __init__(self, prefix: _Optional[str] = ..., partitions: _Optional[int] = ..., worker: _Optional[str] = ..., workflows: _Optional[_Iterable[_Union[Served, _Mapping]]] = ..., concurrency: _Optional[int] = ..., claim_ttl_ms: _Optional[int] = ..., claim_skew_ms: _Optional[int] = ..., max_idle_ms: _Optional[int] = ..., due_page: _Optional[int] = ..., due_pages: _Optional[int] = ..., snapshot_every: _Optional[int] = ..., op_retries: _Optional[int] = ..., seed: _Optional[bytes] = ..., settings: _Optional[_Union[_run_pb2.RunSettings, _Mapping]] = ..., store_waits: _Optional[bool] = ..., wait_seconds: _Optional[int] = ...) -> None: ...

class ReplayEnd(_message.Message):
    __slots__ = ("idle", "dropped")
    IDLE_FIELD_NUMBER: _ClassVar[int]
    DROPPED_FIELD_NUMBER: _ClassVar[int]
    idle: _run_pb2.Idle
    dropped: _boundary_pb2.Empty
    def __init__(self, idle: _Optional[_Union[_run_pb2.Idle, _Mapping]] = ..., dropped: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ...) -> None: ...

class AgentRequest(_message.Message):
    __slots__ = ("next", "feed", "commit", "replay_ended", "wake", "shutdown")
    class Feed(_message.Message):
        __slots__ = ("id", "result")
        ID_FIELD_NUMBER: _ClassVar[int]
        RESULT_FIELD_NUMBER: _ClassVar[int]
        id: int
        result: _boundary_pb2.OpResult
        def __init__(self, id: _Optional[int] = ..., result: _Optional[_Union[_boundary_pb2.OpResult, _Mapping]] = ...) -> None: ...
    class RunCommit(_message.Message):
        __slots__ = ("run", "commit")
        RUN_FIELD_NUMBER: _ClassVar[int]
        COMMIT_FIELD_NUMBER: _ClassVar[int]
        run: str
        commit: _run_pb2.Commit
        def __init__(self, run: _Optional[str] = ..., commit: _Optional[_Union[_run_pb2.Commit, _Mapping]] = ...) -> None: ...
    class ReplayEnded(_message.Message):
        __slots__ = ("run", "end")
        RUN_FIELD_NUMBER: _ClassVar[int]
        END_FIELD_NUMBER: _ClassVar[int]
        run: str
        end: ReplayEnd
        def __init__(self, run: _Optional[str] = ..., end: _Optional[_Union[ReplayEnd, _Mapping]] = ...) -> None: ...
    NEXT_FIELD_NUMBER: _ClassVar[int]
    FEED_FIELD_NUMBER: _ClassVar[int]
    COMMIT_FIELD_NUMBER: _ClassVar[int]
    REPLAY_ENDED_FIELD_NUMBER: _ClassVar[int]
    WAKE_FIELD_NUMBER: _ClassVar[int]
    SHUTDOWN_FIELD_NUMBER: _ClassVar[int]
    next: _layout_pb2.Time
    feed: AgentRequest.Feed
    commit: AgentRequest.RunCommit
    replay_ended: AgentRequest.ReplayEnded
    wake: _boundary_pb2.Empty
    shutdown: _boundary_pb2.Empty
    def __init__(self, next: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., feed: _Optional[_Union[AgentRequest.Feed, _Mapping]] = ..., commit: _Optional[_Union[AgentRequest.RunCommit, _Mapping]] = ..., replay_ended: _Optional[_Union[AgentRequest.ReplayEnded, _Mapping]] = ..., wake: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., shutdown: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ...) -> None: ...

class AgentCommand(_message.Message):
    __slots__ = ("store", "replay", "committed", "stop_replay", "wait_until", "stop")
    class Store(_message.Message):
        __slots__ = ("id", "op")
        ID_FIELD_NUMBER: _ClassVar[int]
        OP_FIELD_NUMBER: _ClassVar[int]
        id: int
        op: _boundary_pb2.StoreOp
        def __init__(self, id: _Optional[int] = ..., op: _Optional[_Union[_boundary_pb2.StoreOp, _Mapping]] = ...) -> None: ...
    class Replay(_message.Message):
        __slots__ = ("run", "input")
        RUN_FIELD_NUMBER: _ClassVar[int]
        INPUT_FIELD_NUMBER: _ClassVar[int]
        run: str
        input: _run_pb2.RunInput
        def __init__(self, run: _Optional[str] = ..., input: _Optional[_Union[_run_pb2.RunInput, _Mapping]] = ...) -> None: ...
    class Committed(_message.Message):
        __slots__ = ("run", "n", "won", "inbox")
        RUN_FIELD_NUMBER: _ClassVar[int]
        N_FIELD_NUMBER: _ClassVar[int]
        WON_FIELD_NUMBER: _ClassVar[int]
        INBOX_FIELD_NUMBER: _ClassVar[int]
        run: str
        n: int
        won: bool
        inbox: _containers.RepeatedCompositeFieldContainer[_run_pb2.InboxMessage]
        def __init__(self, run: _Optional[str] = ..., n: _Optional[int] = ..., won: _Optional[bool] = ..., inbox: _Optional[_Iterable[_Union[_run_pb2.InboxMessage, _Mapping]]] = ...) -> None: ...
    STORE_FIELD_NUMBER: _ClassVar[int]
    REPLAY_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_FIELD_NUMBER: _ClassVar[int]
    STOP_REPLAY_FIELD_NUMBER: _ClassVar[int]
    WAIT_UNTIL_FIELD_NUMBER: _ClassVar[int]
    STOP_FIELD_NUMBER: _ClassVar[int]
    store: AgentCommand.Store
    replay: AgentCommand.Replay
    committed: AgentCommand.Committed
    stop_replay: str
    wait_until: _layout_pb2.Time
    stop: _boundary_pb2.Empty
    def __init__(self, store: _Optional[_Union[AgentCommand.Store, _Mapping]] = ..., replay: _Optional[_Union[AgentCommand.Replay, _Mapping]] = ..., committed: _Optional[_Union[AgentCommand.Committed, _Mapping]] = ..., stop_replay: _Optional[str] = ..., wait_until: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., stop: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ...) -> None: ...

class AgentResponse(_message.Message):
    __slots__ = ("error", "command", "done")
    ERROR_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    error: _boundary_pb2.CoreError
    command: AgentCommand
    done: _boundary_pb2.Empty
    def __init__(self, error: _Optional[_Union[_boundary_pb2.CoreError, _Mapping]] = ..., command: _Optional[_Union[AgentCommand, _Mapping]] = ..., done: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ...) -> None: ...
