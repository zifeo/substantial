from substantial.boundary.v1 import boundary_pb2 as _boundary_pb2
from substantial.boundary.v1 import run_pb2 as _run_pb2
from substantial.layout.v1 import layout_pb2 as _layout_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AgentCommand(_message.Message):
    __slots__ = ["committed", "replay", "scanned", "stop", "stop_replay", "store", "wait_until"]
    class Committed(_message.Message):
        __slots__ = ["cost", "inbox", "n", "run", "won"]
        COST_FIELD_NUMBER: _ClassVar[int]
        INBOX_FIELD_NUMBER: _ClassVar[int]
        N_FIELD_NUMBER: _ClassVar[int]
        RUN_FIELD_NUMBER: _ClassVar[int]
        WON_FIELD_NUMBER: _ClassVar[int]
        cost: _layout_pb2.Cost
        inbox: _containers.RepeatedCompositeFieldContainer[_run_pb2.InboxMessage]
        n: int
        run: str
        won: bool
        def __init__(self, run: _Optional[str] = ..., n: _Optional[int] = ..., won: bool = ..., inbox: _Optional[_Iterable[_Union[_run_pb2.InboxMessage, _Mapping]]] = ..., cost: _Optional[_Union[_layout_pb2.Cost, _Mapping]] = ...) -> None: ...
    class Replay(_message.Message):
        __slots__ = ["due_at", "input", "run"]
        DUE_AT_FIELD_NUMBER: _ClassVar[int]
        INPUT_FIELD_NUMBER: _ClassVar[int]
        RUN_FIELD_NUMBER: _ClassVar[int]
        due_at: _layout_pb2.Time
        input: _run_pb2.RunInput
        run: str
        def __init__(self, run: _Optional[str] = ..., input: _Optional[_Union[_run_pb2.RunInput, _Mapping]] = ..., due_at: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...
    class Scanned(_message.Message):
        __slots__ = ["due", "workflow"]
        DUE_FIELD_NUMBER: _ClassVar[int]
        WORKFLOW_FIELD_NUMBER: _ClassVar[int]
        due: int
        workflow: str
        def __init__(self, workflow: _Optional[str] = ..., due: _Optional[int] = ...) -> None: ...
    class Store(_message.Message):
        __slots__ = ["id", "op"]
        ID_FIELD_NUMBER: _ClassVar[int]
        OP_FIELD_NUMBER: _ClassVar[int]
        id: int
        op: _boundary_pb2.StoreOp
        def __init__(self, id: _Optional[int] = ..., op: _Optional[_Union[_boundary_pb2.StoreOp, _Mapping]] = ...) -> None: ...
    COMMITTED_FIELD_NUMBER: _ClassVar[int]
    REPLAY_FIELD_NUMBER: _ClassVar[int]
    SCANNED_FIELD_NUMBER: _ClassVar[int]
    STOP_FIELD_NUMBER: _ClassVar[int]
    STOP_REPLAY_FIELD_NUMBER: _ClassVar[int]
    STORE_FIELD_NUMBER: _ClassVar[int]
    WAIT_UNTIL_FIELD_NUMBER: _ClassVar[int]
    committed: AgentCommand.Committed
    replay: AgentCommand.Replay
    scanned: AgentCommand.Scanned
    stop: _boundary_pb2.Empty
    stop_replay: str
    store: AgentCommand.Store
    wait_until: _layout_pb2.Time
    def __init__(self, store: _Optional[_Union[AgentCommand.Store, _Mapping]] = ..., replay: _Optional[_Union[AgentCommand.Replay, _Mapping]] = ..., committed: _Optional[_Union[AgentCommand.Committed, _Mapping]] = ..., stop_replay: _Optional[str] = ..., wait_until: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., stop: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., scanned: _Optional[_Union[AgentCommand.Scanned, _Mapping]] = ...) -> None: ...

class AgentConfig(_message.Message):
    __slots__ = ["claim_skew_ms", "claim_ttl_ms", "concurrency", "due_page", "due_pages", "max_idle_ms", "op_retries", "prefix", "recheck_ms", "retention_min_ms", "seed", "settings", "snapshot_every", "store_waits", "tombstone_ms", "wait_seconds", "worker", "workflows"]
    CLAIM_SKEW_MS_FIELD_NUMBER: _ClassVar[int]
    CLAIM_TTL_MS_FIELD_NUMBER: _ClassVar[int]
    CONCURRENCY_FIELD_NUMBER: _ClassVar[int]
    DUE_PAGES_FIELD_NUMBER: _ClassVar[int]
    DUE_PAGE_FIELD_NUMBER: _ClassVar[int]
    MAX_IDLE_MS_FIELD_NUMBER: _ClassVar[int]
    OP_RETRIES_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    RECHECK_MS_FIELD_NUMBER: _ClassVar[int]
    RETENTION_MIN_MS_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_EVERY_FIELD_NUMBER: _ClassVar[int]
    STORE_WAITS_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONE_MS_FIELD_NUMBER: _ClassVar[int]
    WAIT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    WORKFLOWS_FIELD_NUMBER: _ClassVar[int]
    claim_skew_ms: int
    claim_ttl_ms: int
    concurrency: int
    due_page: int
    due_pages: int
    max_idle_ms: int
    op_retries: int
    prefix: str
    recheck_ms: int
    retention_min_ms: int
    seed: bytes
    settings: _run_pb2.RunSettings
    snapshot_every: int
    store_waits: bool
    tombstone_ms: int
    wait_seconds: int
    worker: str
    workflows: _containers.RepeatedCompositeFieldContainer[Served]
    def __init__(self, prefix: _Optional[str] = ..., worker: _Optional[str] = ..., workflows: _Optional[_Iterable[_Union[Served, _Mapping]]] = ..., concurrency: _Optional[int] = ..., claim_ttl_ms: _Optional[int] = ..., claim_skew_ms: _Optional[int] = ..., max_idle_ms: _Optional[int] = ..., due_page: _Optional[int] = ..., due_pages: _Optional[int] = ..., snapshot_every: _Optional[int] = ..., op_retries: _Optional[int] = ..., seed: _Optional[bytes] = ..., settings: _Optional[_Union[_run_pb2.RunSettings, _Mapping]] = ..., store_waits: bool = ..., wait_seconds: _Optional[int] = ..., tombstone_ms: _Optional[int] = ..., retention_min_ms: _Optional[int] = ..., recheck_ms: _Optional[int] = ...) -> None: ...

class AgentRequest(_message.Message):
    __slots__ = ["commit", "drain", "feed", "next", "replay_ended", "shutdown", "wake", "wake_workflow"]
    class Feed(_message.Message):
        __slots__ = ["id", "result"]
        ID_FIELD_NUMBER: _ClassVar[int]
        RESULT_FIELD_NUMBER: _ClassVar[int]
        id: int
        result: _boundary_pb2.OpResult
        def __init__(self, id: _Optional[int] = ..., result: _Optional[_Union[_boundary_pb2.OpResult, _Mapping]] = ...) -> None: ...
    class ReplayEnded(_message.Message):
        __slots__ = ["end", "run"]
        END_FIELD_NUMBER: _ClassVar[int]
        RUN_FIELD_NUMBER: _ClassVar[int]
        end: ReplayEnd
        run: str
        def __init__(self, run: _Optional[str] = ..., end: _Optional[_Union[ReplayEnd, _Mapping]] = ...) -> None: ...
    class RunCommit(_message.Message):
        __slots__ = ["commit", "run"]
        COMMIT_FIELD_NUMBER: _ClassVar[int]
        RUN_FIELD_NUMBER: _ClassVar[int]
        commit: _run_pb2.Commit
        run: str
        def __init__(self, run: _Optional[str] = ..., commit: _Optional[_Union[_run_pb2.Commit, _Mapping]] = ...) -> None: ...
    COMMIT_FIELD_NUMBER: _ClassVar[int]
    DRAIN_FIELD_NUMBER: _ClassVar[int]
    FEED_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIELD_NUMBER: _ClassVar[int]
    REPLAY_ENDED_FIELD_NUMBER: _ClassVar[int]
    SHUTDOWN_FIELD_NUMBER: _ClassVar[int]
    WAKE_FIELD_NUMBER: _ClassVar[int]
    WAKE_WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    commit: AgentRequest.RunCommit
    drain: _layout_pb2.Time
    feed: AgentRequest.Feed
    next: _layout_pb2.Time
    replay_ended: AgentRequest.ReplayEnded
    shutdown: _boundary_pb2.Empty
    wake: _boundary_pb2.Empty
    wake_workflow: str
    def __init__(self, next: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., feed: _Optional[_Union[AgentRequest.Feed, _Mapping]] = ..., commit: _Optional[_Union[AgentRequest.RunCommit, _Mapping]] = ..., replay_ended: _Optional[_Union[AgentRequest.ReplayEnded, _Mapping]] = ..., wake: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., shutdown: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., wake_workflow: _Optional[str] = ..., drain: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...

class AgentResponse(_message.Message):
    __slots__ = ["command", "done", "error"]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    command: AgentCommand
    done: _boundary_pb2.Empty
    error: _boundary_pb2.CoreError
    def __init__(self, error: _Optional[_Union[_boundary_pb2.CoreError, _Mapping]] = ..., command: _Optional[_Union[AgentCommand, _Mapping]] = ..., done: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ...) -> None: ...

class ReplayEnd(_message.Message):
    __slots__ = ["dropped", "idle"]
    DROPPED_FIELD_NUMBER: _ClassVar[int]
    IDLE_FIELD_NUMBER: _ClassVar[int]
    dropped: _boundary_pb2.Empty
    idle: _run_pb2.Idle
    def __init__(self, idle: _Optional[_Union[_run_pb2.Idle, _Mapping]] = ..., dropped: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ...) -> None: ...

class Served(_message.Message):
    __slots__ = ["name", "retention", "unversioned", "versions"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    RETENTION_FIELD_NUMBER: _ClassVar[int]
    UNVERSIONED_FIELD_NUMBER: _ClassVar[int]
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    name: str
    retention: _run_pb2.Retention
    unversioned: bool
    versions: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, name: _Optional[str] = ..., versions: _Optional[_Iterable[int]] = ..., unversioned: bool = ..., retention: _Optional[_Union[_run_pb2.Retention, _Mapping]] = ...) -> None: ...
