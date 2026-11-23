from substantial.boundary.v1 import boundary_pb2 as _boundary_pb2
from substantial.layout.v1 import layout_pb2 as _layout_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
RUN_STATUS_BLOCKED: RunStatus
RUN_STATUS_CANCELLED: RunStatus
RUN_STATUS_COMPLETED: RunStatus
RUN_STATUS_FAILED: RunStatus
RUN_STATUS_PENDING: RunStatus
RUN_STATUS_RUNNING: RunStatus
RUN_STATUS_SUSPENDED: RunStatus
RUN_STATUS_UNSPECIFIED: RunStatus
WAITING_BLOCKED: Waiting
WAITING_INBOX: Waiting
WAITING_MESSAGE: Waiting
WAITING_START: Waiting
WAITING_TIME: Waiting
WAITING_UNSPECIFIED: Waiting

class CancelPlan(_message.Message):
    __slots__ = ["chain", "id", "now", "ord", "prefix", "reason"]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    ORD_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    chain: str
    id: str
    now: _layout_pb2.Time
    ord: str
    prefix: str
    reason: str
    def __init__(self, prefix: _Optional[str] = ..., chain: _Optional[str] = ..., reason: _Optional[str] = ..., id: _Optional[str] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., ord: _Optional[str] = ...) -> None: ...

class CreatePlan(_message.Message):
    __slots__ = ["prefix", "probe"]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PROBE_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    probe: str
    def __init__(self, prefix: _Optional[str] = ..., probe: _Optional[str] = ...) -> None: ...

class History(_message.Message):
    __slots__ = ["events", "failed", "no_chain"]
    class Events(_message.Message):
        __slots__ = ["events", "run"]
        EVENTS_FIELD_NUMBER: _ClassVar[int]
        RUN_FIELD_NUMBER: _ClassVar[int]
        events: _containers.RepeatedCompositeFieldContainer[HistoryEvent]
        run: str
        def __init__(self, run: _Optional[str] = ..., events: _Optional[_Iterable[_Union[HistoryEvent, _Mapping]]] = ...) -> None: ...
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    NO_CHAIN_FIELD_NUMBER: _ClassVar[int]
    events: History.Events
    failed: _boundary_pb2.StoreFailure
    no_chain: NoChain
    def __init__(self, events: _Optional[_Union[History.Events, _Mapping]] = ..., no_chain: _Optional[_Union[NoChain, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ...) -> None: ...

class HistoryEvent(_message.Message):
    __slots__ = ["at", "data", "kind", "run", "segment"]
    AT_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    at: _layout_pb2.Time
    data: bytes
    kind: str
    run: str
    segment: int
    def __init__(self, run: _Optional[str] = ..., segment: _Optional[int] = ..., at: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., kind: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class HistoryPlan(_message.Message):
    __slots__ = ["chain", "prefix"]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    chain: str
    prefix: str
    def __init__(self, prefix: _Optional[str] = ..., chain: _Optional[str] = ...) -> None: ...

class NoChain(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class OpenPlan(_message.Message):
    __slots__ = ["prefix", "probe"]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PROBE_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    probe: str
    def __init__(self, prefix: _Optional[str] = ..., probe: _Optional[str] = ...) -> None: ...

class Opened(_message.Message):
    __slots__ = ["config_error", "failed", "layout_error", "open"]
    CONFIG_ERROR_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    LAYOUT_ERROR_FIELD_NUMBER: _ClassVar[int]
    OPEN_FIELD_NUMBER: _ClassVar[int]
    config_error: str
    failed: _boundary_pb2.StoreFailure
    layout_error: str
    open: _boundary_pb2.Empty
    def __init__(self, open: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., layout_error: _Optional[str] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ..., config_error: _Optional[str] = ...) -> None: ...

class PlanInput(_message.Message):
    __slots__ = ["cancel", "create", "history", "open", "read", "result", "retry", "runs", "send", "start", "status"]
    CANCEL_FIELD_NUMBER: _ClassVar[int]
    CREATE_FIELD_NUMBER: _ClassVar[int]
    HISTORY_FIELD_NUMBER: _ClassVar[int]
    OPEN_FIELD_NUMBER: _ClassVar[int]
    READ_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    RETRY_FIELD_NUMBER: _ClassVar[int]
    RUNS_FIELD_NUMBER: _ClassVar[int]
    SEND_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    cancel: CancelPlan
    create: CreatePlan
    history: HistoryPlan
    open: OpenPlan
    read: ReadPlan
    result: ResultPlan
    retry: RetryPlan
    runs: RunsPlan
    send: SendPlan
    start: StartPlan
    status: StatusPlan
    def __init__(self, send: _Optional[_Union[SendPlan, _Mapping]] = ..., create: _Optional[_Union[CreatePlan, _Mapping]] = ..., open: _Optional[_Union[OpenPlan, _Mapping]] = ..., start: _Optional[_Union[StartPlan, _Mapping]] = ..., result: _Optional[_Union[ResultPlan, _Mapping]] = ..., status: _Optional[_Union[StatusPlan, _Mapping]] = ..., cancel: _Optional[_Union[CancelPlan, _Mapping]] = ..., read: _Optional[_Union[ReadPlan, _Mapping]] = ..., runs: _Optional[_Union[RunsPlan, _Mapping]] = ..., history: _Optional[_Union[HistoryPlan, _Mapping]] = ..., retry: _Optional[_Union[RetryPlan, _Mapping]] = ...) -> None: ...

class PlanRequest(_message.Message):
    __slots__ = ["feed", "next"]
    FEED_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIELD_NUMBER: _ClassVar[int]
    feed: _boundary_pb2.OpResult
    next: _boundary_pb2.Empty
    def __init__(self, next: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., feed: _Optional[_Union[_boundary_pb2.OpResult, _Mapping]] = ...) -> None: ...

class PlanResponse(_message.Message):
    __slots__ = ["done", "error", "step"]
    DONE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    done: _boundary_pb2.Empty
    error: _boundary_pb2.CoreError
    step: PlanStep
    def __init__(self, error: _Optional[_Union[_boundary_pb2.CoreError, _Mapping]] = ..., step: _Optional[_Union[PlanStep, _Mapping]] = ..., done: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ...) -> None: ...

class PlanStep(_message.Message):
    __slots__ = ["history", "opened", "read", "result", "retried", "runs", "sent", "started", "status", "store"]
    HISTORY_FIELD_NUMBER: _ClassVar[int]
    OPENED_FIELD_NUMBER: _ClassVar[int]
    READ_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    RETRIED_FIELD_NUMBER: _ClassVar[int]
    RUNS_FIELD_NUMBER: _ClassVar[int]
    SENT_FIELD_NUMBER: _ClassVar[int]
    STARTED_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    STORE_FIELD_NUMBER: _ClassVar[int]
    history: History
    opened: Opened
    read: ReadValue
    result: RunResult
    retried: Retried
    runs: Runs
    sent: Sent
    started: Started
    status: RunState
    store: _boundary_pb2.StoreOp
    def __init__(self, store: _Optional[_Union[_boundary_pb2.StoreOp, _Mapping]] = ..., sent: _Optional[_Union[Sent, _Mapping]] = ..., opened: _Optional[_Union[Opened, _Mapping]] = ..., started: _Optional[_Union[Started, _Mapping]] = ..., result: _Optional[_Union[RunResult, _Mapping]] = ..., status: _Optional[_Union[RunState, _Mapping]] = ..., read: _Optional[_Union[ReadValue, _Mapping]] = ..., runs: _Optional[_Union[Runs, _Mapping]] = ..., history: _Optional[_Union[History, _Mapping]] = ..., retried: _Optional[_Union[Retried, _Mapping]] = ...) -> None: ...

class ReadPlan(_message.Message):
    __slots__ = ["chain", "key", "prefix"]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    chain: str
    key: str
    prefix: str
    def __init__(self, prefix: _Optional[str] = ..., chain: _Optional[str] = ..., key: _Optional[str] = ...) -> None: ...

class ReadValue(_message.Message):
    __slots__ = ["failed", "none", "value"]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    NONE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    failed: _boundary_pb2.StoreFailure
    none: _boundary_pb2.Empty
    value: _layout_pb2.Value
    def __init__(self, value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., none: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ...) -> None: ...

class ResultPlan(_message.Message):
    __slots__ = ["chain", "prefix", "run"]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    chain: str
    prefix: str
    run: str
    def __init__(self, prefix: _Optional[str] = ..., chain: _Optional[str] = ..., run: _Optional[str] = ...) -> None: ...

class Retried(_message.Message):
    __slots__ = ["created", "existing", "failed", "no_chain", "not_retryable"]
    class NotRetryable(_message.Message):
        __slots__ = ["message", "run"]
        MESSAGE_FIELD_NUMBER: _ClassVar[int]
        RUN_FIELD_NUMBER: _ClassVar[int]
        message: str
        run: str
        def __init__(self, run: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...
    class Retry(_message.Message):
        __slots__ = ["run", "workflow"]
        RUN_FIELD_NUMBER: _ClassVar[int]
        WORKFLOW_FIELD_NUMBER: _ClassVar[int]
        run: str
        workflow: str
        def __init__(self, run: _Optional[str] = ..., workflow: _Optional[str] = ...) -> None: ...
    CREATED_FIELD_NUMBER: _ClassVar[int]
    EXISTING_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    NOT_RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    NO_CHAIN_FIELD_NUMBER: _ClassVar[int]
    created: Retried.Retry
    existing: Retried.Retry
    failed: _boundary_pb2.StoreFailure
    no_chain: NoChain
    not_retryable: Retried.NotRetryable
    def __init__(self, created: _Optional[_Union[Retried.Retry, _Mapping]] = ..., existing: _Optional[_Union[Retried.Retry, _Mapping]] = ..., not_retryable: _Optional[_Union[Retried.NotRetryable, _Mapping]] = ..., no_chain: _Optional[_Union[NoChain, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ...) -> None: ...

class RetryPlan(_message.Message):
    __slots__ = ["budget", "chain", "keep", "now", "prefix"]
    BUDGET_FIELD_NUMBER: _ClassVar[int]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    KEEP_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    budget: _layout_pb2.Budget
    chain: str
    keep: int
    now: _layout_pb2.Time
    prefix: str
    def __init__(self, prefix: _Optional[str] = ..., chain: _Optional[str] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., budget: _Optional[_Union[_layout_pb2.Budget, _Mapping]] = ..., keep: _Optional[int] = ...) -> None: ...

class RunExists(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class RunInfo(_message.Message):
    __slots__ = ["blocked", "chain", "due_at", "run", "version", "waiting"]
    BLOCKED_FIELD_NUMBER: _ClassVar[int]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    DUE_AT_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WAITING_FIELD_NUMBER: _ClassVar[int]
    blocked: _layout_pb2.Blocked
    chain: str
    due_at: _layout_pb2.Time
    run: str
    version: int
    waiting: Waiting
    def __init__(self, chain: _Optional[str] = ..., run: _Optional[str] = ..., version: _Optional[int] = ..., due_at: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., waiting: _Optional[_Union[Waiting, str]] = ..., blocked: _Optional[_Union[_layout_pb2.Blocked, _Mapping]] = ...) -> None: ...

class RunResult(_message.Message):
    __slots__ = ["ended", "failed", "no_chain", "not_ended"]
    class Ended(_message.Message):
        __slots__ = ["result", "run"]
        RESULT_FIELD_NUMBER: _ClassVar[int]
        RUN_FIELD_NUMBER: _ClassVar[int]
        result: _layout_pb2.Result
        run: str
        def __init__(self, run: _Optional[str] = ..., result: _Optional[_Union[_layout_pb2.Result, _Mapping]] = ...) -> None: ...
    class NotEnded(_message.Message):
        __slots__ = ["result_key", "run"]
        RESULT_KEY_FIELD_NUMBER: _ClassVar[int]
        RUN_FIELD_NUMBER: _ClassVar[int]
        result_key: str
        run: str
        def __init__(self, run: _Optional[str] = ..., result_key: _Optional[str] = ...) -> None: ...
    ENDED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    NOT_ENDED_FIELD_NUMBER: _ClassVar[int]
    NO_CHAIN_FIELD_NUMBER: _ClassVar[int]
    ended: RunResult.Ended
    failed: _boundary_pb2.StoreFailure
    no_chain: NoChain
    not_ended: RunResult.NotEnded
    def __init__(self, ended: _Optional[_Union[RunResult.Ended, _Mapping]] = ..., no_chain: _Optional[_Union[NoChain, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ..., not_ended: _Optional[_Union[RunResult.NotEnded, _Mapping]] = ...) -> None: ...

class RunState(_message.Message):
    __slots__ = ["failed", "no_chain", "status"]
    class Of(_message.Message):
        __slots__ = ["blocked", "run", "status"]
        BLOCKED_FIELD_NUMBER: _ClassVar[int]
        RUN_FIELD_NUMBER: _ClassVar[int]
        STATUS_FIELD_NUMBER: _ClassVar[int]
        blocked: _layout_pb2.Blocked
        run: str
        status: RunStatus
        def __init__(self, run: _Optional[str] = ..., status: _Optional[_Union[RunStatus, str]] = ..., blocked: _Optional[_Union[_layout_pb2.Blocked, _Mapping]] = ...) -> None: ...
    FAILED_FIELD_NUMBER: _ClassVar[int]
    NO_CHAIN_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    failed: _boundary_pb2.StoreFailure
    no_chain: NoChain
    status: RunState.Of
    def __init__(self, status: _Optional[_Union[RunState.Of, _Mapping]] = ..., no_chain: _Optional[_Union[NoChain, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ...) -> None: ...

class Runs(_message.Message):
    __slots__ = ["end", "failed", "page"]
    class Page(_message.Message):
        __slots__ = ["runs"]
        RUNS_FIELD_NUMBER: _ClassVar[int]
        runs: _containers.RepeatedCompositeFieldContainer[RunInfo]
        def __init__(self, runs: _Optional[_Iterable[_Union[RunInfo, _Mapping]]] = ...) -> None: ...
    END_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    end: _boundary_pb2.Empty
    failed: _boundary_pb2.StoreFailure
    page: Runs.Page
    def __init__(self, page: _Optional[_Union[Runs.Page, _Mapping]] = ..., end: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ...) -> None: ...

class RunsPlan(_message.Message):
    __slots__ = ["prefix", "workflow"]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    workflow: str
    def __init__(self, prefix: _Optional[str] = ..., workflow: _Optional[str] = ...) -> None: ...

class SendPlan(_message.Message):
    __slots__ = ["chain", "id", "name", "now", "ord", "prefix", "value"]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    ORD_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    chain: str
    id: str
    name: str
    now: _layout_pb2.Time
    ord: str
    prefix: str
    value: _layout_pb2.Value
    def __init__(self, prefix: _Optional[str] = ..., chain: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., id: _Optional[str] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., ord: _Optional[str] = ...) -> None: ...

class Sent(_message.Message):
    __slots__ = ["accepted", "failed", "no_chain", "refused"]
    class Accepted(_message.Message):
        __slots__ = ["run", "workflow"]
        RUN_FIELD_NUMBER: _ClassVar[int]
        WORKFLOW_FIELD_NUMBER: _ClassVar[int]
        run: str
        workflow: str
        def __init__(self, run: _Optional[str] = ..., workflow: _Optional[str] = ...) -> None: ...
    class Refused(_message.Message):
        __slots__ = ["run", "status"]
        RUN_FIELD_NUMBER: _ClassVar[int]
        STATUS_FIELD_NUMBER: _ClassVar[int]
        run: str
        status: _layout_pb2.Status
        def __init__(self, run: _Optional[str] = ..., status: _Optional[_Union[_layout_pb2.Status, str]] = ...) -> None: ...
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    NO_CHAIN_FIELD_NUMBER: _ClassVar[int]
    REFUSED_FIELD_NUMBER: _ClassVar[int]
    accepted: Sent.Accepted
    failed: _boundary_pb2.StoreFailure
    no_chain: NoChain
    refused: Sent.Refused
    def __init__(self, accepted: _Optional[_Union[Sent.Accepted, _Mapping]] = ..., refused: _Optional[_Union[Sent.Refused, _Mapping]] = ..., no_chain: _Optional[_Union[NoChain, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ...) -> None: ...

class StartMessage(_message.Message):
    __slots__ = ["id", "name", "ord", "value"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ORD_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    ord: str
    value: _layout_pb2.Value
    def __init__(self, name: _Optional[str] = ..., value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., id: _Optional[str] = ..., ord: _Optional[str] = ...) -> None: ...

class StartPlan(_message.Message):
    __slots__ = ["budget", "id", "input", "message", "now", "prefix", "upgrade", "version", "workflow"]
    BUDGET_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    UPGRADE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    budget: _layout_pb2.Budget
    id: str
    input: _layout_pb2.Value
    message: StartMessage
    now: _layout_pb2.Time
    prefix: str
    upgrade: bool
    version: int
    workflow: str
    def __init__(self, prefix: _Optional[str] = ..., workflow: _Optional[str] = ..., version: _Optional[int] = ..., input: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., id: _Optional[str] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., budget: _Optional[_Union[_layout_pb2.Budget, _Mapping]] = ..., upgrade: bool = ..., message: _Optional[_Union[StartMessage, _Mapping]] = ...) -> None: ...

class Started(_message.Message):
    __slots__ = ["created", "existing", "failed", "run_exists", "sent", "versioned"]
    class Versioned(_message.Message):
        __slots__ = ["run", "version"]
        RUN_FIELD_NUMBER: _ClassVar[int]
        VERSION_FIELD_NUMBER: _ClassVar[int]
        run: str
        version: int
        def __init__(self, run: _Optional[str] = ..., version: _Optional[int] = ...) -> None: ...
    CREATED_FIELD_NUMBER: _ClassVar[int]
    EXISTING_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    RUN_EXISTS_FIELD_NUMBER: _ClassVar[int]
    SENT_FIELD_NUMBER: _ClassVar[int]
    VERSIONED_FIELD_NUMBER: _ClassVar[int]
    created: _boundary_pb2.Empty
    existing: _boundary_pb2.Empty
    failed: _boundary_pb2.StoreFailure
    run_exists: RunExists
    sent: Sent
    versioned: Started.Versioned
    def __init__(self, created: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., existing: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., run_exists: _Optional[_Union[RunExists, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ..., versioned: _Optional[_Union[Started.Versioned, _Mapping]] = ..., sent: _Optional[_Union[Sent, _Mapping]] = ...) -> None: ...

class StatusPlan(_message.Message):
    __slots__ = ["chain", "now", "prefix"]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    chain: str
    now: _layout_pb2.Time
    prefix: str
    def __init__(self, prefix: _Optional[str] = ..., chain: _Optional[str] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...

class RunStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class Waiting(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
