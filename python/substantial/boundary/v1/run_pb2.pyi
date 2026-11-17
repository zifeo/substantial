from substantial.boundary.v1 import boundary_pb2 as _boundary_pb2
from substantial.layout.v1 import layout_pb2 as _layout_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

COMMIT_KIND_END: CommitKind
COMMIT_KIND_PROGRESS: CommitKind
COMMIT_KIND_SUSPEND: CommitKind
COMMIT_KIND_UNSPECIFIED: CommitKind
DESCRIPTOR: _descriptor.FileDescriptor

class Call(_message.Message):
    __slots__ = ["bind", "cancel", "chain_result", "child_result", "cost", "end", "iterate", "now", "publish", "random", "receive", "rollback", "saga_begin", "saga_end", "send", "should_continue_as_new", "sleep_ms", "sleep_until", "start", "step", "uuid", "wait"]
    class Bind(_message.Message):
        __slots__ = ["names"]
        NAMES_FIELD_NUMBER: _ClassVar[int]
        names: _containers.RepeatedScalarFieldContainer[str]
        def __init__(self, names: _Optional[_Iterable[str]] = ...) -> None: ...
    class Cancel(_message.Message):
        __slots__ = ["reason", "run"]
        REASON_FIELD_NUMBER: _ClassVar[int]
        RUN_FIELD_NUMBER: _ClassVar[int]
        reason: str
        run: str
        def __init__(self, run: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...
    class ChainResult(_message.Message):
        __slots__ = ["chain", "timeout_ms"]
        CHAIN_FIELD_NUMBER: _ClassVar[int]
        TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
        chain: str
        timeout_ms: int
        def __init__(self, chain: _Optional[str] = ..., timeout_ms: _Optional[int] = ...) -> None: ...
    class SagaEnd(_message.Message):
        __slots__ = ["failed", "parent", "saga"]
        FAILED_FIELD_NUMBER: _ClassVar[int]
        PARENT_FIELD_NUMBER: _ClassVar[int]
        SAGA_FIELD_NUMBER: _ClassVar[int]
        failed: bool
        parent: int
        saga: int
        def __init__(self, saga: _Optional[int] = ..., failed: bool = ..., parent: _Optional[int] = ...) -> None: ...
    class Send(_message.Message):
        __slots__ = ["id", "name", "run", "value"]
        ID_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        RUN_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        id: str
        name: str
        run: str
        value: _layout_pb2.Value
        def __init__(self, run: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., id: _Optional[str] = ...) -> None: ...
    class Start(_message.Message):
        __slots__ = ["budget", "id", "input", "version", "workflow"]
        BUDGET_FIELD_NUMBER: _ClassVar[int]
        ID_FIELD_NUMBER: _ClassVar[int]
        INPUT_FIELD_NUMBER: _ClassVar[int]
        VERSION_FIELD_NUMBER: _ClassVar[int]
        WORKFLOW_FIELD_NUMBER: _ClassVar[int]
        budget: _layout_pb2.Budget
        id: str
        input: _layout_pb2.Value
        version: int
        workflow: str
        def __init__(self, workflow: _Optional[str] = ..., version: _Optional[int] = ..., input: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., id: _Optional[str] = ..., budget: _Optional[_Union[_layout_pb2.Budget, _Mapping]] = ...) -> None: ...
    class Step(_message.Message):
        __slots__ = ["name", "options"]
        NAME_FIELD_NUMBER: _ClassVar[int]
        OPTIONS_FIELD_NUMBER: _ClassVar[int]
        name: str
        options: StepOptions
        def __init__(self, name: _Optional[str] = ..., options: _Optional[_Union[StepOptions, _Mapping]] = ...) -> None: ...
    class Wait(_message.Message):
        __slots__ = ["timeout_ms"]
        TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
        timeout_ms: int
        def __init__(self, timeout_ms: _Optional[int] = ...) -> None: ...
    BIND_FIELD_NUMBER: _ClassVar[int]
    CANCEL_FIELD_NUMBER: _ClassVar[int]
    CHAIN_RESULT_FIELD_NUMBER: _ClassVar[int]
    CHILD_RESULT_FIELD_NUMBER: _ClassVar[int]
    COST_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    ITERATE_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    PUBLISH_FIELD_NUMBER: _ClassVar[int]
    RANDOM_FIELD_NUMBER: _ClassVar[int]
    RECEIVE_FIELD_NUMBER: _ClassVar[int]
    ROLLBACK_FIELD_NUMBER: _ClassVar[int]
    SAGA_BEGIN_FIELD_NUMBER: _ClassVar[int]
    SAGA_END_FIELD_NUMBER: _ClassVar[int]
    SEND_FIELD_NUMBER: _ClassVar[int]
    SHOULD_CONTINUE_AS_NEW_FIELD_NUMBER: _ClassVar[int]
    SLEEP_MS_FIELD_NUMBER: _ClassVar[int]
    SLEEP_UNTIL_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    WAIT_FIELD_NUMBER: _ClassVar[int]
    bind: Call.Bind
    cancel: Call.Cancel
    chain_result: Call.ChainResult
    child_result: str
    cost: _boundary_pb2.Empty
    end: End
    iterate: _layout_pb2.Value
    now: _boundary_pb2.Empty
    publish: _layout_pb2.Published
    random: _boundary_pb2.Empty
    receive: str
    rollback: _boundary_pb2.Empty
    saga_begin: _boundary_pb2.Empty
    saga_end: Call.SagaEnd
    send: Call.Send
    should_continue_as_new: _boundary_pb2.Empty
    sleep_ms: int
    sleep_until: _layout_pb2.Time
    start: Call.Start
    step: Call.Step
    uuid: _boundary_pb2.Empty
    wait: Call.Wait
    def __init__(self, step: _Optional[_Union[Call.Step, _Mapping]] = ..., now: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., random: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., uuid: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., sleep_ms: _Optional[int] = ..., sleep_until: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., wait: _Optional[_Union[Call.Wait, _Mapping]] = ..., receive: _Optional[str] = ..., bind: _Optional[_Union[Call.Bind, _Mapping]] = ..., start: _Optional[_Union[Call.Start, _Mapping]] = ..., child_result: _Optional[str] = ..., send: _Optional[_Union[Call.Send, _Mapping]] = ..., publish: _Optional[_Union[_layout_pb2.Published, _Mapping]] = ..., saga_begin: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., saga_end: _Optional[_Union[Call.SagaEnd, _Mapping]] = ..., rollback: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., iterate: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., should_continue_as_new: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., end: _Optional[_Union[End, _Mapping]] = ..., cost: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., cancel: _Optional[_Union[Call.Cancel, _Mapping]] = ..., chain_result: _Optional[_Union[Call.ChainResult, _Mapping]] = ...) -> None: ...

class Called(_message.Message):
    __slots__ = ["done", "failed", "live", "messages", "occurrence", "pending", "run", "saga", "undos", "value"]
    class Messages(_message.Message):
        __slots__ = ["messages"]
        MESSAGES_FIELD_NUMBER: _ClassVar[int]
        messages: _containers.RepeatedCompositeFieldContainer[Message]
        def __init__(self, messages: _Optional[_Iterable[_Union[Message, _Mapping]]] = ...) -> None: ...
    class Undos(_message.Message):
        __slots__ = ["undos"]
        UNDOS_FIELD_NUMBER: _ClassVar[int]
        undos: _containers.RepeatedCompositeFieldContainer[Undo]
        def __init__(self, undos: _Optional[_Iterable[_Union[Undo, _Mapping]]] = ...) -> None: ...
    DONE_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    LIVE_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    PENDING_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    SAGA_FIELD_NUMBER: _ClassVar[int]
    UNDOS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    done: _boundary_pb2.Empty
    failed: _layout_pb2.Failure
    live: bool
    messages: Called.Messages
    occurrence: int
    pending: int
    run: str
    saga: int
    undos: Called.Undos
    value: _layout_pb2.Value
    def __init__(self, pending: _Optional[int] = ..., messages: _Optional[_Union[Called.Messages, _Mapping]] = ..., run: _Optional[str] = ..., saga: _Optional[int] = ..., undos: _Optional[_Union[Called.Undos, _Mapping]] = ..., value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., done: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., failed: _Optional[_Union[_layout_pb2.Failure, _Mapping]] = ..., occurrence: _Optional[int] = ..., live: bool = ...) -> None: ...

class Commit(_message.Message):
    __slots__ = ["consumed", "kind", "segment"]
    CONSUMED_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    consumed: _containers.RepeatedScalarFieldContainer[str]
    kind: CommitKind
    segment: _layout_pb2.Segment
    def __init__(self, kind: _Optional[_Union[CommitKind, str]] = ..., segment: _Optional[_Union[_layout_pb2.Segment, _Mapping]] = ..., consumed: _Optional[_Iterable[str]] = ...) -> None: ...

class End(_message.Message):
    __slots__ = ["cancelled", "completed", "continued", "failed"]
    class Continued(_message.Message):
        __slots__ = ["input", "state", "version", "workflow"]
        INPUT_FIELD_NUMBER: _ClassVar[int]
        STATE_FIELD_NUMBER: _ClassVar[int]
        VERSION_FIELD_NUMBER: _ClassVar[int]
        WORKFLOW_FIELD_NUMBER: _ClassVar[int]
        input: _layout_pb2.Value
        state: _layout_pb2.Value
        version: int
        workflow: str
        def __init__(self, workflow: _Optional[str] = ..., input: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., state: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., version: _Optional[int] = ...) -> None: ...
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    CONTINUED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    cancelled: _layout_pb2.Cancelled
    completed: _layout_pb2.Completed
    continued: End.Continued
    failed: _layout_pb2.Failed
    def __init__(self, completed: _Optional[_Union[_layout_pb2.Completed, _Mapping]] = ..., failed: _Optional[_Union[_layout_pb2.Failed, _Mapping]] = ..., cancelled: _Optional[_Union[_layout_pb2.Cancelled, _Mapping]] = ..., continued: _Optional[_Union[End.Continued, _Mapping]] = ...) -> None: ...

class Idle(_message.Message):
    __slots__ = ["next_wake"]
    NEXT_WAKE_FIELD_NUMBER: _ClassVar[int]
    next_wake: _layout_pb2.Time
    def __init__(self, next_wake: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...

class InboxMessage(_message.Message):
    __slots__ = ["body", "key"]
    BODY_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    body: _layout_pb2.Message
    key: str
    def __init__(self, key: _Optional[str] = ..., body: _Optional[_Union[_layout_pb2.Message, _Mapping]] = ...) -> None: ...

class Message(_message.Message):
    __slots__ = ["id", "name", "value"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    value: _layout_pb2.Value
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ...) -> None: ...

class Next(_message.Message):
    __slots__ = ["child_ended", "commit", "deliver", "execute", "idle", "live", "resolve", "store", "wait_steps"]
    class Execute(_message.Message):
        __slots__ = ["attempt", "call", "timeout_ms"]
        ATTEMPT_FIELD_NUMBER: _ClassVar[int]
        CALL_FIELD_NUMBER: _ClassVar[int]
        TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
        attempt: int
        call: int
        timeout_ms: int
        def __init__(self, call: _Optional[int] = ..., attempt: _Optional[int] = ..., timeout_ms: _Optional[int] = ...) -> None: ...
    class Resolve(_message.Message):
        __slots__ = ["call", "resolution"]
        CALL_FIELD_NUMBER: _ClassVar[int]
        RESOLUTION_FIELD_NUMBER: _ClassVar[int]
        call: int
        resolution: Resolution
        def __init__(self, call: _Optional[int] = ..., resolution: _Optional[_Union[Resolution, _Mapping]] = ...) -> None: ...
    CHILD_ENDED_FIELD_NUMBER: _ClassVar[int]
    COMMIT_FIELD_NUMBER: _ClassVar[int]
    DELIVER_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_FIELD_NUMBER: _ClassVar[int]
    IDLE_FIELD_NUMBER: _ClassVar[int]
    LIVE_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_FIELD_NUMBER: _ClassVar[int]
    STORE_FIELD_NUMBER: _ClassVar[int]
    WAIT_STEPS_FIELD_NUMBER: _ClassVar[int]
    child_ended: str
    commit: Commit
    deliver: Message
    execute: Next.Execute
    idle: Idle
    live: bool
    resolve: Next.Resolve
    store: _boundary_pb2.StoreOp
    wait_steps: WaitSteps
    def __init__(self, resolve: _Optional[_Union[Next.Resolve, _Mapping]] = ..., deliver: _Optional[_Union[Message, _Mapping]] = ..., execute: _Optional[_Union[Next.Execute, _Mapping]] = ..., commit: _Optional[_Union[Commit, _Mapping]] = ..., wait_steps: _Optional[_Union[WaitSteps, _Mapping]] = ..., idle: _Optional[_Union[Idle, _Mapping]] = ..., child_ended: _Optional[str] = ..., store: _Optional[_Union[_boundary_pb2.StoreOp, _Mapping]] = ..., live: bool = ...) -> None: ...

class Outcome(_message.Message):
    __slots__ = ["error", "value"]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    error: _layout_pb2.Failure
    value: _layout_pb2.Value
    def __init__(self, value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., error: _Optional[_Union[_layout_pb2.Failure, _Mapping]] = ...) -> None: ...

class Prices(_message.Message):
    __slots__ = ["create", "currency", "delete", "get", "list"]
    CREATE_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    DELETE_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    LIST_FIELD_NUMBER: _ClassVar[int]
    create: int
    currency: str
    delete: int
    get: int
    list: int
    def __init__(self, currency: _Optional[str] = ..., create: _Optional[int] = ..., get: _Optional[int] = ..., list: _Optional[int] = ..., delete: _Optional[int] = ...) -> None: ...

class Resolution(_message.Message):
    __slots__ = ["cancelled", "error", "value"]
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    cancelled: str
    error: _layout_pb2.Failure
    value: _layout_pb2.Value
    def __init__(self, value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., error: _Optional[_Union[_layout_pb2.Failure, _Mapping]] = ..., cancelled: _Optional[str] = ...) -> None: ...

class Retention(_message.Message):
    __slots__ = ["ms"]
    MS_FIELD_NUMBER: _ClassVar[int]
    ms: int
    def __init__(self, ms: _Optional[int] = ...) -> None: ...

class RetryPolicy(_message.Message):
    __slots__ = ["initial_ms", "max_attempts", "max_ms", "multiplier"]
    INITIAL_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    MAX_MS_FIELD_NUMBER: _ClassVar[int]
    MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    initial_ms: int
    max_attempts: int
    max_ms: int
    multiplier: float
    def __init__(self, max_attempts: _Optional[int] = ..., initial_ms: _Optional[int] = ..., max_ms: _Optional[int] = ..., multiplier: _Optional[float] = ...) -> None: ...

class RunInput(_message.Message):
    __slots__ = ["cost", "history", "history_bytes", "inbox", "next_n", "now", "prefix", "run", "seed", "settings", "start", "worker"]
    COST_FIELD_NUMBER: _ClassVar[int]
    HISTORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    HISTORY_FIELD_NUMBER: _ClassVar[int]
    INBOX_FIELD_NUMBER: _ClassVar[int]
    NEXT_N_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    cost: _layout_pb2.Cost
    history: _containers.RepeatedCompositeFieldContainer[_layout_pb2.Event]
    history_bytes: int
    inbox: _containers.RepeatedCompositeFieldContainer[InboxMessage]
    next_n: int
    now: _layout_pb2.Time
    prefix: str
    run: str
    seed: bytes
    settings: RunSettings
    start: _layout_pb2.Start
    worker: str
    def __init__(self, run: _Optional[str] = ..., start: _Optional[_Union[_layout_pb2.Start, _Mapping]] = ..., history: _Optional[_Iterable[_Union[_layout_pb2.Event, _Mapping]]] = ..., history_bytes: _Optional[int] = ..., next_n: _Optional[int] = ..., inbox: _Optional[_Iterable[_Union[InboxMessage, _Mapping]]] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., seed: _Optional[bytes] = ..., worker: _Optional[str] = ..., settings: _Optional[_Union[RunSettings, _Mapping]] = ..., prefix: _Optional[str] = ..., cost: _Optional[_Union[_layout_pb2.Cost, _Mapping]] = ...) -> None: ...

class RunRequest(_message.Message):
    __slots__ = ["arrived", "call", "next", "satisfied", "step_ended", "stored"]
    class Arrived(_message.Message):
        __slots__ = ["cost", "messages"]
        COST_FIELD_NUMBER: _ClassVar[int]
        MESSAGES_FIELD_NUMBER: _ClassVar[int]
        cost: _layout_pb2.Cost
        messages: _containers.RepeatedCompositeFieldContainer[InboxMessage]
        def __init__(self, messages: _Optional[_Iterable[_Union[InboxMessage, _Mapping]]] = ..., cost: _Optional[_Union[_layout_pb2.Cost, _Mapping]] = ...) -> None: ...
    class MakeCall(_message.Message):
        __slots__ = ["call", "now"]
        CALL_FIELD_NUMBER: _ClassVar[int]
        NOW_FIELD_NUMBER: _ClassVar[int]
        call: Call
        now: _layout_pb2.Time
        def __init__(self, call: _Optional[_Union[Call, _Mapping]] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...
    class StepEnded(_message.Message):
        __slots__ = ["attempt", "call", "charges", "now", "outcome"]
        ATTEMPT_FIELD_NUMBER: _ClassVar[int]
        CALL_FIELD_NUMBER: _ClassVar[int]
        CHARGES_FIELD_NUMBER: _ClassVar[int]
        NOW_FIELD_NUMBER: _ClassVar[int]
        OUTCOME_FIELD_NUMBER: _ClassVar[int]
        attempt: int
        call: int
        charges: _containers.RepeatedCompositeFieldContainer[_layout_pb2.Amount]
        now: _layout_pb2.Time
        outcome: Outcome
        def __init__(self, call: _Optional[int] = ..., attempt: _Optional[int] = ..., outcome: _Optional[_Union[Outcome, _Mapping]] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., charges: _Optional[_Iterable[_Union[_layout_pb2.Amount, _Mapping]]] = ...) -> None: ...
    ARRIVED_FIELD_NUMBER: _ClassVar[int]
    CALL_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIELD_NUMBER: _ClassVar[int]
    SATISFIED_FIELD_NUMBER: _ClassVar[int]
    STEP_ENDED_FIELD_NUMBER: _ClassVar[int]
    STORED_FIELD_NUMBER: _ClassVar[int]
    arrived: RunRequest.Arrived
    call: RunRequest.MakeCall
    next: _layout_pb2.Time
    satisfied: int
    step_ended: RunRequest.StepEnded
    stored: _boundary_pb2.OpResult
    def __init__(self, call: _Optional[_Union[RunRequest.MakeCall, _Mapping]] = ..., step_ended: _Optional[_Union[RunRequest.StepEnded, _Mapping]] = ..., satisfied: _Optional[int] = ..., next: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., arrived: _Optional[_Union[RunRequest.Arrived, _Mapping]] = ..., stored: _Optional[_Union[_boundary_pb2.OpResult, _Mapping]] = ...) -> None: ...

class RunResponse(_message.Message):
    __slots__ = ["called", "done", "error", "next"]
    CALLED_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIELD_NUMBER: _ClassVar[int]
    called: Called
    done: _boundary_pb2.Empty
    error: _boundary_pb2.CoreError
    next: Next
    def __init__(self, error: _Optional[_Union[_boundary_pb2.CoreError, _Mapping]] = ..., called: _Optional[_Union[Called, _Mapping]] = ..., next: _Optional[_Union[Next, _Mapping]] = ..., done: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ...) -> None: ...

class RunSettings(_message.Message):
    __slots__ = ["commit_every_ms", "continue_as_new_after_bytes", "continue_as_new_after_segments", "dedup_window_ms", "prices", "retention"]
    COMMIT_EVERY_MS_FIELD_NUMBER: _ClassVar[int]
    CONTINUE_AS_NEW_AFTER_BYTES_FIELD_NUMBER: _ClassVar[int]
    CONTINUE_AS_NEW_AFTER_SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    DEDUP_WINDOW_MS_FIELD_NUMBER: _ClassVar[int]
    PRICES_FIELD_NUMBER: _ClassVar[int]
    RETENTION_FIELD_NUMBER: _ClassVar[int]
    commit_every_ms: int
    continue_as_new_after_bytes: int
    continue_as_new_after_segments: int
    dedup_window_ms: int
    prices: Prices
    retention: Retention
    def __init__(self, commit_every_ms: _Optional[int] = ..., continue_as_new_after_segments: _Optional[int] = ..., continue_as_new_after_bytes: _Optional[int] = ..., dedup_window_ms: _Optional[int] = ..., retention: _Optional[_Union[Retention, _Mapping]] = ..., prices: _Optional[_Union[Prices, _Mapping]] = ...) -> None: ...

class StepOptions(_message.Message):
    __slots__ = ["occurrence", "retry", "saga", "timeout_ms", "undo"]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    RETRY_FIELD_NUMBER: _ClassVar[int]
    SAGA_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    UNDO_FIELD_NUMBER: _ClassVar[int]
    occurrence: int
    retry: RetryPolicy
    saga: int
    timeout_ms: int
    undo: bool
    def __init__(self, retry: _Optional[_Union[RetryPolicy, _Mapping]] = ..., timeout_ms: _Optional[int] = ..., undo: bool = ..., saga: _Optional[int] = ..., occurrence: _Optional[int] = ...) -> None: ...

class Undo(_message.Message):
    __slots__ = ["call", "name", "occurrence", "value"]
    CALL_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    call: int
    name: str
    occurrence: int
    value: _layout_pb2.Value
    def __init__(self, call: _Optional[int] = ..., name: _Optional[str] = ..., occurrence: _Optional[int] = ..., value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ...) -> None: ...

class WaitSteps(_message.Message):
    __slots__ = ["until"]
    UNTIL_FIELD_NUMBER: _ClassVar[int]
    until: _layout_pb2.Time
    def __init__(self, until: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...

class CommitKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
