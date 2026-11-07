from substantial.boundary.v1 import boundary_pb2 as _boundary_pb2
from substantial.layout.v1 import layout_pb2 as _layout_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CommitKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMMIT_KIND_UNSPECIFIED: _ClassVar[CommitKind]
    COMMIT_KIND_PROGRESS: _ClassVar[CommitKind]
    COMMIT_KIND_SUSPEND: _ClassVar[CommitKind]
    COMMIT_KIND_END: _ClassVar[CommitKind]
COMMIT_KIND_UNSPECIFIED: CommitKind
COMMIT_KIND_PROGRESS: CommitKind
COMMIT_KIND_SUSPEND: CommitKind
COMMIT_KIND_END: CommitKind

class RunSettings(_message.Message):
    __slots__ = ("commit_every_ms", "continue_as_new_after_segments", "continue_as_new_after_bytes", "dedup_window_ms")
    COMMIT_EVERY_MS_FIELD_NUMBER: _ClassVar[int]
    CONTINUE_AS_NEW_AFTER_SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    CONTINUE_AS_NEW_AFTER_BYTES_FIELD_NUMBER: _ClassVar[int]
    DEDUP_WINDOW_MS_FIELD_NUMBER: _ClassVar[int]
    commit_every_ms: int
    continue_as_new_after_segments: int
    continue_as_new_after_bytes: int
    dedup_window_ms: int
    def __init__(self, commit_every_ms: _Optional[int] = ..., continue_as_new_after_segments: _Optional[int] = ..., continue_as_new_after_bytes: _Optional[int] = ..., dedup_window_ms: _Optional[int] = ...) -> None: ...

class InboxMessage(_message.Message):
    __slots__ = ("key", "body")
    KEY_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    key: str
    body: _layout_pb2.Message
    def __init__(self, key: _Optional[str] = ..., body: _Optional[_Union[_layout_pb2.Message, _Mapping]] = ...) -> None: ...

class RunInput(_message.Message):
    __slots__ = ("run", "start", "history", "history_bytes", "next_n", "inbox", "now", "seed", "worker", "settings", "prefix", "partitions")
    RUN_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    HISTORY_FIELD_NUMBER: _ClassVar[int]
    HISTORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    NEXT_N_FIELD_NUMBER: _ClassVar[int]
    INBOX_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    run: str
    start: _layout_pb2.Start
    history: _containers.RepeatedCompositeFieldContainer[_layout_pb2.Event]
    history_bytes: int
    next_n: int
    inbox: _containers.RepeatedCompositeFieldContainer[InboxMessage]
    now: _layout_pb2.Time
    seed: bytes
    worker: str
    settings: RunSettings
    prefix: str
    partitions: int
    def __init__(self, run: _Optional[str] = ..., start: _Optional[_Union[_layout_pb2.Start, _Mapping]] = ..., history: _Optional[_Iterable[_Union[_layout_pb2.Event, _Mapping]]] = ..., history_bytes: _Optional[int] = ..., next_n: _Optional[int] = ..., inbox: _Optional[_Iterable[_Union[InboxMessage, _Mapping]]] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., seed: _Optional[bytes] = ..., worker: _Optional[str] = ..., settings: _Optional[_Union[RunSettings, _Mapping]] = ..., prefix: _Optional[str] = ..., partitions: _Optional[int] = ...) -> None: ...

class RetryPolicy(_message.Message):
    __slots__ = ("max_attempts", "initial_ms", "max_ms", "multiplier")
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    INITIAL_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_MS_FIELD_NUMBER: _ClassVar[int]
    MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    max_attempts: int
    initial_ms: int
    max_ms: int
    multiplier: float
    def __init__(self, max_attempts: _Optional[int] = ..., initial_ms: _Optional[int] = ..., max_ms: _Optional[int] = ..., multiplier: _Optional[float] = ...) -> None: ...

class StepOptions(_message.Message):
    __slots__ = ("retry", "timeout_ms", "undo", "saga", "occurrence")
    RETRY_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    UNDO_FIELD_NUMBER: _ClassVar[int]
    SAGA_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    retry: RetryPolicy
    timeout_ms: int
    undo: bool
    saga: int
    occurrence: int
    def __init__(self, retry: _Optional[_Union[RetryPolicy, _Mapping]] = ..., timeout_ms: _Optional[int] = ..., undo: _Optional[bool] = ..., saga: _Optional[int] = ..., occurrence: _Optional[int] = ...) -> None: ...

class End(_message.Message):
    __slots__ = ("completed", "failed", "cancelled", "continued")
    class Continued(_message.Message):
        __slots__ = ("workflow", "input", "state", "version")
        WORKFLOW_FIELD_NUMBER: _ClassVar[int]
        INPUT_FIELD_NUMBER: _ClassVar[int]
        STATE_FIELD_NUMBER: _ClassVar[int]
        VERSION_FIELD_NUMBER: _ClassVar[int]
        workflow: str
        input: _layout_pb2.Value
        state: _layout_pb2.Value
        version: int
        def __init__(self, workflow: _Optional[str] = ..., input: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., state: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., version: _Optional[int] = ...) -> None: ...
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    CONTINUED_FIELD_NUMBER: _ClassVar[int]
    completed: _layout_pb2.Completed
    failed: _layout_pb2.Failed
    cancelled: _layout_pb2.Cancelled
    continued: End.Continued
    def __init__(self, completed: _Optional[_Union[_layout_pb2.Completed, _Mapping]] = ..., failed: _Optional[_Union[_layout_pb2.Failed, _Mapping]] = ..., cancelled: _Optional[_Union[_layout_pb2.Cancelled, _Mapping]] = ..., continued: _Optional[_Union[End.Continued, _Mapping]] = ...) -> None: ...

class Call(_message.Message):
    __slots__ = ("step", "now", "random", "uuid", "sleep_ms", "sleep_until", "wait", "receive", "bind", "start", "child_result", "send", "publish", "saga_begin", "saga_end", "rollback", "iterate", "should_continue_as_new", "end")
    class Step(_message.Message):
        __slots__ = ("name", "options")
        NAME_FIELD_NUMBER: _ClassVar[int]
        OPTIONS_FIELD_NUMBER: _ClassVar[int]
        name: str
        options: StepOptions
        def __init__(self, name: _Optional[str] = ..., options: _Optional[_Union[StepOptions, _Mapping]] = ...) -> None: ...
    class Wait(_message.Message):
        __slots__ = ("timeout_ms",)
        TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
        timeout_ms: int
        def __init__(self, timeout_ms: _Optional[int] = ...) -> None: ...
    class Bind(_message.Message):
        __slots__ = ("names",)
        NAMES_FIELD_NUMBER: _ClassVar[int]
        names: _containers.RepeatedScalarFieldContainer[str]
        def __init__(self, names: _Optional[_Iterable[str]] = ...) -> None: ...
    class Start(_message.Message):
        __slots__ = ("workflow", "version", "input", "id")
        WORKFLOW_FIELD_NUMBER: _ClassVar[int]
        VERSION_FIELD_NUMBER: _ClassVar[int]
        INPUT_FIELD_NUMBER: _ClassVar[int]
        ID_FIELD_NUMBER: _ClassVar[int]
        workflow: str
        version: int
        input: _layout_pb2.Value
        id: str
        def __init__(self, workflow: _Optional[str] = ..., version: _Optional[int] = ..., input: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., id: _Optional[str] = ...) -> None: ...
    class Send(_message.Message):
        __slots__ = ("run", "name", "value", "id")
        RUN_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        ID_FIELD_NUMBER: _ClassVar[int]
        run: str
        name: str
        value: _layout_pb2.Value
        id: str
        def __init__(self, run: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., id: _Optional[str] = ...) -> None: ...
    class SagaEnd(_message.Message):
        __slots__ = ("saga", "failed", "parent")
        SAGA_FIELD_NUMBER: _ClassVar[int]
        FAILED_FIELD_NUMBER: _ClassVar[int]
        PARENT_FIELD_NUMBER: _ClassVar[int]
        saga: int
        failed: bool
        parent: int
        def __init__(self, saga: _Optional[int] = ..., failed: _Optional[bool] = ..., parent: _Optional[int] = ...) -> None: ...
    STEP_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    RANDOM_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    SLEEP_MS_FIELD_NUMBER: _ClassVar[int]
    SLEEP_UNTIL_FIELD_NUMBER: _ClassVar[int]
    WAIT_FIELD_NUMBER: _ClassVar[int]
    RECEIVE_FIELD_NUMBER: _ClassVar[int]
    BIND_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    CHILD_RESULT_FIELD_NUMBER: _ClassVar[int]
    SEND_FIELD_NUMBER: _ClassVar[int]
    PUBLISH_FIELD_NUMBER: _ClassVar[int]
    SAGA_BEGIN_FIELD_NUMBER: _ClassVar[int]
    SAGA_END_FIELD_NUMBER: _ClassVar[int]
    ROLLBACK_FIELD_NUMBER: _ClassVar[int]
    ITERATE_FIELD_NUMBER: _ClassVar[int]
    SHOULD_CONTINUE_AS_NEW_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    step: Call.Step
    now: _boundary_pb2.Empty
    random: _boundary_pb2.Empty
    uuid: _boundary_pb2.Empty
    sleep_ms: int
    sleep_until: _layout_pb2.Time
    wait: Call.Wait
    receive: str
    bind: Call.Bind
    start: Call.Start
    child_result: str
    send: Call.Send
    publish: _layout_pb2.Published
    saga_begin: _boundary_pb2.Empty
    saga_end: Call.SagaEnd
    rollback: _boundary_pb2.Empty
    iterate: _layout_pb2.Value
    should_continue_as_new: _boundary_pb2.Empty
    end: End
    def __init__(self, step: _Optional[_Union[Call.Step, _Mapping]] = ..., now: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., random: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., uuid: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., sleep_ms: _Optional[int] = ..., sleep_until: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., wait: _Optional[_Union[Call.Wait, _Mapping]] = ..., receive: _Optional[str] = ..., bind: _Optional[_Union[Call.Bind, _Mapping]] = ..., start: _Optional[_Union[Call.Start, _Mapping]] = ..., child_result: _Optional[str] = ..., send: _Optional[_Union[Call.Send, _Mapping]] = ..., publish: _Optional[_Union[_layout_pb2.Published, _Mapping]] = ..., saga_begin: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., saga_end: _Optional[_Union[Call.SagaEnd, _Mapping]] = ..., rollback: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., iterate: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., should_continue_as_new: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., end: _Optional[_Union[End, _Mapping]] = ...) -> None: ...

class Message(_message.Message):
    __slots__ = ("id", "name", "value")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    value: _layout_pb2.Value
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ...) -> None: ...

class Undo(_message.Message):
    __slots__ = ("call", "name", "occurrence", "value")
    CALL_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    call: int
    name: str
    occurrence: int
    value: _layout_pb2.Value
    def __init__(self, call: _Optional[int] = ..., name: _Optional[str] = ..., occurrence: _Optional[int] = ..., value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ...) -> None: ...

class Called(_message.Message):
    __slots__ = ("pending", "messages", "run", "saga", "undos", "value", "done", "occurrence", "live")
    class Messages(_message.Message):
        __slots__ = ("messages",)
        MESSAGES_FIELD_NUMBER: _ClassVar[int]
        messages: _containers.RepeatedCompositeFieldContainer[Message]
        def __init__(self, messages: _Optional[_Iterable[_Union[Message, _Mapping]]] = ...) -> None: ...
    class Undos(_message.Message):
        __slots__ = ("undos",)
        UNDOS_FIELD_NUMBER: _ClassVar[int]
        undos: _containers.RepeatedCompositeFieldContainer[Undo]
        def __init__(self, undos: _Optional[_Iterable[_Union[Undo, _Mapping]]] = ...) -> None: ...
    PENDING_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    SAGA_FIELD_NUMBER: _ClassVar[int]
    UNDOS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    LIVE_FIELD_NUMBER: _ClassVar[int]
    pending: int
    messages: Called.Messages
    run: str
    saga: int
    undos: Called.Undos
    value: _layout_pb2.Value
    done: _boundary_pb2.Empty
    occurrence: int
    live: bool
    def __init__(self, pending: _Optional[int] = ..., messages: _Optional[_Union[Called.Messages, _Mapping]] = ..., run: _Optional[str] = ..., saga: _Optional[int] = ..., undos: _Optional[_Union[Called.Undos, _Mapping]] = ..., value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., done: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., occurrence: _Optional[int] = ..., live: _Optional[bool] = ...) -> None: ...

class Outcome(_message.Message):
    __slots__ = ("value", "error")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    value: _layout_pb2.Value
    error: _layout_pb2.Failure
    def __init__(self, value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., error: _Optional[_Union[_layout_pb2.Failure, _Mapping]] = ...) -> None: ...

class Resolution(_message.Message):
    __slots__ = ("value", "error", "cancelled")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    value: _layout_pb2.Value
    error: _layout_pb2.Failure
    cancelled: str
    def __init__(self, value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., error: _Optional[_Union[_layout_pb2.Failure, _Mapping]] = ..., cancelled: _Optional[str] = ...) -> None: ...

class Commit(_message.Message):
    __slots__ = ("kind", "segment", "consumed")
    KIND_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    CONSUMED_FIELD_NUMBER: _ClassVar[int]
    kind: CommitKind
    segment: _layout_pb2.Segment
    consumed: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, kind: _Optional[_Union[CommitKind, str]] = ..., segment: _Optional[_Union[_layout_pb2.Segment, _Mapping]] = ..., consumed: _Optional[_Iterable[str]] = ...) -> None: ...

class WaitSteps(_message.Message):
    __slots__ = ("until",)
    UNTIL_FIELD_NUMBER: _ClassVar[int]
    until: _layout_pb2.Time
    def __init__(self, until: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...

class Idle(_message.Message):
    __slots__ = ("next_wake",)
    NEXT_WAKE_FIELD_NUMBER: _ClassVar[int]
    next_wake: _layout_pb2.Time
    def __init__(self, next_wake: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...

class Next(_message.Message):
    __slots__ = ("resolve", "deliver", "execute", "commit", "wait_steps", "idle", "child_ended", "store", "live")
    class Resolve(_message.Message):
        __slots__ = ("call", "resolution")
        CALL_FIELD_NUMBER: _ClassVar[int]
        RESOLUTION_FIELD_NUMBER: _ClassVar[int]
        call: int
        resolution: Resolution
        def __init__(self, call: _Optional[int] = ..., resolution: _Optional[_Union[Resolution, _Mapping]] = ...) -> None: ...
    class Execute(_message.Message):
        __slots__ = ("call", "attempt", "timeout_ms")
        CALL_FIELD_NUMBER: _ClassVar[int]
        ATTEMPT_FIELD_NUMBER: _ClassVar[int]
        TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
        call: int
        attempt: int
        timeout_ms: int
        def __init__(self, call: _Optional[int] = ..., attempt: _Optional[int] = ..., timeout_ms: _Optional[int] = ...) -> None: ...
    RESOLVE_FIELD_NUMBER: _ClassVar[int]
    DELIVER_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_FIELD_NUMBER: _ClassVar[int]
    COMMIT_FIELD_NUMBER: _ClassVar[int]
    WAIT_STEPS_FIELD_NUMBER: _ClassVar[int]
    IDLE_FIELD_NUMBER: _ClassVar[int]
    CHILD_ENDED_FIELD_NUMBER: _ClassVar[int]
    STORE_FIELD_NUMBER: _ClassVar[int]
    LIVE_FIELD_NUMBER: _ClassVar[int]
    resolve: Next.Resolve
    deliver: Message
    execute: Next.Execute
    commit: Commit
    wait_steps: WaitSteps
    idle: Idle
    child_ended: str
    store: _boundary_pb2.StoreOp
    live: bool
    def __init__(self, resolve: _Optional[_Union[Next.Resolve, _Mapping]] = ..., deliver: _Optional[_Union[Message, _Mapping]] = ..., execute: _Optional[_Union[Next.Execute, _Mapping]] = ..., commit: _Optional[_Union[Commit, _Mapping]] = ..., wait_steps: _Optional[_Union[WaitSteps, _Mapping]] = ..., idle: _Optional[_Union[Idle, _Mapping]] = ..., child_ended: _Optional[str] = ..., store: _Optional[_Union[_boundary_pb2.StoreOp, _Mapping]] = ..., live: _Optional[bool] = ...) -> None: ...

class RunRequest(_message.Message):
    __slots__ = ("call", "step_ended", "satisfied", "next", "arrived", "stored")
    class MakeCall(_message.Message):
        __slots__ = ("call", "now")
        CALL_FIELD_NUMBER: _ClassVar[int]
        NOW_FIELD_NUMBER: _ClassVar[int]
        call: Call
        now: _layout_pb2.Time
        def __init__(self, call: _Optional[_Union[Call, _Mapping]] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...
    class StepEnded(_message.Message):
        __slots__ = ("call", "attempt", "outcome", "now")
        CALL_FIELD_NUMBER: _ClassVar[int]
        ATTEMPT_FIELD_NUMBER: _ClassVar[int]
        OUTCOME_FIELD_NUMBER: _ClassVar[int]
        NOW_FIELD_NUMBER: _ClassVar[int]
        call: int
        attempt: int
        outcome: Outcome
        now: _layout_pb2.Time
        def __init__(self, call: _Optional[int] = ..., attempt: _Optional[int] = ..., outcome: _Optional[_Union[Outcome, _Mapping]] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...
    class Arrived(_message.Message):
        __slots__ = ("messages",)
        MESSAGES_FIELD_NUMBER: _ClassVar[int]
        messages: _containers.RepeatedCompositeFieldContainer[InboxMessage]
        def __init__(self, messages: _Optional[_Iterable[_Union[InboxMessage, _Mapping]]] = ...) -> None: ...
    CALL_FIELD_NUMBER: _ClassVar[int]
    STEP_ENDED_FIELD_NUMBER: _ClassVar[int]
    SATISFIED_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIELD_NUMBER: _ClassVar[int]
    ARRIVED_FIELD_NUMBER: _ClassVar[int]
    STORED_FIELD_NUMBER: _ClassVar[int]
    call: RunRequest.MakeCall
    step_ended: RunRequest.StepEnded
    satisfied: int
    next: _layout_pb2.Time
    arrived: RunRequest.Arrived
    stored: _boundary_pb2.OpResult
    def __init__(self, call: _Optional[_Union[RunRequest.MakeCall, _Mapping]] = ..., step_ended: _Optional[_Union[RunRequest.StepEnded, _Mapping]] = ..., satisfied: _Optional[int] = ..., next: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ..., arrived: _Optional[_Union[RunRequest.Arrived, _Mapping]] = ..., stored: _Optional[_Union[_boundary_pb2.OpResult, _Mapping]] = ...) -> None: ...

class RunResponse(_message.Message):
    __slots__ = ("error", "called", "next", "done")
    ERROR_FIELD_NUMBER: _ClassVar[int]
    CALLED_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    error: _boundary_pb2.CoreError
    called: Called
    next: Next
    done: _boundary_pb2.Empty
    def __init__(self, error: _Optional[_Union[_boundary_pb2.CoreError, _Mapping]] = ..., called: _Optional[_Union[Called, _Mapping]] = ..., next: _Optional[_Union[Next, _Mapping]] = ..., done: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ...) -> None: ...
