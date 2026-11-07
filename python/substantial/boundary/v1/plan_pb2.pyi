from substantial.boundary.v1 import boundary_pb2 as _boundary_pb2
from substantial.layout.v1 import layout_pb2 as _layout_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RunStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_STATUS_UNSPECIFIED: _ClassVar[RunStatus]
    RUN_STATUS_PENDING: _ClassVar[RunStatus]
    RUN_STATUS_RUNNING: _ClassVar[RunStatus]
    RUN_STATUS_SUSPENDED: _ClassVar[RunStatus]
    RUN_STATUS_COMPLETED: _ClassVar[RunStatus]
    RUN_STATUS_FAILED: _ClassVar[RunStatus]
    RUN_STATUS_CANCELLED: _ClassVar[RunStatus]
RUN_STATUS_UNSPECIFIED: RunStatus
RUN_STATUS_PENDING: RunStatus
RUN_STATUS_RUNNING: RunStatus
RUN_STATUS_SUSPENDED: RunStatus
RUN_STATUS_COMPLETED: RunStatus
RUN_STATUS_FAILED: RunStatus
RUN_STATUS_CANCELLED: RunStatus

class PlanInput(_message.Message):
    __slots__ = ("send", "create", "open", "start", "result", "status", "cancel", "read")
    SEND_FIELD_NUMBER: _ClassVar[int]
    CREATE_FIELD_NUMBER: _ClassVar[int]
    OPEN_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CANCEL_FIELD_NUMBER: _ClassVar[int]
    READ_FIELD_NUMBER: _ClassVar[int]
    send: SendPlan
    create: CreatePlan
    open: OpenPlan
    start: StartPlan
    result: ResultPlan
    status: StatusPlan
    cancel: CancelPlan
    read: ReadPlan
    def __init__(self, send: _Optional[_Union[SendPlan, _Mapping]] = ..., create: _Optional[_Union[CreatePlan, _Mapping]] = ..., open: _Optional[_Union[OpenPlan, _Mapping]] = ..., start: _Optional[_Union[StartPlan, _Mapping]] = ..., result: _Optional[_Union[ResultPlan, _Mapping]] = ..., status: _Optional[_Union[StatusPlan, _Mapping]] = ..., cancel: _Optional[_Union[CancelPlan, _Mapping]] = ..., read: _Optional[_Union[ReadPlan, _Mapping]] = ...) -> None: ...

class SendPlan(_message.Message):
    __slots__ = ("prefix", "partitions", "chain", "name", "value", "id", "now")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    partitions: int
    chain: str
    name: str
    value: _layout_pb2.Value
    id: str
    now: _layout_pb2.Time
    def __init__(self, prefix: _Optional[str] = ..., partitions: _Optional[int] = ..., chain: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., id: _Optional[str] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...

class CreatePlan(_message.Message):
    __slots__ = ("prefix", "partitions")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    partitions: int
    def __init__(self, prefix: _Optional[str] = ..., partitions: _Optional[int] = ...) -> None: ...

class OpenPlan(_message.Message):
    __slots__ = ("prefix",)
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    def __init__(self, prefix: _Optional[str] = ...) -> None: ...

class StartPlan(_message.Message):
    __slots__ = ("prefix", "partitions", "workflow", "version", "input", "id", "now")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    partitions: int
    workflow: str
    version: int
    input: _layout_pb2.Value
    id: str
    now: _layout_pb2.Time
    def __init__(self, prefix: _Optional[str] = ..., partitions: _Optional[int] = ..., workflow: _Optional[str] = ..., version: _Optional[int] = ..., input: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., id: _Optional[str] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...

class ResultPlan(_message.Message):
    __slots__ = ("prefix", "partitions", "chain", "run")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    partitions: int
    chain: str
    run: str
    def __init__(self, prefix: _Optional[str] = ..., partitions: _Optional[int] = ..., chain: _Optional[str] = ..., run: _Optional[str] = ...) -> None: ...

class StatusPlan(_message.Message):
    __slots__ = ("prefix", "partitions", "chain", "now")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    partitions: int
    chain: str
    now: _layout_pb2.Time
    def __init__(self, prefix: _Optional[str] = ..., partitions: _Optional[int] = ..., chain: _Optional[str] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...

class CancelPlan(_message.Message):
    __slots__ = ("prefix", "partitions", "chain", "reason", "id", "now")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    partitions: int
    chain: str
    reason: str
    id: str
    now: _layout_pb2.Time
    def __init__(self, prefix: _Optional[str] = ..., partitions: _Optional[int] = ..., chain: _Optional[str] = ..., reason: _Optional[str] = ..., id: _Optional[str] = ..., now: _Optional[_Union[_layout_pb2.Time, _Mapping]] = ...) -> None: ...

class ReadPlan(_message.Message):
    __slots__ = ("prefix", "partitions", "chain", "key")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    partitions: int
    chain: str
    key: str
    def __init__(self, prefix: _Optional[str] = ..., partitions: _Optional[int] = ..., chain: _Optional[str] = ..., key: _Optional[str] = ...) -> None: ...

class PlanRequest(_message.Message):
    __slots__ = ("next", "feed")
    NEXT_FIELD_NUMBER: _ClassVar[int]
    FEED_FIELD_NUMBER: _ClassVar[int]
    next: _boundary_pb2.Empty
    feed: _boundary_pb2.OpResult
    def __init__(self, next: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., feed: _Optional[_Union[_boundary_pb2.OpResult, _Mapping]] = ...) -> None: ...

class Sent(_message.Message):
    __slots__ = ("accepted", "refused", "no_chain", "failed")
    class Refused(_message.Message):
        __slots__ = ("run", "status")
        RUN_FIELD_NUMBER: _ClassVar[int]
        STATUS_FIELD_NUMBER: _ClassVar[int]
        run: str
        status: _layout_pb2.Status
        def __init__(self, run: _Optional[str] = ..., status: _Optional[_Union[_layout_pb2.Status, str]] = ...) -> None: ...
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    REFUSED_FIELD_NUMBER: _ClassVar[int]
    NO_CHAIN_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    accepted: str
    refused: Sent.Refused
    no_chain: _boundary_pb2.Empty
    failed: _boundary_pb2.StoreFailure
    def __init__(self, accepted: _Optional[str] = ..., refused: _Optional[_Union[Sent.Refused, _Mapping]] = ..., no_chain: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ...) -> None: ...

class Opened(_message.Message):
    __slots__ = ("partitions", "layout_error", "failed")
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    LAYOUT_ERROR_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    partitions: int
    layout_error: str
    failed: _boundary_pb2.StoreFailure
    def __init__(self, partitions: _Optional[int] = ..., layout_error: _Optional[str] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ...) -> None: ...

class Started(_message.Message):
    __slots__ = ("created", "existing", "run_exists", "failed")
    CREATED_FIELD_NUMBER: _ClassVar[int]
    EXISTING_FIELD_NUMBER: _ClassVar[int]
    RUN_EXISTS_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    created: _boundary_pb2.Empty
    existing: _boundary_pb2.Empty
    run_exists: _boundary_pb2.Empty
    failed: _boundary_pb2.StoreFailure
    def __init__(self, created: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., existing: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., run_exists: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ...) -> None: ...

class RunResult(_message.Message):
    __slots__ = ("ended", "no_chain", "failed", "not_ended")
    class Ended(_message.Message):
        __slots__ = ("run", "result")
        RUN_FIELD_NUMBER: _ClassVar[int]
        RESULT_FIELD_NUMBER: _ClassVar[int]
        run: str
        result: _layout_pb2.Result
        def __init__(self, run: _Optional[str] = ..., result: _Optional[_Union[_layout_pb2.Result, _Mapping]] = ...) -> None: ...
    class NotEnded(_message.Message):
        __slots__ = ("run", "result_key")
        RUN_FIELD_NUMBER: _ClassVar[int]
        RESULT_KEY_FIELD_NUMBER: _ClassVar[int]
        run: str
        result_key: str
        def __init__(self, run: _Optional[str] = ..., result_key: _Optional[str] = ...) -> None: ...
    ENDED_FIELD_NUMBER: _ClassVar[int]
    NO_CHAIN_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    NOT_ENDED_FIELD_NUMBER: _ClassVar[int]
    ended: RunResult.Ended
    no_chain: _boundary_pb2.Empty
    failed: _boundary_pb2.StoreFailure
    not_ended: RunResult.NotEnded
    def __init__(self, ended: _Optional[_Union[RunResult.Ended, _Mapping]] = ..., no_chain: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ..., not_ended: _Optional[_Union[RunResult.NotEnded, _Mapping]] = ...) -> None: ...

class RunState(_message.Message):
    __slots__ = ("status", "no_chain", "failed")
    class Of(_message.Message):
        __slots__ = ("run", "status")
        RUN_FIELD_NUMBER: _ClassVar[int]
        STATUS_FIELD_NUMBER: _ClassVar[int]
        run: str
        status: RunStatus
        def __init__(self, run: _Optional[str] = ..., status: _Optional[_Union[RunStatus, str]] = ...) -> None: ...
    STATUS_FIELD_NUMBER: _ClassVar[int]
    NO_CHAIN_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    status: RunState.Of
    no_chain: _boundary_pb2.Empty
    failed: _boundary_pb2.StoreFailure
    def __init__(self, status: _Optional[_Union[RunState.Of, _Mapping]] = ..., no_chain: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ...) -> None: ...

class ReadValue(_message.Message):
    __slots__ = ("value", "none", "failed")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    NONE_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    value: _layout_pb2.Value
    none: _boundary_pb2.Empty
    failed: _boundary_pb2.StoreFailure
    def __init__(self, value: _Optional[_Union[_layout_pb2.Value, _Mapping]] = ..., none: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ..., failed: _Optional[_Union[_boundary_pb2.StoreFailure, _Mapping]] = ...) -> None: ...

class PlanStep(_message.Message):
    __slots__ = ("store", "sent", "opened", "started", "result", "status", "read")
    STORE_FIELD_NUMBER: _ClassVar[int]
    SENT_FIELD_NUMBER: _ClassVar[int]
    OPENED_FIELD_NUMBER: _ClassVar[int]
    STARTED_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    READ_FIELD_NUMBER: _ClassVar[int]
    store: _boundary_pb2.StoreOp
    sent: Sent
    opened: Opened
    started: Started
    result: RunResult
    status: RunState
    read: ReadValue
    def __init__(self, store: _Optional[_Union[_boundary_pb2.StoreOp, _Mapping]] = ..., sent: _Optional[_Union[Sent, _Mapping]] = ..., opened: _Optional[_Union[Opened, _Mapping]] = ..., started: _Optional[_Union[Started, _Mapping]] = ..., result: _Optional[_Union[RunResult, _Mapping]] = ..., status: _Optional[_Union[RunState, _Mapping]] = ..., read: _Optional[_Union[ReadValue, _Mapping]] = ...) -> None: ...

class PlanResponse(_message.Message):
    __slots__ = ("error", "step", "done")
    ERROR_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    error: _boundary_pb2.CoreError
    step: PlanStep
    done: _boundary_pb2.Empty
    def __init__(self, error: _Optional[_Union[_boundary_pb2.CoreError, _Mapping]] = ..., step: _Optional[_Union[PlanStep, _Mapping]] = ..., done: _Optional[_Union[_boundary_pb2.Empty, _Mapping]] = ...) -> None: ...
