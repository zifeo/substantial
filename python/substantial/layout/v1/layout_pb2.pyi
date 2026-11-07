from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STATUS_UNSPECIFIED: _ClassVar[Status]
    STATUS_COMPLETED: _ClassVar[Status]
    STATUS_FAILED: _ClassVar[Status]
    STATUS_CANCELLED: _ClassVar[Status]
    STATUS_CONTINUED: _ClassVar[Status]
STATUS_UNSPECIFIED: Status
STATUS_COMPLETED: Status
STATUS_FAILED: Status
STATUS_CANCELLED: Status
STATUS_CONTINUED: Status

class Time(_message.Message):
    __slots__ = ("ms",)
    MS_FIELD_NUMBER: _ClassVar[int]
    ms: int
    def __init__(self, ms: _Optional[int] = ...) -> None: ...

class Value(_message.Message):
    __slots__ = ("json", "raw")
    JSON_FIELD_NUMBER: _ClassVar[int]
    RAW_FIELD_NUMBER: _ClassVar[int]
    json: bytes
    raw: bytes
    def __init__(self, json: _Optional[bytes] = ..., raw: _Optional[bytes] = ...) -> None: ...

class Failure(_message.Message):
    __slots__ = ("type", "message", "retryable", "details")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    type: str
    message: str
    retryable: bool
    details: Value
    def __init__(self, type: _Optional[str] = ..., message: _Optional[str] = ..., retryable: _Optional[bool] = ..., details: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class Parent(_message.Message):
    __slots__ = ("run", "occurrence")
    RUN_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    run: str
    occurrence: int
    def __init__(self, run: _Optional[str] = ..., occurrence: _Optional[int] = ...) -> None: ...

class Seen(_message.Message):
    __slots__ = ("id", "at")
    ID_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    at: Time
    def __init__(self, id: _Optional[str] = ..., at: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class Start(_message.Message):
    __slots__ = ("workflow", "version", "input", "chain", "seq", "previous", "resumes", "parent", "state", "carried", "created_at", "seen")
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_FIELD_NUMBER: _ClassVar[int]
    RESUMES_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CARRIED_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    SEEN_FIELD_NUMBER: _ClassVar[int]
    workflow: str
    version: int
    input: Value
    chain: str
    seq: int
    previous: str
    resumes: str
    parent: Parent
    state: Value
    carried: _containers.RepeatedScalarFieldContainer[str]
    created_at: Time
    seen: _containers.RepeatedCompositeFieldContainer[Seen]
    def __init__(self, workflow: _Optional[str] = ..., version: _Optional[int] = ..., input: _Optional[_Union[Value, _Mapping]] = ..., chain: _Optional[str] = ..., seq: _Optional[int] = ..., previous: _Optional[str] = ..., resumes: _Optional[str] = ..., parent: _Optional[_Union[Parent, _Mapping]] = ..., state: _Optional[_Union[Value, _Mapping]] = ..., carried: _Optional[_Iterable[str]] = ..., created_at: _Optional[_Union[Time, _Mapping]] = ..., seen: _Optional[_Iterable[_Union[Seen, _Mapping]]] = ...) -> None: ...

class Step(_message.Message):
    __slots__ = ("name", "occurrence", "attempt", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    occurrence: int
    attempt: int
    value: Value
    def __init__(self, name: _Optional[str] = ..., occurrence: _Optional[int] = ..., attempt: _Optional[int] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class StepRetry(_message.Message):
    __slots__ = ("name", "occurrence", "attempt", "error", "at")
    NAME_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    name: str
    occurrence: int
    attempt: int
    error: Failure
    at: Time
    def __init__(self, name: _Optional[str] = ..., occurrence: _Optional[int] = ..., attempt: _Optional[int] = ..., error: _Optional[_Union[Failure, _Mapping]] = ..., at: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class StepFailed(_message.Message):
    __slots__ = ("name", "occurrence", "attempt", "error")
    NAME_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    name: str
    occurrence: int
    attempt: int
    error: Failure
    def __init__(self, name: _Optional[str] = ..., occurrence: _Optional[int] = ..., attempt: _Optional[int] = ..., error: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class Timer(_message.Message):
    __slots__ = ("occurrence", "until")
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    UNTIL_FIELD_NUMBER: _ClassVar[int]
    occurrence: int
    until: Time
    def __init__(self, occurrence: _Optional[int] = ..., until: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class Fired(_message.Message):
    __slots__ = ("occurrence",)
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    occurrence: int
    def __init__(self, occurrence: _Optional[int] = ...) -> None: ...

class MessageConsumed(_message.Message):
    __slots__ = ("key", "id", "name", "value", "ended", "at")
    KEY_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ENDED_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    key: str
    id: str
    name: str
    value: Value
    ended: Ended
    at: Time
    def __init__(self, key: _Optional[str] = ..., id: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., ended: _Optional[_Union[Ended, _Mapping]] = ..., at: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class Child(_message.Message):
    __slots__ = ("occurrence", "run", "workflow", "version", "input")
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    occurrence: int
    run: str
    workflow: str
    version: int
    input: Value
    def __init__(self, occurrence: _Optional[int] = ..., run: _Optional[str] = ..., workflow: _Optional[str] = ..., version: _Optional[int] = ..., input: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class Sent(_message.Message):
    __slots__ = ("occurrence", "run", "id", "name", "value", "to", "refused")
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    REFUSED_FIELD_NUMBER: _ClassVar[int]
    occurrence: int
    run: str
    id: str
    name: str
    value: Value
    to: Delivery
    refused: Failure
    def __init__(self, occurrence: _Optional[int] = ..., run: _Optional[str] = ..., id: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., to: _Optional[_Union[Delivery, _Mapping]] = ..., refused: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class Delivery(_message.Message):
    __slots__ = ("run", "workflow")
    RUN_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    run: str
    workflow: str
    def __init__(self, run: _Optional[str] = ..., workflow: _Optional[str] = ...) -> None: ...

class Published(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: Value
    def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class Completed(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: Value
    def __init__(self, value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class Failed(_message.Message):
    __slots__ = ("error",)
    ERROR_FIELD_NUMBER: _ClassVar[int]
    error: Failure
    def __init__(self, error: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class Cancelled(_message.Message):
    __slots__ = ("reason",)
    REASON_FIELD_NUMBER: _ClassVar[int]
    reason: str
    def __init__(self, reason: _Optional[str] = ...) -> None: ...

class Continued(_message.Message):
    __slots__ = ("run", "workflow", "input", "state", "carried", "version", "seen")
    RUN_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CARRIED_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    SEEN_FIELD_NUMBER: _ClassVar[int]
    run: str
    workflow: str
    input: Value
    state: Value
    carried: _containers.RepeatedScalarFieldContainer[str]
    version: int
    seen: _containers.RepeatedCompositeFieldContainer[Seen]
    def __init__(self, run: _Optional[str] = ..., workflow: _Optional[str] = ..., input: _Optional[_Union[Value, _Mapping]] = ..., state: _Optional[_Union[Value, _Mapping]] = ..., carried: _Optional[_Iterable[str]] = ..., version: _Optional[int] = ..., seen: _Optional[_Iterable[_Union[Seen, _Mapping]]] = ...) -> None: ...

class Event(_message.Message):
    __slots__ = ("step", "step_retry", "step_failed", "timer", "fired", "message", "child", "sent", "published", "completed", "failed", "cancelled", "continued")
    STEP_FIELD_NUMBER: _ClassVar[int]
    STEP_RETRY_FIELD_NUMBER: _ClassVar[int]
    STEP_FAILED_FIELD_NUMBER: _ClassVar[int]
    TIMER_FIELD_NUMBER: _ClassVar[int]
    FIRED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    CHILD_FIELD_NUMBER: _ClassVar[int]
    SENT_FIELD_NUMBER: _ClassVar[int]
    PUBLISHED_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    CONTINUED_FIELD_NUMBER: _ClassVar[int]
    step: Step
    step_retry: StepRetry
    step_failed: StepFailed
    timer: Timer
    fired: Fired
    message: MessageConsumed
    child: Child
    sent: Sent
    published: Published
    completed: Completed
    failed: Failed
    cancelled: Cancelled
    continued: Continued
    def __init__(self, step: _Optional[_Union[Step, _Mapping]] = ..., step_retry: _Optional[_Union[StepRetry, _Mapping]] = ..., step_failed: _Optional[_Union[StepFailed, _Mapping]] = ..., timer: _Optional[_Union[Timer, _Mapping]] = ..., fired: _Optional[_Union[Fired, _Mapping]] = ..., message: _Optional[_Union[MessageConsumed, _Mapping]] = ..., child: _Optional[_Union[Child, _Mapping]] = ..., sent: _Optional[_Union[Sent, _Mapping]] = ..., published: _Optional[_Union[Published, _Mapping]] = ..., completed: _Optional[_Union[Completed, _Mapping]] = ..., failed: _Optional[_Union[Failed, _Mapping]] = ..., cancelled: _Optional[_Union[Cancelled, _Mapping]] = ..., continued: _Optional[_Union[Continued, _Mapping]] = ...) -> None: ...

class Segment(_message.Message):
    __slots__ = ("n", "events", "next_wake", "worker", "created_at")
    N_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_WAKE_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    n: int
    events: _containers.RepeatedCompositeFieldContainer[Event]
    next_wake: Time
    worker: str
    created_at: Time
    def __init__(self, n: _Optional[int] = ..., events: _Optional[_Iterable[_Union[Event, _Mapping]]] = ..., next_wake: _Optional[_Union[Time, _Mapping]] = ..., worker: _Optional[str] = ..., created_at: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class Snapshot(_message.Message):
    __slots__ = ("n", "events", "next_wake")
    N_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_WAKE_FIELD_NUMBER: _ClassVar[int]
    n: int
    events: _containers.RepeatedCompositeFieldContainer[Event]
    next_wake: Time
    def __init__(self, n: _Optional[int] = ..., events: _Optional[_Iterable[_Union[Event, _Mapping]]] = ..., next_wake: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class Message(_message.Message):
    __slots__ = ("id", "name", "value", "sent_at", "ended")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    SENT_AT_FIELD_NUMBER: _ClassVar[int]
    ENDED_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    value: Value
    sent_at: Time
    ended: Ended
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., sent_at: _Optional[_Union[Time, _Mapping]] = ..., ended: _Optional[_Union[Ended, _Mapping]] = ...) -> None: ...

class Ended(_message.Message):
    __slots__ = ("run", "status", "value", "error", "reason")
    RUN_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    run: str
    status: Status
    value: Value
    error: Failure
    reason: str
    def __init__(self, run: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., error: _Optional[_Union[Failure, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class Claim(_message.Message):
    __slots__ = ("worker", "at", "ttl_ms")
    WORKER_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    TTL_MS_FIELD_NUMBER: _ClassVar[int]
    worker: str
    at: Time
    ttl_ms: int
    def __init__(self, worker: _Optional[str] = ..., at: _Optional[_Union[Time, _Mapping]] = ..., ttl_ms: _Optional[int] = ...) -> None: ...

class Result(_message.Message):
    __slots__ = ("status", "value", "error", "reason", "next", "ended_at")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIELD_NUMBER: _ClassVar[int]
    ENDED_AT_FIELD_NUMBER: _ClassVar[int]
    status: Status
    value: Value
    error: Failure
    reason: str
    next: str
    ended_at: Time
    def __init__(self, status: _Optional[_Union[Status, str]] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., error: _Optional[_Union[Failure, _Mapping]] = ..., reason: _Optional[str] = ..., next: _Optional[str] = ..., ended_at: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class PublishedValue(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: Value
    def __init__(self, value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...
