from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
STATUS_CANCELLED: Status
STATUS_COMPLETED: Status
STATUS_CONTINUED: Status
STATUS_FAILED: Status
STATUS_UNSPECIFIED: Status

class Amount(_message.Message):
    __slots__ = ["currency", "nano"]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    NANO_FIELD_NUMBER: _ClassVar[int]
    currency: str
    nano: int
    def __init__(self, currency: _Optional[str] = ..., nano: _Optional[int] = ...) -> None: ...

class Blocked(_message.Message):
    __slots__ = ["at", "attempt", "error", "n", "retry_at", "worker"]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    N_FIELD_NUMBER: _ClassVar[int]
    RETRY_AT_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    at: Time
    attempt: int
    error: Failure
    n: int
    retry_at: Time
    worker: str
    def __init__(self, error: _Optional[_Union[Failure, _Mapping]] = ..., at: _Optional[_Union[Time, _Mapping]] = ..., worker: _Optional[str] = ..., retry_at: _Optional[_Union[Time, _Mapping]] = ..., attempt: _Optional[int] = ..., n: _Optional[int] = ...) -> None: ...

class Budget(_message.Message):
    __slots__ = ["limits"]
    LIMITS_FIELD_NUMBER: _ClassVar[int]
    limits: _containers.RepeatedCompositeFieldContainer[Limit]
    def __init__(self, limits: _Optional[_Iterable[_Union[Limit, _Mapping]]] = ...) -> None: ...

class BudgetExceeded(_message.Message):
    __slots__ = ["limit", "meter", "used"]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    METER_FIELD_NUMBER: _ClassVar[int]
    USED_FIELD_NUMBER: _ClassVar[int]
    limit: int
    meter: str
    used: int
    def __init__(self, meter: _Optional[str] = ..., limit: _Optional[int] = ..., used: _Optional[int] = ...) -> None: ...

class Cancelled(_message.Message):
    __slots__ = ["reason"]
    REASON_FIELD_NUMBER: _ClassVar[int]
    reason: str
    def __init__(self, reason: _Optional[str] = ...) -> None: ...

class Child(_message.Message):
    __slots__ = ["abandon", "budget", "input", "occurrence", "run", "version", "workflow"]
    ABANDON_FIELD_NUMBER: _ClassVar[int]
    BUDGET_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    abandon: bool
    budget: Budget
    input: Value
    occurrence: int
    run: str
    version: int
    workflow: str
    def __init__(self, occurrence: _Optional[int] = ..., run: _Optional[str] = ..., workflow: _Optional[str] = ..., version: _Optional[int] = ..., input: _Optional[_Union[Value, _Mapping]] = ..., budget: _Optional[_Union[Budget, _Mapping]] = ..., abandon: bool = ...) -> None: ...

class Completed(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: Value
    def __init__(self, value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class Continued(_message.Message):
    __slots__ = ["carried", "input", "run", "seen", "state", "version", "workflow"]
    CARRIED_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    SEEN_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    carried: _containers.RepeatedScalarFieldContainer[str]
    input: Value
    run: str
    seen: _containers.RepeatedCompositeFieldContainer[Seen]
    state: Value
    version: int
    workflow: str
    def __init__(self, run: _Optional[str] = ..., workflow: _Optional[str] = ..., input: _Optional[_Union[Value, _Mapping]] = ..., state: _Optional[_Union[Value, _Mapping]] = ..., carried: _Optional[_Iterable[str]] = ..., version: _Optional[int] = ..., seen: _Optional[_Iterable[_Union[Seen, _Mapping]]] = ...) -> None: ...

class Cost(_message.Message):
    __slots__ = ["children", "steps", "store"]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    STORE_FIELD_NUMBER: _ClassVar[int]
    children: Meters
    steps: _containers.RepeatedCompositeFieldContainer[StepCharges]
    store: Meters
    def __init__(self, store: _Optional[_Union[Meters, _Mapping]] = ..., steps: _Optional[_Iterable[_Union[StepCharges, _Mapping]]] = ..., children: _Optional[_Union[Meters, _Mapping]] = ...) -> None: ...

class Delivery(_message.Message):
    __slots__ = ["run", "workflow"]
    RUN_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    run: str
    workflow: str
    def __init__(self, run: _Optional[str] = ..., workflow: _Optional[str] = ...) -> None: ...

class Ended(_message.Message):
    __slots__ = ["cost", "error", "reason", "run", "status", "value"]
    COST_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    cost: Cost
    error: Failure
    reason: str
    run: str
    status: Status
    value: Value
    def __init__(self, run: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., error: _Optional[_Union[Failure, _Mapping]] = ..., reason: _Optional[str] = ..., cost: _Optional[_Union[Cost, _Mapping]] = ...) -> None: ...

class Event(_message.Message):
    __slots__ = ["budget_exceeded", "cancelled", "child", "completed", "continued", "failed", "fired", "message", "patched", "published", "sent", "step", "step_failed", "step_retry", "timer", "watched"]
    BUDGET_EXCEEDED_FIELD_NUMBER: _ClassVar[int]
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    CHILD_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    CONTINUED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    FIRED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    PATCHED_FIELD_NUMBER: _ClassVar[int]
    PUBLISHED_FIELD_NUMBER: _ClassVar[int]
    SENT_FIELD_NUMBER: _ClassVar[int]
    STEP_FAILED_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    STEP_RETRY_FIELD_NUMBER: _ClassVar[int]
    TIMER_FIELD_NUMBER: _ClassVar[int]
    WATCHED_FIELD_NUMBER: _ClassVar[int]
    budget_exceeded: BudgetExceeded
    cancelled: Cancelled
    child: Child
    completed: Completed
    continued: Continued
    failed: Failed
    fired: Fired
    message: MessageConsumed
    patched: Patched
    published: Published
    sent: Sent
    step: Step
    step_failed: StepFailed
    step_retry: StepRetry
    timer: Timer
    watched: Watched
    def __init__(self, step: _Optional[_Union[Step, _Mapping]] = ..., step_retry: _Optional[_Union[StepRetry, _Mapping]] = ..., step_failed: _Optional[_Union[StepFailed, _Mapping]] = ..., timer: _Optional[_Union[Timer, _Mapping]] = ..., fired: _Optional[_Union[Fired, _Mapping]] = ..., message: _Optional[_Union[MessageConsumed, _Mapping]] = ..., child: _Optional[_Union[Child, _Mapping]] = ..., sent: _Optional[_Union[Sent, _Mapping]] = ..., published: _Optional[_Union[Published, _Mapping]] = ..., completed: _Optional[_Union[Completed, _Mapping]] = ..., failed: _Optional[_Union[Failed, _Mapping]] = ..., cancelled: _Optional[_Union[Cancelled, _Mapping]] = ..., continued: _Optional[_Union[Continued, _Mapping]] = ..., budget_exceeded: _Optional[_Union[BudgetExceeded, _Mapping]] = ..., watched: _Optional[_Union[Watched, _Mapping]] = ..., patched: _Optional[_Union[Patched, _Mapping]] = ...) -> None: ...

class Failed(_message.Message):
    __slots__ = ["error"]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    error: Failure
    def __init__(self, error: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class Failure(_message.Message):
    __slots__ = ["details", "message", "retryable", "type"]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    details: Value
    message: str
    retryable: bool
    type: str
    def __init__(self, type: _Optional[str] = ..., message: _Optional[str] = ..., retryable: bool = ..., details: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class Fired(_message.Message):
    __slots__ = ["occurrence"]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    occurrence: int
    def __init__(self, occurrence: _Optional[int] = ...) -> None: ...

class Head(_message.Message):
    __slots__ = ["archive", "blocked", "children", "first", "inbox", "lease", "next", "origins", "parent", "pending", "pointer_etag", "published", "resumed", "run", "segments", "start"]
    ARCHIVE_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_FIELD_NUMBER: _ClassVar[int]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    FIRST_FIELD_NUMBER: _ClassVar[int]
    INBOX_FIELD_NUMBER: _ClassVar[int]
    LEASE_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIELD_NUMBER: _ClassVar[int]
    ORIGINS_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    PENDING_FIELD_NUMBER: _ClassVar[int]
    POINTER_ETAG_FIELD_NUMBER: _ClassVar[int]
    PUBLISHED_FIELD_NUMBER: _ClassVar[int]
    RESUMED_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    archive: bytes
    blocked: Blocked
    children: _containers.RepeatedScalarFieldContainer[str]
    first: Start
    inbox: _containers.RepeatedCompositeFieldContainer[Inboxed]
    lease: Lease
    next: str
    origins: _containers.RepeatedCompositeFieldContainer[Origin]
    parent: ParentChain
    pending: int
    pointer_etag: str
    published: _containers.RepeatedCompositeFieldContainer[Published]
    resumed: _containers.RepeatedCompositeFieldContainer[Event]
    run: str
    segments: _containers.RepeatedCompositeFieldContainer[Segment]
    start: Start
    def __init__(self, run: _Optional[str] = ..., start: _Optional[_Union[Start, _Mapping]] = ..., resumed: _Optional[_Iterable[_Union[Event, _Mapping]]] = ..., origins: _Optional[_Iterable[_Union[Origin, _Mapping]]] = ..., segments: _Optional[_Iterable[_Union[Segment, _Mapping]]] = ..., inbox: _Optional[_Iterable[_Union[Inboxed, _Mapping]]] = ..., lease: _Optional[_Union[Lease, _Mapping]] = ..., pending: _Optional[int] = ..., archive: _Optional[bytes] = ..., published: _Optional[_Iterable[_Union[Published, _Mapping]]] = ..., parent: _Optional[_Union[ParentChain, _Mapping]] = ..., children: _Optional[_Iterable[str]] = ..., blocked: _Optional[_Union[Blocked, _Mapping]] = ..., pointer_etag: _Optional[str] = ..., next: _Optional[str] = ..., first: _Optional[_Union[Start, _Mapping]] = ...) -> None: ...

class Inboxed(_message.Message):
    __slots__ = ["body", "key"]
    BODY_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    body: Message
    key: str
    def __init__(self, key: _Optional[str] = ..., body: _Optional[_Union[Message, _Mapping]] = ...) -> None: ...

class Lease(_message.Message):
    __slots__ = ["at", "ttl_ms", "worker"]
    AT_FIELD_NUMBER: _ClassVar[int]
    TTL_MS_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    at: Time
    ttl_ms: int
    worker: str
    def __init__(self, worker: _Optional[str] = ..., at: _Optional[_Union[Time, _Mapping]] = ..., ttl_ms: _Optional[int] = ...) -> None: ...

class Limit(_message.Message):
    __slots__ = ["limit", "meter"]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    METER_FIELD_NUMBER: _ClassVar[int]
    limit: int
    meter: str
    def __init__(self, meter: _Optional[str] = ..., limit: _Optional[int] = ...) -> None: ...

class Message(_message.Message):
    __slots__ = ["ended", "id", "name", "sent_at", "value"]
    ENDED_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SENT_AT_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ended: Ended
    id: str
    name: str
    sent_at: Time
    value: Value
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., sent_at: _Optional[_Union[Time, _Mapping]] = ..., ended: _Optional[_Union[Ended, _Mapping]] = ...) -> None: ...

class MessageConsumed(_message.Message):
    __slots__ = ["at", "ended", "id", "key", "name", "value"]
    AT_FIELD_NUMBER: _ClassVar[int]
    ENDED_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    at: Time
    ended: Ended
    id: str
    key: str
    name: str
    value: Value
    def __init__(self, key: _Optional[str] = ..., id: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., ended: _Optional[_Union[Ended, _Mapping]] = ..., at: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class Meters(_message.Message):
    __slots__ = ["amounts", "requests"]
    AMOUNTS_FIELD_NUMBER: _ClassVar[int]
    REQUESTS_FIELD_NUMBER: _ClassVar[int]
    amounts: _containers.RepeatedCompositeFieldContainer[Amount]
    requests: Requests
    def __init__(self, requests: _Optional[_Union[Requests, _Mapping]] = ..., amounts: _Optional[_Iterable[_Union[Amount, _Mapping]]] = ...) -> None: ...

class Origin(_message.Message):
    __slots__ = ["created_at", "events", "n", "run"]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    N_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    created_at: Time
    events: int
    n: int
    run: str
    def __init__(self, run: _Optional[str] = ..., n: _Optional[int] = ..., created_at: _Optional[_Union[Time, _Mapping]] = ..., events: _Optional[int] = ...) -> None: ...

class Parent(_message.Message):
    __slots__ = ["abandon", "occurrence", "run", "segment"]
    ABANDON_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    abandon: bool
    occurrence: int
    run: str
    segment: int
    def __init__(self, run: _Optional[str] = ..., occurrence: _Optional[int] = ..., segment: _Optional[int] = ..., abandon: bool = ...) -> None: ...

class ParentChain(_message.Message):
    __slots__ = ["chain", "workflow"]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    chain: str
    workflow: str
    def __init__(self, chain: _Optional[str] = ..., workflow: _Optional[str] = ...) -> None: ...

class Patched(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class Pointer(_message.Message):
    __slots__ = ["chain"]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    chain: str
    def __init__(self, chain: _Optional[str] = ...) -> None: ...

class Published(_message.Message):
    __slots__ = ["key", "value"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: Value
    def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class Requests(_message.Message):
    __slots__ = ["cas", "create", "delete", "get", "list"]
    CAS_FIELD_NUMBER: _ClassVar[int]
    CREATE_FIELD_NUMBER: _ClassVar[int]
    DELETE_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    LIST_FIELD_NUMBER: _ClassVar[int]
    cas: int
    create: int
    delete: int
    get: int
    list: int
    def __init__(self, create: _Optional[int] = ..., get: _Optional[int] = ..., list: _Optional[int] = ..., delete: _Optional[int] = ..., cas: _Optional[int] = ...) -> None: ...

class Result(_message.Message):
    __slots__ = ["ended_at", "error", "next", "reason", "status", "value"]
    ENDED_AT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ended_at: Time
    error: Failure
    next: str
    reason: str
    status: Status
    value: Value
    def __init__(self, status: _Optional[_Union[Status, str]] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., error: _Optional[_Union[Failure, _Mapping]] = ..., reason: _Optional[str] = ..., next: _Optional[str] = ..., ended_at: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class Run(_message.Message):
    __slots__ = ["head", "pointer"]
    HEAD_FIELD_NUMBER: _ClassVar[int]
    POINTER_FIELD_NUMBER: _ClassVar[int]
    head: Head
    pointer: Pointer
    def __init__(self, head: _Optional[_Union[Head, _Mapping]] = ..., pointer: _Optional[_Union[Pointer, _Mapping]] = ...) -> None: ...

class Seen(_message.Message):
    __slots__ = ["at", "id"]
    AT_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    at: Time
    id: str
    def __init__(self, id: _Optional[str] = ..., at: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class Segment(_message.Message):
    __slots__ = ["children", "cost", "created_at", "events", "n", "next_wake", "rest", "retention_days", "worker"]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    COST_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_WAKE_FIELD_NUMBER: _ClassVar[int]
    N_FIELD_NUMBER: _ClassVar[int]
    REST_FIELD_NUMBER: _ClassVar[int]
    RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    children: bool
    cost: Cost
    created_at: Time
    events: _containers.RepeatedCompositeFieldContainer[Event]
    n: int
    next_wake: Time
    rest: bool
    retention_days: int
    worker: str
    def __init__(self, n: _Optional[int] = ..., events: _Optional[_Iterable[_Union[Event, _Mapping]]] = ..., next_wake: _Optional[_Union[Time, _Mapping]] = ..., worker: _Optional[str] = ..., created_at: _Optional[_Union[Time, _Mapping]] = ..., retention_days: _Optional[int] = ..., rest: bool = ..., cost: _Optional[_Union[Cost, _Mapping]] = ..., children: bool = ...) -> None: ...

class Sent(_message.Message):
    __slots__ = ["id", "name", "occurrence", "refused", "run", "to", "value"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    REFUSED_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    occurrence: int
    refused: Failure
    run: str
    to: Delivery
    value: Value
    def __init__(self, occurrence: _Optional[int] = ..., run: _Optional[str] = ..., id: _Optional[str] = ..., name: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., to: _Optional[_Union[Delivery, _Mapping]] = ..., refused: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class Start(_message.Message):
    __slots__ = ["budget", "carried", "chain", "children", "cost", "created_at", "input", "keep", "line", "parent", "previous", "resumes", "seen", "seq", "state", "version", "workflow"]
    BUDGET_FIELD_NUMBER: _ClassVar[int]
    CARRIED_FIELD_NUMBER: _ClassVar[int]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    COST_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    KEEP_FIELD_NUMBER: _ClassVar[int]
    LINE_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_FIELD_NUMBER: _ClassVar[int]
    RESUMES_FIELD_NUMBER: _ClassVar[int]
    SEEN_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    budget: Budget
    carried: _containers.RepeatedScalarFieldContainer[str]
    chain: str
    children: bool
    cost: Cost
    created_at: Time
    input: Value
    keep: int
    line: str
    parent: Parent
    previous: str
    resumes: str
    seen: _containers.RepeatedCompositeFieldContainer[Seen]
    seq: int
    state: Value
    version: int
    workflow: str
    def __init__(self, workflow: _Optional[str] = ..., version: _Optional[int] = ..., input: _Optional[_Union[Value, _Mapping]] = ..., chain: _Optional[str] = ..., seq: _Optional[int] = ..., previous: _Optional[str] = ..., resumes: _Optional[str] = ..., parent: _Optional[_Union[Parent, _Mapping]] = ..., state: _Optional[_Union[Value, _Mapping]] = ..., carried: _Optional[_Iterable[str]] = ..., created_at: _Optional[_Union[Time, _Mapping]] = ..., seen: _Optional[_Iterable[_Union[Seen, _Mapping]]] = ..., line: _Optional[str] = ..., budget: _Optional[_Union[Budget, _Mapping]] = ..., cost: _Optional[_Union[Cost, _Mapping]] = ..., keep: _Optional[int] = ..., children: bool = ...) -> None: ...

class Step(_message.Message):
    __slots__ = ["attempt", "charges", "name", "occurrence", "value"]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    CHARGES_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    attempt: int
    charges: _containers.RepeatedCompositeFieldContainer[Amount]
    name: str
    occurrence: int
    value: Value
    def __init__(self, name: _Optional[str] = ..., occurrence: _Optional[int] = ..., attempt: _Optional[int] = ..., value: _Optional[_Union[Value, _Mapping]] = ..., charges: _Optional[_Iterable[_Union[Amount, _Mapping]]] = ...) -> None: ...

class StepCharges(_message.Message):
    __slots__ = ["amounts", "step"]
    AMOUNTS_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    amounts: _containers.RepeatedCompositeFieldContainer[Amount]
    step: str
    def __init__(self, step: _Optional[str] = ..., amounts: _Optional[_Iterable[_Union[Amount, _Mapping]]] = ...) -> None: ...

class StepFailed(_message.Message):
    __slots__ = ["attempt", "charges", "error", "name", "occurrence"]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    CHARGES_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    attempt: int
    charges: _containers.RepeatedCompositeFieldContainer[Amount]
    error: Failure
    name: str
    occurrence: int
    def __init__(self, name: _Optional[str] = ..., occurrence: _Optional[int] = ..., attempt: _Optional[int] = ..., error: _Optional[_Union[Failure, _Mapping]] = ..., charges: _Optional[_Iterable[_Union[Amount, _Mapping]]] = ...) -> None: ...

class StepRetry(_message.Message):
    __slots__ = ["at", "attempt", "charges", "deadline", "error", "name", "occurrence"]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    CHARGES_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    at: Time
    attempt: int
    charges: _containers.RepeatedCompositeFieldContainer[Amount]
    deadline: Time
    error: Failure
    name: str
    occurrence: int
    def __init__(self, name: _Optional[str] = ..., occurrence: _Optional[int] = ..., attempt: _Optional[int] = ..., error: _Optional[_Union[Failure, _Mapping]] = ..., at: _Optional[_Union[Time, _Mapping]] = ..., charges: _Optional[_Iterable[_Union[Amount, _Mapping]]] = ..., deadline: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class Time(_message.Message):
    __slots__ = ["ms"]
    MS_FIELD_NUMBER: _ClassVar[int]
    ms: int
    def __init__(self, ms: _Optional[int] = ...) -> None: ...

class Timer(_message.Message):
    __slots__ = ["occurrence", "until"]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    UNTIL_FIELD_NUMBER: _ClassVar[int]
    occurrence: int
    until: Time
    def __init__(self, occurrence: _Optional[int] = ..., until: _Optional[_Union[Time, _Mapping]] = ...) -> None: ...

class Value(_message.Message):
    __slots__ = ["json", "raw"]
    JSON_FIELD_NUMBER: _ClassVar[int]
    RAW_FIELD_NUMBER: _ClassVar[int]
    json: bytes
    raw: bytes
    def __init__(self, json: _Optional[bytes] = ..., raw: _Optional[bytes] = ...) -> None: ...

class Watched(_message.Message):
    __slots__ = ["chain", "ended", "occurrence", "refused", "waiting"]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    ENDED_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    REFUSED_FIELD_NUMBER: _ClassVar[int]
    WAITING_FIELD_NUMBER: _ClassVar[int]
    chain: str
    ended: Ended
    occurrence: int
    refused: Failure
    waiting: str
    def __init__(self, occurrence: _Optional[int] = ..., chain: _Optional[str] = ..., waiting: _Optional[str] = ..., ended: _Optional[_Union[Ended, _Mapping]] = ..., refused: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
