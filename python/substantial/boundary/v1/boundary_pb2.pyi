from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
ERROR_KIND_CORRUPT: ErrorKind
ERROR_KIND_ENDED: ErrorKind
ERROR_KIND_INVALID: ErrorKind
ERROR_KIND_LAYOUT: ErrorKind
ERROR_KIND_NON_DETERMINISM: ErrorKind
ERROR_KIND_UNSPECIFIED: ErrorKind
ERROR_KIND_VALUE_TOO_LARGE: ErrorKind
MACHINE_KIND_AGENT: MachineKind
MACHINE_KIND_PLAN: MachineKind
MACHINE_KIND_RUN: MachineKind
MACHINE_KIND_UNSPECIFIED: MachineKind

class CoreError(_message.Message):
    __slots__ = ["kind", "message"]
    KIND_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    kind: ErrorKind
    message: str
    def __init__(self, kind: _Optional[_Union[ErrorKind, str]] = ..., message: _Optional[str] = ...) -> None: ...

class Empty(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ListWait(_message.Message):
    __slots__ = ["etag", "seconds"]
    ETAG_FIELD_NUMBER: _ClassVar[int]
    SECONDS_FIELD_NUMBER: _ClassVar[int]
    etag: str
    seconds: int
    def __init__(self, etag: _Optional[str] = ..., seconds: _Optional[int] = ...) -> None: ...

class OpResult(_message.Message):
    __slots__ = ["attempts", "created", "deleted", "exists", "failed", "listing", "mismatch", "missing", "object", "written"]
    class Listing(_message.Message):
        __slots__ = ["etag", "keys"]
        ETAG_FIELD_NUMBER: _ClassVar[int]
        KEYS_FIELD_NUMBER: _ClassVar[int]
        etag: str
        keys: _containers.RepeatedScalarFieldContainer[str]
        def __init__(self, keys: _Optional[_Iterable[str]] = ..., etag: _Optional[str] = ...) -> None: ...
    class Object(_message.Message):
        __slots__ = ["bytes", "etag"]
        BYTES_FIELD_NUMBER: _ClassVar[int]
        ETAG_FIELD_NUMBER: _ClassVar[int]
        bytes: bytes
        etag: str
        def __init__(self, bytes: _Optional[bytes] = ..., etag: _Optional[str] = ...) -> None: ...
    class Written(_message.Message):
        __slots__ = ["etag"]
        ETAG_FIELD_NUMBER: _ClassVar[int]
        etag: str
        def __init__(self, etag: _Optional[str] = ...) -> None: ...
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    DELETED_FIELD_NUMBER: _ClassVar[int]
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    LISTING_FIELD_NUMBER: _ClassVar[int]
    MISMATCH_FIELD_NUMBER: _ClassVar[int]
    MISSING_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    WRITTEN_FIELD_NUMBER: _ClassVar[int]
    attempts: int
    created: OpResult.Written
    deleted: Empty
    exists: Empty
    failed: StoreFailure
    listing: OpResult.Listing
    mismatch: Empty
    missing: Empty
    object: OpResult.Object
    written: OpResult.Written
    def __init__(self, created: _Optional[_Union[OpResult.Written, _Mapping]] = ..., exists: _Optional[_Union[Empty, _Mapping]] = ..., object: _Optional[_Union[OpResult.Object, _Mapping]] = ..., missing: _Optional[_Union[Empty, _Mapping]] = ..., listing: _Optional[_Union[OpResult.Listing, _Mapping]] = ..., deleted: _Optional[_Union[Empty, _Mapping]] = ..., failed: _Optional[_Union[StoreFailure, _Mapping]] = ..., written: _Optional[_Union[OpResult.Written, _Mapping]] = ..., mismatch: _Optional[_Union[Empty, _Mapping]] = ..., attempts: _Optional[int] = ...) -> None: ...

class StoreFailure(_message.Message):
    __slots__ = ["message"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...

class StoreOp(_message.Message):
    __slots__ = ["cas", "create", "delete", "get", "list"]
    class Cas(_message.Message):
        __slots__ = ["bytes", "etag", "expiry_days", "key"]
        BYTES_FIELD_NUMBER: _ClassVar[int]
        ETAG_FIELD_NUMBER: _ClassVar[int]
        EXPIRY_DAYS_FIELD_NUMBER: _ClassVar[int]
        KEY_FIELD_NUMBER: _ClassVar[int]
        bytes: bytes
        etag: str
        expiry_days: int
        key: str
        def __init__(self, key: _Optional[str] = ..., bytes: _Optional[bytes] = ..., etag: _Optional[str] = ..., expiry_days: _Optional[int] = ...) -> None: ...
    class Create(_message.Message):
        __slots__ = ["bytes", "expiry_days", "key"]
        BYTES_FIELD_NUMBER: _ClassVar[int]
        EXPIRY_DAYS_FIELD_NUMBER: _ClassVar[int]
        KEY_FIELD_NUMBER: _ClassVar[int]
        bytes: bytes
        expiry_days: int
        key: str
        def __init__(self, key: _Optional[str] = ..., bytes: _Optional[bytes] = ..., expiry_days: _Optional[int] = ...) -> None: ...
    class List(_message.Message):
        __slots__ = ["max_keys", "prefix", "start_after", "wait"]
        MAX_KEYS_FIELD_NUMBER: _ClassVar[int]
        PREFIX_FIELD_NUMBER: _ClassVar[int]
        START_AFTER_FIELD_NUMBER: _ClassVar[int]
        WAIT_FIELD_NUMBER: _ClassVar[int]
        max_keys: int
        prefix: str
        start_after: str
        wait: ListWait
        def __init__(self, prefix: _Optional[str] = ..., start_after: _Optional[str] = ..., max_keys: _Optional[int] = ..., wait: _Optional[_Union[ListWait, _Mapping]] = ...) -> None: ...
    CAS_FIELD_NUMBER: _ClassVar[int]
    CREATE_FIELD_NUMBER: _ClassVar[int]
    DELETE_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    LIST_FIELD_NUMBER: _ClassVar[int]
    cas: StoreOp.Cas
    create: StoreOp.Create
    delete: str
    get: str
    list: StoreOp.List
    def __init__(self, create: _Optional[_Union[StoreOp.Create, _Mapping]] = ..., get: _Optional[str] = ..., list: _Optional[_Union[StoreOp.List, _Mapping]] = ..., delete: _Optional[str] = ..., cas: _Optional[_Union[StoreOp.Cas, _Mapping]] = ...) -> None: ...

class MachineKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class ErrorKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
