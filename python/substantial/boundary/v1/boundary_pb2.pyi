from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MachineKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MACHINE_KIND_UNSPECIFIED: _ClassVar[MachineKind]
    MACHINE_KIND_RUN: _ClassVar[MachineKind]
    MACHINE_KIND_AGENT: _ClassVar[MachineKind]
    MACHINE_KIND_PLAN: _ClassVar[MachineKind]

class ErrorKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ERROR_KIND_UNSPECIFIED: _ClassVar[ErrorKind]
    ERROR_KIND_NON_DETERMINISM: _ClassVar[ErrorKind]
    ERROR_KIND_INVALID: _ClassVar[ErrorKind]
    ERROR_KIND_ENDED: _ClassVar[ErrorKind]
    ERROR_KIND_CORRUPT: _ClassVar[ErrorKind]
    ERROR_KIND_LAYOUT: _ClassVar[ErrorKind]
    ERROR_KIND_VALUE_TOO_LARGE: _ClassVar[ErrorKind]
MACHINE_KIND_UNSPECIFIED: MachineKind
MACHINE_KIND_RUN: MachineKind
MACHINE_KIND_AGENT: MachineKind
MACHINE_KIND_PLAN: MachineKind
ERROR_KIND_UNSPECIFIED: ErrorKind
ERROR_KIND_NON_DETERMINISM: ErrorKind
ERROR_KIND_INVALID: ErrorKind
ERROR_KIND_ENDED: ErrorKind
ERROR_KIND_CORRUPT: ErrorKind
ERROR_KIND_LAYOUT: ErrorKind
ERROR_KIND_VALUE_TOO_LARGE: ErrorKind

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CoreError(_message.Message):
    __slots__ = ("kind", "message")
    KIND_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    kind: ErrorKind
    message: str
    def __init__(self, kind: _Optional[_Union[ErrorKind, str]] = ..., message: _Optional[str] = ...) -> None: ...

class StoreOp(_message.Message):
    __slots__ = ("create", "get", "list", "delete")
    class Create(_message.Message):
        __slots__ = ("key", "bytes")
        KEY_FIELD_NUMBER: _ClassVar[int]
        BYTES_FIELD_NUMBER: _ClassVar[int]
        key: str
        bytes: bytes
        def __init__(self, key: _Optional[str] = ..., bytes: _Optional[bytes] = ...) -> None: ...
    class List(_message.Message):
        __slots__ = ("prefix", "start_after", "max_keys", "wait")
        PREFIX_FIELD_NUMBER: _ClassVar[int]
        START_AFTER_FIELD_NUMBER: _ClassVar[int]
        MAX_KEYS_FIELD_NUMBER: _ClassVar[int]
        WAIT_FIELD_NUMBER: _ClassVar[int]
        prefix: str
        start_after: str
        max_keys: int
        wait: ListWait
        def __init__(self, prefix: _Optional[str] = ..., start_after: _Optional[str] = ..., max_keys: _Optional[int] = ..., wait: _Optional[_Union[ListWait, _Mapping]] = ...) -> None: ...
    CREATE_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    LIST_FIELD_NUMBER: _ClassVar[int]
    DELETE_FIELD_NUMBER: _ClassVar[int]
    create: StoreOp.Create
    get: str
    list: StoreOp.List
    delete: str
    def __init__(self, create: _Optional[_Union[StoreOp.Create, _Mapping]] = ..., get: _Optional[str] = ..., list: _Optional[_Union[StoreOp.List, _Mapping]] = ..., delete: _Optional[str] = ...) -> None: ...

class ListWait(_message.Message):
    __slots__ = ("etag", "seconds")
    ETAG_FIELD_NUMBER: _ClassVar[int]
    SECONDS_FIELD_NUMBER: _ClassVar[int]
    etag: str
    seconds: int
    def __init__(self, etag: _Optional[str] = ..., seconds: _Optional[int] = ...) -> None: ...

class StoreFailure(_message.Message):
    __slots__ = ("message",)
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...

class OpResult(_message.Message):
    __slots__ = ("created", "exists", "bytes", "missing", "listing", "deleted", "failed")
    class Listing(_message.Message):
        __slots__ = ("keys", "etag")
        KEYS_FIELD_NUMBER: _ClassVar[int]
        ETAG_FIELD_NUMBER: _ClassVar[int]
        keys: _containers.RepeatedScalarFieldContainer[str]
        etag: str
        def __init__(self, keys: _Optional[_Iterable[str]] = ..., etag: _Optional[str] = ...) -> None: ...
    CREATED_FIELD_NUMBER: _ClassVar[int]
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    BYTES_FIELD_NUMBER: _ClassVar[int]
    MISSING_FIELD_NUMBER: _ClassVar[int]
    LISTING_FIELD_NUMBER: _ClassVar[int]
    DELETED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    created: Empty
    exists: Empty
    bytes: bytes
    missing: Empty
    listing: OpResult.Listing
    deleted: Empty
    failed: StoreFailure
    def __init__(self, created: _Optional[_Union[Empty, _Mapping]] = ..., exists: _Optional[_Union[Empty, _Mapping]] = ..., bytes: _Optional[bytes] = ..., missing: _Optional[_Union[Empty, _Mapping]] = ..., listing: _Optional[_Union[OpResult.Listing, _Mapping]] = ..., deleted: _Optional[_Union[Empty, _Mapping]] = ..., failed: _Optional[_Union[StoreFailure, _Mapping]] = ...) -> None: ...
