import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Result(_message.Message):
    __slots__ = ("envelope_version", "call_id", "function_id", "function_version", "status", "error", "payload_schema_version", "payload_content_type", "payload", "payload_ref", "input_object_key", "input_offset", "attempt", "ingested_at", "started_at", "completed_at")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SUCCESS: _ClassVar[Result.Status]
        FAILED: _ClassVar[Result.Status]
        SKIPPED: _ClassVar[Result.Status]
    SUCCESS: Result.Status
    FAILED: Result.Status
    SKIPPED: Result.Status
    ENVELOPE_VERSION_FIELD_NUMBER: _ClassVar[int]
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_REF_FIELD_NUMBER: _ClassVar[int]
    INPUT_OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    INPUT_OFFSET_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    INGESTED_AT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    envelope_version: int
    call_id: str
    function_id: str
    function_version: str
    status: Result.Status
    error: Error
    payload_schema_version: str
    payload_content_type: str
    payload: bytes
    payload_ref: str
    input_object_key: str
    input_offset: int
    attempt: int
    ingested_at: _timestamp_pb2.Timestamp
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    def __init__(self, envelope_version: _Optional[int] = ..., call_id: _Optional[str] = ..., function_id: _Optional[str] = ..., function_version: _Optional[str] = ..., status: _Optional[_Union[Result.Status, str]] = ..., error: _Optional[_Union[Error, _Mapping]] = ..., payload_schema_version: _Optional[str] = ..., payload_content_type: _Optional[str] = ..., payload: _Optional[bytes] = ..., payload_ref: _Optional[str] = ..., input_object_key: _Optional[str] = ..., input_offset: _Optional[int] = ..., attempt: _Optional[int] = ..., ingested_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Error(_message.Message):
    __slots__ = ("code", "message", "retryable")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    retryable: bool
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ..., retryable: _Optional[bool] = ...) -> None: ...
