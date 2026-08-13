import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AudioReference(_message.Message):
    __slots__ = ("envelope_version", "call_id", "object_key", "sample_rate", "channels", "duration_seconds", "ingested_at", "hydrated_at", "source_metadata")
    ENVELOPE_VERSION_FIELD_NUMBER: _ClassVar[int]
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_RATE_FIELD_NUMBER: _ClassVar[int]
    CHANNELS_FIELD_NUMBER: _ClassVar[int]
    DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    INGESTED_AT_FIELD_NUMBER: _ClassVar[int]
    HYDRATED_AT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_METADATA_FIELD_NUMBER: _ClassVar[int]
    envelope_version: int
    call_id: str
    object_key: str
    sample_rate: int
    channels: int
    duration_seconds: float
    ingested_at: _timestamp_pb2.Timestamp
    hydrated_at: _timestamp_pb2.Timestamp
    source_metadata: bytes
    def __init__(self, envelope_version: _Optional[int] = ..., call_id: _Optional[str] = ..., object_key: _Optional[str] = ..., sample_rate: _Optional[int] = ..., channels: _Optional[int] = ..., duration_seconds: _Optional[float] = ..., ingested_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., hydrated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., source_metadata: _Optional[bytes] = ...) -> None: ...
