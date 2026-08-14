"""Wire and in-process types.

These are dataclasses, not generated protobuf, on purpose: protobuf + Buf is
step 2 of the build order (spec §13) and the SDK is step 1. Every field here
mirrors the spec's message definitions 1:1, and serialization goes through
`faas_sdk.codec.Codec`, so swapping in generated classes later is one adapter
rather than a rewrite of the runner.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime

ENVELOPE_VERSION = 1


class Status(enum.IntEnum):
    """Spec §6 Result.Status, with the zero value reserved.

    §6 numbers SUCCESS = 0. It is 1 here and on the wire so that an unset field
    -- proto3 gives enums no presence -- cannot decode as a success. Codecs
    treat UNSPECIFIED as poison rather than defaulting it; nothing in the SDK
    ever constructs it.
    """

    UNSPECIFIED = 0
    SUCCESS = 1
    FAILED = 2
    SKIPPED = 3


@dataclass(frozen=True)
class AudioReference:
    """Spec §4.2. The claim check: metadata plus a pointer, never the audio."""

    call_id: str
    object_key: str
    sample_rate: int = 16000
    channels: int = 1
    duration_seconds: float = 0.0
    ingested_at: datetime | None = None
    hydrated_at: datetime | None = None
    source_metadata: bytes = b""
    envelope_version: int = ENVELOPE_VERSION


@dataclass(frozen=True)
class ErrorInfo:
    """Spec §6 Error."""

    code: str
    message: str = ""
    retryable: bool = False


@dataclass(frozen=True)
class Result:
    """Spec §6 Result -- the stable envelope around an opaque payload.

    `payload` and `payload_ref` are the `oneof`: exactly one is set on SUCCESS,
    neither on FAILED or SKIPPED.
    """

    call_id: str
    function_id: str
    function_version: str
    status: Status
    input_object_key: str
    input_offset: int
    attempt: int
    envelope_version: int = ENVELOPE_VERSION
    error: ErrorInfo | None = None
    payload_schema_version: str = ""
    payload_content_type: str = ""
    payload: bytes | None = None
    payload_ref: str | None = None
    ingested_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def key(self) -> bytes:
        """Spec §6: composite, so log compaction cannot let one function's
        result destroy another's for the same call."""
        return f"{self.call_id}:{self.function_id}:{self.function_version}".encode()

    @property
    def partition_key(self) -> bytes:
        """Spec §6: partition on call_id alone so the aggregator needs no shuffle."""
        return self.call_id.encode()


@dataclass(frozen=True)
class FunctionResult:
    """What a function author returns (spec §5.1).

    Deliberately not `Result`: authors own the payload, the SDK owns the
    envelope. Returning `None` from `process` means SKIPPED.
    """

    payload: bytes = b""
    schema_version: str = ""
    content_type: str = "application/octet-stream"
    status: Status = Status.SUCCESS

    @classmethod
    def skip(cls) -> FunctionResult:
        return cls(status=Status.SKIPPED)


@dataclass(frozen=True)
class TopicPartition:
    topic: str
    partition: int


@dataclass(frozen=True)
class InboundMessage:
    """A record as read from the internal topic, decoupled from librdkafka."""

    topic: str
    partition: int
    offset: int
    value: bytes
    key: bytes | None = None
    timestamp_ms: int | None = None
    headers: tuple = ()

    @property
    def tp(self) -> TopicPartition:
        return TopicPartition(self.topic, self.partition)


@dataclass(frozen=True)
class Job:
    """One unit of work. Must stay picklable: it crosses a process boundary."""

    job_id: str
    ref: AudioReference
    message: InboundMessage
    attempt: int = 1

    @property
    def call_id(self) -> str:
        return self.ref.call_id


@dataclass(frozen=True)
class JobOutcome:
    """The picklable result of one attempt, as it comes back from a worker."""

    job: Job
    status: Status
    payload: bytes | None = None
    schema_version: str = ""
    content_type: str = ""
    error: ErrorInfo | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def succeeded(self) -> bool:
        return self.status is not Status.FAILED


@dataclass
class OutboundRecord:
    """What the SDK hands a producer: key and partition key are distinct (§6)."""

    topic: str
    key: bytes | None
    value: bytes
    partition_key: bytes | None = None
    headers: dict = field(default_factory=dict)
