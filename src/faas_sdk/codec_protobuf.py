"""Protobuf implementation of the `Codec` seam (spec §13 step 2).

Swapping this in for `JsonCodec` is the only change needed to put the real wire
format on the topics -- the runner, the ledger, the DLQ and the result emitter
never learn about it.

Two things protobuf gets wrong by default and that this adapter fixes:

  - **No null.** An unset Timestamp deserialises as the epoch, so "never
    hydrated" and "hydrated 1970-01-01" become indistinguishable. Presence
    checks (`HasField`) restore the distinction.
  - **Silent tolerance.** `ParseFromString` on garbage raises a protobuf-native
    exception, which `pool.classify` would treat as retryable -- so a
    permanently malformed message would burn its whole retry budget before
    reaching the DLQ. Everything is normalised to `DecodeError`, which the
    runner treats as poison on arrival.
"""

from __future__ import annotations

from datetime import datetime, timezone

from faas.v1 import audio_reference_pb2, result_pb2

from .codec import DecodeError
from .models import ENVELOPE_VERSION, AudioReference, ErrorInfo, Result, Status


class ProtobufCodec:
    def decode_reference(self, raw: bytes) -> AudioReference:
        message = audio_reference_pb2.AudioReference()
        try:
            message.ParseFromString(raw)
        except Exception as exc:  # noqa: BLE001 - protobuf's error types vary by runtime
            raise DecodeError(f"not a valid AudioReference: {exc}") from exc

        # A zero-length or wrong-schema payload can parse cleanly into an empty
        # message. Without a call_id there is nothing to key a result on, so
        # this is poison, not an empty-but-valid reference.
        if not message.call_id:
            raise DecodeError("AudioReference has no call_id")

        return AudioReference(
            envelope_version=message.envelope_version or ENVELOPE_VERSION,
            call_id=message.call_id,
            object_key=message.object_key,
            sample_rate=message.sample_rate,
            channels=message.channels,
            duration_seconds=message.duration_seconds,
            ingested_at=_to_datetime(message, "ingested_at"),
            hydrated_at=_to_datetime(message, "hydrated_at"),
            source_metadata=message.source_metadata,
        )

    def encode_reference(self, ref: AudioReference) -> bytes:
        message = audio_reference_pb2.AudioReference(
            envelope_version=ref.envelope_version,
            call_id=ref.call_id,
            object_key=ref.object_key,
            sample_rate=ref.sample_rate,
            channels=ref.channels,
            duration_seconds=ref.duration_seconds,
            source_metadata=ref.source_metadata,
        )
        _set_datetime(message, "ingested_at", ref.ingested_at)
        _set_datetime(message, "hydrated_at", ref.hydrated_at)
        return message.SerializeToString()

    def encode_result(self, result: Result) -> bytes:
        if result.payload is not None and result.payload_ref is not None:
            # payload_body is a oneof: setting both means one is silently
            # dropped on the wire. Fail here rather than downstream.
            raise ValueError(
                "payload and payload_ref are mutually exclusive; the claim check "
                "sets exactly one"
            )
        if result.status is Status.STATUS_UNSPECIFIED:
            raise ValueError(
                "refusing to encode an unspecified status: a record without a "
                "status reads as nothing, and a consumer cannot tell it apart "
                "from a producer bug"
            )

        message = result_pb2.Result(
            envelope_version=result.envelope_version,
            call_id=result.call_id,
            function_id=result.function_id,
            function_version=result.function_version,
            status=int(result.status),
            payload_schema_version=result.payload_schema_version,
            payload_content_type=result.payload_content_type,
            input_object_key=result.input_object_key,
            input_offset=result.input_offset,
            attempt=result.attempt,
        )
        if result.payload is not None:
            message.payload = result.payload
        elif result.payload_ref is not None:
            message.payload_ref = result.payload_ref

        if result.error is not None:
            message.error.code = result.error.code
            message.error.message = result.error.message
            message.error.retryable = result.error.retryable

        _set_datetime(message, "ingested_at", result.ingested_at)
        _set_datetime(message, "started_at", result.started_at)
        _set_datetime(message, "completed_at", result.completed_at)
        return message.SerializeToString()

    def decode_result(self, raw: bytes) -> Result:
        message = result_pb2.Result()
        try:
            message.ParseFromString(raw)
        except Exception as exc:  # noqa: BLE001
            raise DecodeError(f"not a valid Result: {exc}") from exc

        which = message.WhichOneof("payload_body")
        status = Status(message.status)
        if status is Status.STATUS_UNSPECIFIED:
            # The producer never set it. Proto3 has no presence for enums, so
            # this is exactly the partial write or mis-built message that
            # STATUS_UNSPECIFIED exists to surface -- poison, not a status.
            raise DecodeError(
                "Result has no status set (STATUS_UNSPECIFIED); it was written "
                "by something that does not know the schema"
            )
        return Result(
            envelope_version=message.envelope_version or ENVELOPE_VERSION,
            call_id=message.call_id,
            function_id=message.function_id,
            function_version=message.function_version,
            status=status,
            error=(
                ErrorInfo(
                    code=message.error.code,
                    message=message.error.message,
                    retryable=message.error.retryable,
                )
                if message.HasField("error")
                else None
            ),
            payload_schema_version=message.payload_schema_version,
            payload_content_type=message.payload_content_type,
            payload=message.payload if which == "payload" else None,
            payload_ref=message.payload_ref if which == "payload_ref" else None,
            input_object_key=message.input_object_key,
            input_offset=message.input_offset,
            attempt=message.attempt,
            ingested_at=_to_datetime(message, "ingested_at"),
            started_at=_to_datetime(message, "started_at"),
            completed_at=_to_datetime(message, "completed_at"),
        )


def _to_datetime(message, field: str):
    if not message.HasField(field):
        return None
    return getattr(message, field).ToDatetime(tzinfo=timezone.utc)


def _set_datetime(message, field: str, value: datetime | None) -> None:
    if value is None:
        return
    getattr(message, field).FromDatetime(value)
