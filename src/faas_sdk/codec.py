"""Serialization seam.

JSON today because protobuf + Buf is build-order step 2 (spec §13). The runner
only ever calls `decode_reference` / `encode_result`, so a ProtobufCodec drops
in without touching anything else.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from typing import Protocol

from .models import ENVELOPE_VERSION, AudioReference, ErrorInfo, Result, Status


class Codec(Protocol):
    def decode_reference(self, raw: bytes) -> AudioReference: ...

    def encode_result(self, result: Result) -> bytes: ...

    def decode_result(self, raw: bytes) -> Result: ...


class DecodeError(ValueError):
    """Malformed envelope. Always poison -- retrying will not fix bad bytes."""


class JsonCodec:
    def decode_reference(self, raw: bytes) -> AudioReference:
        try:
            data = json.loads(raw)
        except (ValueError, TypeError) as exc:
            raise DecodeError(f"not valid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise DecodeError("reference must be an object")
        try:
            return AudioReference(
                envelope_version=int(data.get("envelope_version", ENVELOPE_VERSION)),
                call_id=data["call_id"],
                object_key=data["object_key"],
                sample_rate=int(data.get("sample_rate", 16000)),
                channels=int(data.get("channels", 1)),
                duration_seconds=float(data.get("duration_seconds", 0.0)),
                ingested_at=_parse_ts(data.get("ingested_at")),
                hydrated_at=_parse_ts(data.get("hydrated_at")),
                source_metadata=_b64d(data.get("source_metadata")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise DecodeError(f"malformed AudioReference: {exc}") from exc

    def encode_reference(self, ref: AudioReference) -> bytes:
        return json.dumps(
            {
                "envelope_version": ref.envelope_version,
                "call_id": ref.call_id,
                "object_key": ref.object_key,
                "sample_rate": ref.sample_rate,
                "channels": ref.channels,
                "duration_seconds": ref.duration_seconds,
                "ingested_at": _fmt_ts(ref.ingested_at),
                "hydrated_at": _fmt_ts(ref.hydrated_at),
                "source_metadata": _b64e(ref.source_metadata),
            }
        ).encode()

    def encode_result(self, result: Result) -> bytes:
        return json.dumps(
            {
                "envelope_version": result.envelope_version,
                "call_id": result.call_id,
                "function_id": result.function_id,
                "function_version": result.function_version,
                "status": int(result.status),
                "error": (
                    None
                    if result.error is None
                    else {
                        "code": result.error.code,
                        "message": result.error.message,
                        "retryable": result.error.retryable,
                    }
                ),
                "payload_schema_version": result.payload_schema_version,
                "payload_content_type": result.payload_content_type,
                "payload": _b64e(result.payload),
                "payload_ref": result.payload_ref,
                "input_object_key": result.input_object_key,
                "input_offset": result.input_offset,
                "attempt": result.attempt,
                "ingested_at": _fmt_ts(result.ingested_at),
                "started_at": _fmt_ts(result.started_at),
                "completed_at": _fmt_ts(result.completed_at),
            }
        ).encode()

    def decode_result(self, raw: bytes) -> Result:
        data = json.loads(raw)
        error = data.get("error")
        return Result(
            envelope_version=data["envelope_version"],
            call_id=data["call_id"],
            function_id=data["function_id"],
            function_version=data["function_version"],
            status=Status(data["status"]),
            error=(
                None
                if error is None
                else ErrorInfo(
                    code=error["code"],
                    message=error["message"],
                    retryable=error["retryable"],
                )
            ),
            payload_schema_version=data["payload_schema_version"],
            payload_content_type=data["payload_content_type"],
            payload=_b64d(data.get("payload")) or None,
            payload_ref=data.get("payload_ref"),
            input_object_key=data["input_object_key"],
            input_offset=data["input_offset"],
            attempt=data["attempt"],
            ingested_at=_parse_ts(data.get("ingested_at")),
            started_at=_parse_ts(data.get("started_at")),
            completed_at=_parse_ts(data.get("completed_at")),
        )


def _b64e(raw: bytes | None) -> str | None:
    return None if raw is None else base64.b64encode(raw).decode()


def _b64d(value) -> bytes:
    if not value:
        return b""
    return base64.b64decode(value)


def _fmt_ts(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(timezone.utc).isoformat()


def _parse_ts(value) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)
