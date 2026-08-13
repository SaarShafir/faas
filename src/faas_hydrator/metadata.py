"""Parsing the input topic (spec §4.1 step 1).

The input schema belongs to whoever writes that topic and the spec does not
define it, so field names are constructor arguments rather than constants.

Everything unusable raises `DecodeError`, which the runner already treats as
poison on arrival: DLQ, commit, no retries. That mapping is the important part.
A parse failure that surfaced as anything else would burn the retry budget on a
message that cannot improve, and three attempts per bad record at 17 files/sec
is how a malformed producer turns into pipeline-wide lag.
"""

from __future__ import annotations

import json
from datetime import datetime

from faas_sdk.codec import DecodeError

from .models import SourceRecord


class JsonSourceDecoder:
    def __init__(
        self,
        call_id_field: str = "call_id",
        audio_id_field: str = "audio_id",
        ingested_at_field: str = "ingested_at",
    ):
        self.call_id_field = call_id_field
        self.audio_id_field = audio_id_field
        self.ingested_at_field = ingested_at_field

    def decode(self, raw: bytes) -> SourceRecord:
        try:
            data = json.loads(raw)
        except (ValueError, TypeError) as exc:
            raise DecodeError(f"source metadata is not valid JSON: {exc}") from exc

        if not isinstance(data, dict):
            raise DecodeError("source metadata must be a JSON object")

        call_id = data.get(self.call_id_field)
        audio_id = data.get(self.audio_id_field)
        if not call_id or not isinstance(call_id, str):
            raise DecodeError(f"source metadata has no usable {self.call_id_field}")
        if not audio_id or not isinstance(audio_id, str):
            raise DecodeError(f"source metadata has no usable {self.audio_id_field}")

        return SourceRecord(
            call_id=call_id,
            audio_id=audio_id,
            ingested_at=self._timestamp(data.get(self.ingested_at_field)),
            # The original bytes, not a re-serialisation: §4.2's passthrough has
            # to survive fields we do not model.
            source_metadata=raw,
        )

    def _timestamp(self, value):
        if value is None:
            return None
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError) as exc:
            raise DecodeError(f"{self.ingested_at_field} is not an ISO-8601 timestamp") from exc
