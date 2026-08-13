"""The hydrator's input record.

Structurally this plays the same role `AudioReference` plays for a function: it
is what the runner carries through the pool and what the DLQ path keys a
failure on. It exposes `object_key` and `duration_seconds` for that reason.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SourceRecord:
    call_id: str
    audio_id: str
    ingested_at: datetime | None = None
    source_metadata: bytes = b""

    @property
    def object_key(self) -> str:
        """Spec §4.1: deterministic, so a reprocessed call overwrites its own
        object rather than creating a second one."""
        return f"{self.call_id}.flac"

    @property
    def duration_seconds(self) -> float:
        """Unknown until the transcode has run.

        The runner reads this to emit throughput-vs-realtime (§5.5); reporting
        zero means the hydrator simply does not publish that metric, which is
        right -- it is a §8 onboarding measure for functions, and the hydrator
        is not one.
        """
        return 0.0
