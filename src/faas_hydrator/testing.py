"""Test doubles for the hydrator.

`flac_bytes` builds a FLAC header by hand, so the header-parsing path and
everything downstream of it are testable without an encoder installed. It is
also what the fake Audio API serves by default: canonical FLAC is what the real
one returns.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .flac import MAGIC, STREAMINFO_LENGTH

_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


def flac_bytes(
    sample_rate: int = 16000,
    channels: int = 1,
    bits: int = 16,
    total_samples: int = 16000 * 300,
    frames: bytes = b"\x00" * 32,
) -> bytes:
    """A FLAC stream with a real STREAMINFO block and placeholder frame data."""
    packed = (
        (sample_rate << 44)
        | ((channels - 1) << 41)
        | ((bits - 1) << 36)
        | (total_samples & ((1 << 36) - 1))
    )
    streaminfo = (
        (4096).to_bytes(2, "big")  # min block size
        + (4096).to_bytes(2, "big")  # max block size
        + (0).to_bytes(3, "big")  # min frame size
        + (0).to_bytes(3, "big")  # max frame size
        + packed.to_bytes(8, "big")
        + b"\x00" * 16  # md5
    )
    assert len(streaminfo) == STREAMINFO_LENGTH
    # 0x80 marks this as the last metadata block; type 0 is STREAMINFO.
    header = bytes([0x80]) + STREAMINFO_LENGTH.to_bytes(3, "big")
    return MAGIC + header + streaminfo + frames


class FakeSourceAudio:
    """Lazy handle over canned bytes, counting fetches to prove read-once."""

    def __init__(self, raw: bytes | None = None):
        self.raw = flac_bytes() if raw is None else raw
        self.fetches = 0
        self.raises: BaseException | None = None
        self._cached: bytes | None = None

    def bytes(self) -> bytes:
        if self._cached is None:
            if self.raises is not None:
                raise self.raises
            self.fetches += 1
            self._cached = self.raw
        return self._cached


class FakeAudioApi:
    def __init__(self, audio: dict | None = None, default: bytes | None = None):
        self.audio = dict(audio or {})
        self.default = flac_bytes() if default is None else default
        self.requested: list = []

    def get_audio(self, audio_id: str) -> bytes:
        self.requested.append(audio_id)
        return self.audio.get(audio_id, self.default)


def source_record(
    call_id: str = "c1",
    audio_id: str = "audio-1",
    ingested_at: datetime | None = _EPOCH,
    source_metadata: bytes = b"{}",
):
    from .models import SourceRecord

    return SourceRecord(
        call_id=call_id,
        audio_id=audio_id,
        ingested_at=ingested_at,
        source_metadata=source_metadata,
    )
