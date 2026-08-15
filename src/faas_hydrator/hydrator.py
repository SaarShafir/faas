"""The hydrator (spec §4.1).

    1. parse metadata, extract audio id     <- metadata.JsonSourceDecoder
    2. GET audio by id                      <- audioapi.SourceAudioHandle
    3. PUT those bytes to {call_id}.flac
    4. publish the reference                <- emitter.ReferenceEmitter
    5. commit                               <- the SDK's ledger

The Audio API serves canonical FLAC -- the encoding is settled upstream -- so
there is no transcode step. The bytes that arrive are the bytes that are
stored, and the reference is filled in from their STREAMINFO header.

Steps 3 and 4 are in that order and cannot be swapped: a reference published
ahead of its object is a dead key for any function fast enough to read it,
which would surface as unexplained transient failures scattered across
unrelated functions. Step 5 follows 4 because the SDK commits only on
completion -- so a crash between them replays the call, and the deterministic
key makes that harmless.

Stateless and dumb, as the spec asks. At 17 files/sec it will never be the
bottleneck, and anything clever here is risk without reward.
"""

from __future__ import annotations

from faas_sdk.errors import PoisonMessageError
from faas_sdk.models import AudioReference, FunctionResult

from .flac import (
    CANONICAL_BITS,
    CANONICAL_CHANNELS,
    CANONICAL_SAMPLE_RATE,
    MalformedFlacError,
    read_streaminfo,
)
from .models import SourceRecord

REFERENCE_CONTENT_TYPE = "application/x-protobuf"
AUDIO_CONTENT_TYPE = "audio/flac"


class Hydrator:
    function_id = "hydrator"
    function_version = "1.0.0"

    def __init__(self, *, object_store, codec, clock=None):
        self.object_store = object_store
        self.codec = codec
        self._clock = clock or _system_clock()

    def process(self, record: SourceRecord, audio) -> FunctionResult:
        data = audio.bytes()
        info = _describe(data)

        self.object_store.put(record.object_key, data, content_type=AUDIO_CONTENT_TYPE)

        reference = AudioReference(
            call_id=record.call_id,
            object_key=record.object_key,
            sample_rate=info.sample_rate,
            channels=info.channels,
            duration_seconds=info.duration_seconds,
            # From the source metadata, not from now: overwriting it would
            # silently reset every end-to-end latency measurement (§4.2).
            ingested_at=record.ingested_at,
            hydrated_at=self._clock.now(),
            source_metadata=record.source_metadata,
        )
        return FunctionResult(
            payload=self.codec.encode_reference(reference),
            content_type=REFERENCE_CONTENT_TYPE,
        )


def _describe(data: bytes):
    """What the reference will claim, read from the bytes rather than assumed.

    Every failure here is poison rather than retryable: the Audio API returned
    these bytes and will return the same bytes on the third attempt, so a retry
    only delays the DLQ and burns quota.
    """
    try:
        info = read_streaminfo(data)
    except MalformedFlacError as exc:
        raise PoisonMessageError(
            f"audio api did not return FLAC: {exc}", code="AUDIO_NOT_FLAC"
        ) from exc

    if not info.duration_known:
        raise PoisonMessageError(
            "FLAC header carries no duration; the reference would claim 0 seconds",
            code="AUDIO_NO_DURATION",
        )

    if (info.sample_rate, info.channels, info.bits_per_sample) != (
        CANONICAL_SAMPLE_RATE,
        CANONICAL_CHANNELS,
        CANONICAL_BITS,
    ):
        # The whole SDK assumes canonical form. Encoding is upstream's job now,
        # and this is the one place a regression there can be caught before it
        # reaches every function at once.
        raise PoisonMessageError(
            f"not canonical: got {info.sample_rate} Hz / {info.channels} ch / "
            f"{info.bits_per_sample} bit, want {CANONICAL_SAMPLE_RATE} / "
            f"{CANONICAL_CHANNELS} / {CANONICAL_BITS}",
            code="AUDIO_NOT_CANONICAL",
        )

    return info


def _system_clock():
    from faas_sdk.clock import SystemClock

    return SystemClock()
