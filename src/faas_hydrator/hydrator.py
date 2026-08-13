"""The hydrator (spec §4.1).

    1. parse metadata, extract audio id     <- metadata.JsonSourceDecoder
    2. GET audio by id                      <- audioapi.SourceAudioHandle
    3. transcode to canonical FLAC          <- transcode.Transcoder
    4. PUT to {call_id}.flac
    5. publish the reference                <- emitter.ReferenceEmitter
    6. commit                               <- the SDK's ledger

Steps 4 and 5 are in that order and cannot be swapped: a reference published
ahead of its object is a dead key for any function fast enough to read it,
which would surface as unexplained transient failures scattered across
unrelated functions. Step 6 follows 5 because the SDK commits only on
completion -- so a crash between them replays the call, and the deterministic
key makes that harmless.

Stateless and dumb, as the spec asks. At 17 files/sec it will never be the
bottleneck, and anything clever here is risk without reward.
"""

from __future__ import annotations

from faas_sdk.models import AudioReference, FunctionResult

from .models import SourceRecord

REFERENCE_CONTENT_TYPE = "application/x-protobuf"


class Hydrator:
    function_id = "hydrator"
    function_version = "1.0.0"

    def __init__(self, *, transcoder, object_store, codec, clock=None):
        self.transcoder = transcoder
        self.object_store = object_store
        self.codec = codec
        self._clock = clock or _system_clock()

    def process(self, record: SourceRecord, audio) -> FunctionResult:
        raw = audio.bytes()
        flac = self.transcoder.to_canonical_flac(raw)

        self.object_store.put(record.object_key, flac.data, content_type="audio/flac")

        reference = AudioReference(
            call_id=record.call_id,
            object_key=record.object_key,
            sample_rate=flac.sample_rate,
            channels=flac.channels,
            duration_seconds=flac.duration_seconds,
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


def _system_clock():
    from faas_sdk.clock import SystemClock

    return SystemClock()
