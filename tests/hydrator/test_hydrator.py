"""The hydrator itself (spec §4.1).

Per message: parse metadata, GET audio by id, PUT those bytes to a
deterministic key, publish the reference, commit. The Audio API serves
canonical FLAC, so nothing here touches the codec -- what it fetched is what it
stores, and the reference is read out of the same bytes.

The ordering in that list is load-bearing and is what most of this file tests.
"""

import pytest

from faas_hydrator.testing import flac_bytes, source_record
from faas_sdk.codec import JsonCodec
from faas_sdk.errors import PoisonMessageError
from faas_sdk.models import Status


def _reference(outcome, codec=None):
    return (codec or JsonCodec()).decode_reference(outcome.payload)


def test_publishes_a_reference_describing_what_it_stored(hydrator, object_store, source_audio):
    outcome = hydrator.process(source_record(call_id="c1"), _audio(source_audio))
    ref = _reference(outcome)

    assert ref.call_id == "c1"
    assert ref.object_key == "c1.flac"
    assert ref.sample_rate == 16000
    assert ref.channels == 1
    assert ref.duration_seconds == 300.0
    assert object_store.objects["c1.flac"] == source_audio


def test_the_bytes_are_stored_exactly_as_they_came_off_the_audio_api(
    hydrator, object_store, source_audio
):
    """The whole point of the simplification: no re-encode, so nothing can
    change the samples between the API and the object store."""
    hydrator.process(source_record(call_id="c1"), _audio(source_audio))

    assert object_store.objects["c1.flac"] is source_audio


def test_the_reference_describes_the_bytes_not_what_canonical_should_be(hydrator, object_store):
    """Read from STREAMINFO, so a reference can never claim a duration the
    object does not have."""
    audio = flac_bytes(total_samples=16000 * 42)
    outcome = hydrator.process(source_record(call_id="c1"), _audio(audio))

    assert _reference(outcome).duration_seconds == 42.0


def test_object_key_is_deterministic(hydrator, object_store, source_audio):
    """Spec §4.1. Reprocessing after a crash overwrites identically, which is
    what makes at-least-once delivery harmless here."""
    hydrator.process(source_record(call_id="c1"), _audio(source_audio))
    hydrator.process(source_record(call_id="c1"), _audio(source_audio))

    assert list(object_store.objects) == ["c1.flac"]


def test_storing_the_object_is_part_of_processing_not_of_publishing(hydrator, object_store):
    """The PUT happens inside process(), which is what makes the ordering
    structural: the emitter only ever publishes a reference the runner got back
    from a completed process() call, so the object is always in place first.

    A reference published ahead of its object would be a dead key for any
    function fast enough to read it -- a race surfacing as unexplained
    transient failures scattered across unrelated functions. The failing half
    of this invariant is in test_pipeline.py."""
    outcome = hydrator.process(source_record(call_id="c1"), _audio())

    assert "c1.flac" in object_store.objects
    assert outcome.payload


def test_audio_that_is_not_flac_is_poison_and_is_never_stored(hydrator, object_store):
    """Bytes the Audio API already returned will be the same bytes on the third
    attempt, so retrying only delays the DLQ and burns quota."""
    with pytest.raises(PoisonMessageError) as excinfo:
        hydrator.process(source_record(call_id="c1"), _audio(b"<!doctype html>502"))

    assert excinfo.value.retryable is False
    assert object_store.objects == {}


def test_flac_that_is_not_canonical_is_rejected(hydrator, object_store):
    """Encoding is upstream's job now. This is the one place a regression there
    is caught before it becomes every function's problem at once."""
    with pytest.raises(PoisonMessageError, match="canonical"):
        hydrator.process(
            source_record(call_id="c1"), _audio(flac_bytes(sample_rate=44100, channels=2))
        )

    assert object_store.objects == {}


def test_flac_with_an_unknown_duration_is_rejected(hydrator, object_store):
    """Rather than publishing a reference that claims the call is 0 seconds
    long -- which every downstream throughput figure would then believe."""
    with pytest.raises(PoisonMessageError, match="duration"):
        hydrator.process(source_record(call_id="c1"), _audio(flac_bytes(total_samples=0)))

    assert object_store.objects == {}


def test_nothing_is_stored_when_the_audio_cannot_be_fetched(hydrator, object_store):
    handle = _audio()
    handle.raises = PoisonMessageError("410 gone", code="AUDIO_GONE")

    with pytest.raises(PoisonMessageError):
        hydrator.process(source_record(call_id="c1"), handle)

    assert object_store.objects == {}


def test_source_metadata_rides_along_opaquely(hydrator):
    """Spec §4.2: opaque passthrough, so this schema never becomes a union of
    every upstream metadata format."""
    raw = b'{"tenant": "acme", "unknown_field": 1}'
    outcome = hydrator.process(source_record(call_id="c1", source_metadata=raw), _audio())

    assert _reference(outcome).source_metadata == raw


def test_metadata_is_not_written_into_the_object(hydrator, object_store):
    """Spec §4.1: one source of truth. The bytes in S3 carry no tenant, no call
    id, nothing that would have to be rewritten when the schema moves -- and
    now that the hydrator only copies, that is a property of what the Audio API
    serves rather than of a flag we pass an encoder."""
    raw = b'{"tenant": "acme"}'
    hydrator.process(source_record(call_id="c1", source_metadata=raw), _audio())

    assert b"acme" not in object_store.objects["c1.flac"]


def test_ingested_at_comes_from_the_source_not_from_now(hydrator, clock):
    """§4.2 says ingested_at is from the source metadata. Overwriting it with
    hydration time would silently reset every end-to-end latency measurement."""
    record = source_record(call_id="c1")
    clock.advance(3600)  # the call sat on the input topic for an hour
    outcome = hydrator.process(record, _audio())
    ref = _reference(outcome)

    assert ref.ingested_at == record.ingested_at
    assert ref.hydrated_at == clock.now()
    assert ref.hydrated_at > ref.ingested_at


def test_the_audio_is_fetched_once(hydrator):
    """Read-once semantics are the point of the claim check (§3)."""
    handle = _audio()
    hydrator.process(source_record(call_id="c1"), handle)

    assert handle.fetches == 1


def test_result_is_a_success_with_the_reference_as_payload(hydrator):
    outcome = hydrator.process(source_record(call_id="c1"), _audio())

    assert outcome.status is Status.SUCCESS
    assert outcome.payload


def _audio(raw: bytes | None = None):
    from faas_hydrator.testing import FakeSourceAudio

    return FakeSourceAudio(raw)
