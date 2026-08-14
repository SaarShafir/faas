"""ProtobufCodec (spec §13 step 2).

The point of the codec seam was that swapping JSON for protobuf touches one
adapter and nothing else. These tests hold that: the same round-trips as the
JSON codec, plus the things only protobuf gets wrong -- oneof handling, unset
timestamps decoding as epoch, and enum numbering drifting from the spec.
"""

from datetime import datetime, timedelta, timezone

import pytest

from faas_sdk.codec import DecodeError
from faas_sdk.models import AudioReference, ErrorInfo, Result, Status

pytest.importorskip("google.protobuf", reason="protobuf runtime not installed")

from faas_sdk.codec_protobuf import ProtobufCodec  # noqa: E402

EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def codec():
    return ProtobufCodec()


def _reference(**overrides) -> AudioReference:
    fields = dict(
        call_id="call-1",
        object_key="call-1.flac",
        sample_rate=16000,
        channels=1,
        duration_seconds=302.5,
        ingested_at=EPOCH,
        hydrated_at=EPOCH + timedelta(seconds=30),
        source_metadata=b"\x00\x01opaque",
    )
    fields.update(overrides)
    return AudioReference(**fields)


def _result(**overrides) -> Result:
    fields = dict(
        call_id="call-1",
        function_id="duration_rms",
        function_version="1.0.0",
        status=Status.SUCCESS,
        input_object_key="call-1.flac",
        input_offset=42,
        attempt=1,
        payload=b'{"rms": 0.12}',
        payload_schema_version="1",
        payload_content_type="application/json",
        ingested_at=EPOCH,
        started_at=EPOCH + timedelta(seconds=1),
        completed_at=EPOCH + timedelta(seconds=3),
    )
    fields.update(overrides)
    return Result(**fields)


# -- AudioReference --------------------------------------------------------


def test_reference_round_trips(codec):
    ref = _reference()
    assert codec.decode_reference(codec.encode_reference(ref)) == ref


def test_reference_preserves_opaque_metadata_bytes(codec):
    """Passthrough must survive intact -- it is someone else's schema."""
    raw = bytes(range(256))
    ref = _reference(source_metadata=raw)
    assert codec.decode_reference(codec.encode_reference(ref)).source_metadata == raw


def test_unset_timestamps_decode_as_none_not_epoch(codec):
    """Protobuf has no null. Without presence checks, "never hydrated" and
    "hydrated at 1970-01-01" become the same value."""
    ref = _reference(ingested_at=None, hydrated_at=None)
    decoded = codec.decode_reference(codec.encode_reference(ref))
    assert decoded.ingested_at is None
    assert decoded.hydrated_at is None


def test_sub_second_timestamp_precision_survives(codec):
    precise = EPOCH + timedelta(seconds=1, microseconds=123456)
    ref = _reference(ingested_at=precise)
    assert codec.decode_reference(codec.encode_reference(ref)).ingested_at == precise


def test_garbage_bytes_raise_decode_error(codec):
    # DecodeError is what the runner treats as poison, so this must not leak a
    # raw protobuf exception -- it would be classified as retryable and the
    # message would be retried three times before reaching the DLQ.
    with pytest.raises(DecodeError):
        codec.decode_reference(b"\xff\xff\xff\xff not a protobuf")


def test_truncated_message_raises_decode_error(codec):
    encoded = codec.encode_reference(_reference())
    with pytest.raises(DecodeError):
        codec.decode_reference(encoded[:-3])


def test_json_bytes_are_rejected_as_poison(codec):
    """The likeliest real incident: a producer that never got the memo."""
    with pytest.raises(DecodeError):
        codec.decode_reference(b'{"call_id": "call-1", "object_key": "call-1.flac"}')


# -- Result ----------------------------------------------------------------


def test_result_round_trips(codec):
    result = _result()
    assert codec.decode_result(codec.encode_result(result)) == result


def test_inline_payload_round_trips(codec):
    decoded = codec.decode_result(codec.encode_result(_result(payload=b"\x00\xff\x00")))
    assert decoded.payload == b"\x00\xff\x00"
    assert decoded.payload_ref is None


def test_payload_ref_round_trips(codec):
    result = _result(payload=None, payload_ref="results/duration_rms/1.0.0/call-1")
    decoded = codec.decode_result(codec.encode_result(result))
    assert decoded.payload_ref == "results/duration_rms/1.0.0/call-1"
    assert decoded.payload is None


def test_oneof_means_a_claim_checked_result_carries_no_inline_payload(codec):
    """Setting both would silently drop one on the wire; the SDK must not."""
    with pytest.raises(ValueError, match="payload"):
        codec.encode_result(_result(payload=b"inline", payload_ref="s3/key"))


def test_failed_result_round_trips_its_error(codec):
    result = _result(
        status=Status.FAILED,
        payload=None,
        error=ErrorInfo(code="DECODE_ERROR", message="not FLAC", retryable=False),
    )
    decoded = codec.decode_result(codec.encode_result(result))
    assert decoded.status is Status.FAILED
    assert decoded.error == ErrorInfo("DECODE_ERROR", "not FLAC", retryable=False)
    assert decoded.payload is None


def test_absent_error_decodes_as_none_not_an_empty_error(codec):
    """An empty Error with code "" would read as a real error downstream."""
    decoded = codec.decode_result(codec.encode_result(_result()))
    assert decoded.error is None


def test_skipped_result_round_trips(codec):
    decoded = codec.decode_result(codec.encode_result(_result(status=Status.SKIPPED, payload=None)))
    assert decoded.status is Status.SKIPPED


# -- the reserved zero value -----------------------------------------------
#
# proto3 gives enums no presence, so whatever sits at 0 is what an unset,
# truncated or half-written status decodes as. These tests are the reason
# SUCCESS is 1 and not §6's 0.


def test_a_result_with_no_status_does_not_decode_as_a_success(codec):
    """The failure this whole change exists to prevent: a producer that never
    sets `status` must not have its payload trusted downstream."""
    from faas.v1 import result_pb2

    message = result_pb2.Result(call_id="call-1", function_id="duration_rms")
    assert not message.status  # unset, i.e. the zero value

    with pytest.raises(DecodeError, match="status"):
        codec.decode_result(message.SerializeToString())


def test_a_status_this_build_does_not_know_is_poison(codec):
    """proto3 enums are open: a newer producer's state arrives as a bare int.
    Reading it as anything -- least of all SUCCESS -- would be a guess."""
    from faas.v1 import result_pb2

    message = result_pb2.Result(call_id="call-1")
    message.status = 99

    with pytest.raises(DecodeError, match="99"):
        codec.decode_result(message.SerializeToString())


def test_the_sdk_refuses_to_emit_an_unspecified_status(codec):
    """The zero value is a tripwire for messages nobody meant to write.
    Deliberately writing one would disarm it."""
    with pytest.raises(ValueError, match="UNSPECIFIED"):
        codec.encode_result(_result(status=Status.UNSPECIFIED))


def test_no_named_state_sits_at_the_zero_value():
    """Pins the deviation from §6 itself, so restoring SUCCESS = 0 fails here
    rather than quietly on a topic."""
    from faas.v1 import result_pb2

    assert Status.UNSPECIFIED == 0
    assert result_pb2.Result.Status.STATUS_UNSPECIFIED == 0
    assert Status.SUCCESS != 0


def test_large_offsets_survive(codec):
    """input_offset is int64 in the schema; a 48h topic will exceed int32."""
    big = 2**40
    assert codec.decode_result(codec.encode_result(_result(input_offset=big))).input_offset == big


# -- schema agreement ------------------------------------------------------


def test_status_numbering_matches_the_python_enum():
    """models.Status and the wire enum must not drift: they are the same
    contract expressed twice, and a mismatch silently relabels every result."""
    from faas.v1 import result_pb2

    assert result_pb2.Result.Status.STATUS_UNSPECIFIED == Status.UNSPECIFIED
    assert result_pb2.Result.Status.SUCCESS == Status.SUCCESS
    assert result_pb2.Result.Status.FAILED == Status.FAILED
    assert result_pb2.Result.Status.SKIPPED == Status.SKIPPED


def test_field_numbers_match_the_spec():
    """Field numbers are the wire format. Once published they are frozen (§10),
    so this pins them against an accidental reorder."""
    from faas.v1 import result_pb2

    fields = {f.name: f.number for f in result_pb2.Result.DESCRIPTOR.fields}
    assert fields == {
        "envelope_version": 1,
        "call_id": 2,
        "function_id": 3,
        "function_version": 4,
        "status": 5,
        "error": 6,
        "payload_schema_version": 7,
        "payload_content_type": 8,
        "payload": 9,
        "payload_ref": 10,
        "input_object_key": 11,
        "input_offset": 12,
        "attempt": 13,
        "ingested_at": 14,
        "started_at": 15,
        "completed_at": 16,
    }


def test_reference_field_numbers_match_the_spec():
    from faas.v1 import audio_reference_pb2

    fields = {f.name: f.number for f in audio_reference_pb2.AudioReference.DESCRIPTOR.fields}
    assert fields == {
        "envelope_version": 1,
        "call_id": 2,
        "object_key": 3,
        "sample_rate": 4,
        "channels": 5,
        "duration_seconds": 6,
        "ingested_at": 7,
        "hydrated_at": 8,
        "source_metadata": 9,
    }


def test_a_newer_producers_unknown_fields_do_not_break_an_older_function(codec):
    """Forward compatibility: a v2 hydrator that adds a field must not break a
    v1 function still running against the live topic.

    Note what this does *not* claim: decoding lands in a dataclass, so unknown
    fields are dropped there and a decode/re-encode cycle would lose them. That
    is fine for the SDK -- the runner only ever decodes references and only ever
    encodes results it built itself -- but anything that proxies a message
    through the SDK must work on the protobuf object, not the dataclass.
    """
    from faas.v1 import audio_reference_pb2

    message = audio_reference_pb2.AudioReference()
    message.ParseFromString(codec.encode_reference(_reference()))
    # A varint field that does not exist in this build of the schema.
    raw = message.SerializeToString() + b"\xd8\x06\x2a"

    decoded = codec.decode_reference(raw)
    assert decoded.call_id == "call-1"
    assert decoded.duration_seconds == 302.5
