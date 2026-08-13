"""Parsing the input topic's metadata (spec §4.1 step 1).

The input schema is upstream-owned and the spec does not define it, so the
field names are configuration rather than constants. What is not negotiable is
the failure mode: anything unparseable must raise DecodeError, which the runner
treats as poison on arrival -- straight to the DLQ, offset committed, no retry
budget burned and no lag accrued.
"""

import json
from datetime import datetime, timezone

import pytest

from faas_hydrator.metadata import JsonSourceDecoder
from faas_sdk.codec import DecodeError

EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def decoder():
    return JsonSourceDecoder()


def _raw(**overrides):
    payload = {
        "call_id": "c1",
        "audio_id": "audio-999",
        "ingested_at": EPOCH.isoformat(),
    }
    payload.update(overrides)
    return json.dumps(payload).encode()


def test_extracts_the_call_and_audio_ids(decoder):
    record = decoder.decode(_raw())

    assert record.call_id == "c1"
    assert record.audio_id == "audio-999"
    assert record.ingested_at == EPOCH


def test_object_key_is_derived_not_carried(decoder):
    """§4.1: the key is deterministic from call_id. Letting upstream supply it
    would give two sources of truth for where the audio lives."""
    assert decoder.decode(_raw()).object_key == "c1.flac"


def test_the_whole_original_message_is_kept_for_passthrough(decoder):
    """§4.2's source_metadata is the original bytes, not a re-serialisation --
    a round-trip through json would quietly drop fields we do not model."""
    raw = _raw(tenant="acme", nested={"a": [1, 2]})
    assert decoder.decode(raw).source_metadata == raw


def test_unknown_fields_are_tolerated(decoder):
    """Upstream will add fields without telling us."""
    assert decoder.decode(_raw(some_new_field="whatever")).call_id == "c1"


def test_a_missing_ingested_at_is_allowed(decoder):
    record = decoder.decode(json.dumps({"call_id": "c1", "audio_id": "a1"}).encode())
    assert record.ingested_at is None


@pytest.mark.parametrize(
    "raw,reason",
    [
        (b"{not json", "not json"),
        (b"[]", "not an object"),
        (b'{"audio_id": "a1"}', "no call_id"),
        (b'{"call_id": "c1"}', "no audio_id"),
        (b'{"call_id": "", "audio_id": "a1"}', "empty call_id"),
        (b'{"call_id": "c1", "audio_id": ""}', "empty audio_id"),
        (b'{"call_id": "c1", "audio_id": "a1", "ingested_at": "not a date"}', "bad timestamp"),
    ],
)
def test_unusable_metadata_is_poison(decoder, raw, reason):
    with pytest.raises(DecodeError):
        decoder.decode(raw)


def test_field_names_are_configurable():
    """Because the input schema belongs to whoever writes the input topic."""
    decoder = JsonSourceDecoder(call_id_field="conversationId", audio_id_field="recordingId")
    raw = json.dumps({"conversationId": "c1", "recordingId": "a1"}).encode()

    record = decoder.decode(raw)
    assert record.call_id == "c1"
    assert record.audio_id == "a1"
