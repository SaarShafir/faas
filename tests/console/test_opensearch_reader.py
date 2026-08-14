"""The log-backed reader, against canned OpenSearch responses.

The documents below are shaped exactly as OpenSearch returned them from the
running stack, dotted keys and all, because the one bug this reader has had so
far lived entirely in that shape.
"""

from __future__ import annotations

import pytest

from faas_console.opensearch_reader import OpenSearchConsoleReader


class StubKafka:
    """The delegate. Broker state cannot come from logs -- consumer lag is the
    gap between a committed offset and a high water mark, which no pod emits."""

    def __init__(self, declarations=None):
        self.declarations = declarations or {}
        self.fleet_called = False

    def fleet(self):
        self.fleet_called = True
        return []

    def topics(self):
        return []

    def lint(self):
        return []


def _document(event_name, call_id="call-1", **attributes):
    """A record in the shape OpenSearch actually returns.

    Note `event.name`: flat, with a literal dot. OpenSearch expands dotted
    names into objects in the *mapping*, so queries use the path
    `attributes.event.name`, but `_source` comes back exactly as indexed.
    """
    return {
        "@timestamp": "2026-08-14T13:15:57.202099173Z",
        "body": event_name,
        "attributes": {
            "event.name": event_name,
            "call_id": call_id,
            **attributes,
        },
    }


@pytest.fixture
def reader():
    r = OpenSearchConsoleReader(
        "http://opensearch:9200", index="ss4o_logs-faas-local", kafka=StubKafka()
    )
    r.documents = []
    r._search = lambda body: r.documents
    return r


def test_the_flat_dotted_event_name_is_understood(reader):
    """The bug that shipped: reading only the nested form finds nothing, and it
    fails *silently* -- the query matches, 25 events come back, and every one of
    them parses as an unknown event type, so the trace renders as "this call
    does not exist"."""
    reader.documents = [
        _document("call.received", object_key="call-1.flac", duration_seconds=45.0),
        _document(
            "call.completed",
            function_id="duration_rms",
            function_version="1.0.0",
            status="SUCCESS",
            attempt=1,
            payload='{"rms": 0.1}',
        ),
    ]

    trace = reader.find_call("call-1")

    assert trace.hydrated
    assert [r.function_id for r in trace.results] == ["duration_rms"]


def test_the_nested_form_also_works(reader):
    """A different backend, or a different collector version, may normalise the
    key. Reading one form only is what caused the bug; reading both is the fix."""
    document = _document("call.completed", function_id="duration_rms", status="SUCCESS")
    del document["attributes"]["event.name"]
    document["attributes"]["event"] = {"name": "call.completed"}
    reader.documents = [document]

    assert len(reader.find_call("call-1").results) == 1


def test_the_payload_comes_back_from_the_log(reader):
    """Payloads are in the events on purpose, so a trace needs no second
    lookup. §6's claim check bounds the size."""
    reader.documents = [
        _document(
            "call.completed",
            function_id="duration_rms",
            function_version="1.0.0",
            status="SUCCESS",
            payload='{"rms": 0.12}',
        )
    ]

    (result,) = reader.find_call("call-1").results
    assert result.payload == b'{"rms": 0.12}'


def test_a_redelivered_call_shows_each_function_once(reader):
    """At-least-once means the same call can complete twice. The trace should
    show the function once, keyed like the §6 record key."""
    reader.documents = [
        _document("call.completed", function_id="duration_rms", function_version="1.0.0"),
        _document("call.completed", function_id="duration_rms", function_version="1.0.0"),
    ]

    assert len(reader.find_call("call-1").results) == 1


def test_dead_letters_and_completions_both_count_as_answered(reader):
    """A function that dead-lettered a call has answered it. Listing it as
    missing would send someone looking for a result that will never exist."""
    reader.kafka.declarations = {"duration_rms": None, "energy_vad": None, "hydrator": None}
    reader.documents = [
        _document("call.received", object_key="x.flac"),
        _document("call.completed", function_id="duration_rms", status="SUCCESS"),
        _document(
            "call.dead_lettered",
            function_id="energy_vad",
            error_code="DECODE_ERROR",
            dlq_topic="faas.dlq.energy_vad",
            source_topic="faas.audio.internal",
            source_partition=3,
            source_offset=9,
        ),
    ]

    trace = reader.find_call("call-1")

    assert trace.missing == []
    assert trace.complete
    (dead,) = trace.dead_letters
    assert (dead.source_topic, dead.source_partition, dead.source_offset) == (
        "faas.audio.internal",
        3,
        9,
    )


def test_an_unhydrated_call_blames_nobody(reader):
    """No reference means no function ever saw it, so none of them are missing."""
    reader.kafka.declarations = {"duration_rms": None, "energy_vad": None}
    reader.documents = []

    trace = reader.find_call("nope")

    assert not trace.hydrated
    assert trace.missing == []


def test_broker_questions_are_delegated(reader):
    """Lag cannot come from logs at any level of effort."""
    reader.fleet()
    assert reader.kafka.fleet_called


def test_an_unreachable_log_store_does_not_raise(reader):
    """The console is most needed when something is broken."""
    r = OpenSearchConsoleReader("http://127.0.0.1:1", index="none", kafka=StubKafka(), timeout=0.5)
    trace = r.find_call("call-1")
    assert trace.results == []
    assert not trace.hydrated
