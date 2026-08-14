"""The console's reader, against a fake broker.

The property worth protecting is the cheap one: a call lookup must read the
*one* partition the call's results are on, not the topic. That is only true
because §6 partitions results on `call_id` alone so the aggregator needs no
shuffle -- the console gets an indexed lookup for free from a decision made for
an entirely different reason. If someone changes the partitioning, these tests
should be what notices.
"""

from __future__ import annotations

import json

import pytest

from faas_console.reader import INTERNAL_TOPIC, RESULTS_TOPIC, KafkaConsoleReader
from faas_sdk.codec import JsonCodec
from faas_sdk.models import AudioReference, ErrorInfo, Result, Status
from faas_sdk.partitioner import partition_for

RESULTS_PARTITIONS = 200
INTERNAL_PARTITIONS = 200


class FakeMessage:
    def __init__(self, value, headers=None, offset=0):
        self._value = value
        self._headers = list((headers or {}).items())
        self._offset = offset

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def offset(self):
        return self._offset

    def error(self):
        return None


class FakeConsumer:
    """Serves records for one assigned partition, bounded by its watermarks.

    Models the real reader's contract rather than a convenient one: the scan is
    driven by low/high water marks, not by a poll returning None, because a
    `None` poll means "nothing yet" and not "end of partition".

    Records the partitions it was asked for, which is what the cost assertions
    below are made against.
    """

    def __init__(self, broker):
        self.broker = broker
        self._queue = []

    def list_topics(self, timeout=None):
        return self.broker.metadata()

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        records = self.broker.records.get((tp.topic, tp.partition), [])
        return (0, len(records))

    def assign(self, partitions):
        tp = partitions[0]
        self.broker.reads.append((tp.topic, tp.partition))
        self._queue = list(self.broker.records.get((tp.topic, tp.partition), []))

    def poll(self, timeout=None):
        if self._queue:
            return self._queue.pop(0)
        return None

    def close(self):
        pass


class FakeBroker:
    def __init__(self):
        self.records: dict[tuple, list] = {}
        self.reads: list[tuple] = []
        self.topics = {
            RESULTS_TOPIC: RESULTS_PARTITIONS,
            INTERNAL_TOPIC: INTERNAL_PARTITIONS,
            "faas.calls.raw": 12,
            "faas.dlq.duration_rms": 1,
            "faas.dlq.hydrator": 1,
        }

    def metadata(self):
        class Topic:
            def __init__(self, n):
                self.partitions = dict.fromkeys(range(n))

        class Metadata:
            pass

        meta = Metadata()
        meta.topics = {name: Topic(n) for name, n in self.topics.items()}
        return meta

    def add(self, topic, partition, value, headers=None):
        bucket = self.records.setdefault((topic, partition), [])
        bucket.append(FakeMessage(value, headers, offset=len(bucket)))


@pytest.fixture
def broker():
    return FakeBroker()


@pytest.fixture
def reader(broker):
    return KafkaConsoleReader(
        "fake:9092",
        codec=JsonCodec(),
        consumer_factory=lambda *_a, **_k: FakeConsumer(broker),
        declarations={},
    )


def _seed_call(broker, call_id, functions=("duration_rms", "silence_ratio")):
    codec = JsonCodec()
    ref_partition = partition_for(call_id.encode(), INTERNAL_PARTITIONS)
    broker.add(
        INTERNAL_TOPIC,
        ref_partition,
        codec.encode_reference(
            AudioReference(call_id=call_id, object_key=f"{call_id}.flac", duration_seconds=42.0)
        ),
    )

    result_partition = partition_for(call_id.encode(), RESULTS_PARTITIONS)
    for function_id in functions:
        broker.add(
            RESULTS_TOPIC,
            result_partition,
            codec.encode_result(
                Result(
                    call_id=call_id,
                    function_id=function_id,
                    function_version="1.0.0",
                    status=Status.SUCCESS,
                    input_object_key=f"{call_id}.flac",
                    input_offset=1,
                    attempt=1,
                    payload=json.dumps(
                        {"duration_seconds": 42.0, "reference_duration_seconds": 42.0}
                    ).encode(),
                )
            ),
        )
    return ref_partition, result_partition


def test_a_call_lookup_reads_one_partition_of_each_topic(reader, broker):
    """The whole affordability argument. Two targeted reads, not 400."""
    ref_partition, result_partition = _seed_call(broker, "call-1")

    trace = reader.find_call("call-1")

    results_read = [p for t, p in broker.reads if t == RESULTS_TOPIC]
    internal_read = [p for t, p in broker.reads if t == INTERNAL_TOPIC]
    assert results_read == [result_partition]
    assert internal_read == [ref_partition]
    assert trace.hydrated


def test_the_partitions_it_read_are_reported(reader, broker):
    """Shown in the UI, so the cost of a lookup is visible rather than claimed."""
    _seed_call(broker, "call-1")
    trace = reader.find_call("call-1")
    assert any("of 200" in entry for entry in trace.partitions_scanned)
    assert len(trace.partitions_scanned) == 2


def test_results_for_other_calls_on_the_same_partition_are_filtered(reader, broker):
    """200 partitions and far more than 200 calls, so a partition holds many
    calls. Sharing one is normal and must not leak between traces."""
    partition = partition_for(b"call-1", RESULTS_PARTITIONS)
    _seed_call(broker, "call-1")

    codec = JsonCodec()
    broker.add(
        RESULTS_TOPIC,
        partition,
        codec.encode_result(
            Result(
                call_id="someone-else",
                function_id="duration_rms",
                function_version="1.0.0",
                status=Status.SUCCESS,
                input_object_key="x.flac",
                input_offset=2,
                attempt=1,
                payload=b"{}",
            )
        ),
    )

    trace = reader.find_call("call-1")
    assert [r.function_id for r in trace.results] == ["duration_rms", "silence_ratio"]


def test_a_redelivered_result_shows_once_per_function(reader, broker):
    """At-least-once means a redelivered call produces two records with the
    same key. The trace should show the function once, not twice."""
    _seed_call(broker, "call-1", functions=("duration_rms",))
    _seed_call(broker, "call-1", functions=("duration_rms",))

    trace = reader.find_call("call-1")
    assert len(trace.results) == 1


def test_an_unhydrated_call_reports_no_missing_functions(reader, broker):
    """A call that failed hydration has no reference, so no function ever saw
    it. Listing all ten as "missing" would blame them for the hydrator."""
    reader.declarations = {"duration_rms": None, "silence_ratio": None}

    trace = reader.find_call("never-hydrated")

    assert not trace.hydrated
    assert trace.missing == []
    assert not trace.complete


def test_missing_functions_are_the_expected_set_minus_answers(reader, broker):
    """§7's completeness question, answered manually until the aggregator
    exists."""
    from faas_sdk.config import FunctionConfig

    def declaration(function_id):
        return FunctionConfig(
            function_id=function_id,
            function_version="1.0.0",
            image="x",
            results_topic_partitions=200,
        )

    reader.declarations = {
        f: declaration(f) for f in ("duration_rms", "silence_ratio", "energy_vad")
    }
    _seed_call(broker, "call-1", functions=("duration_rms", "silence_ratio"))

    trace = reader.find_call("call-1")

    assert trace.missing == ["energy_vad"]
    assert not trace.complete


def test_a_duration_disagreement_is_surfaced(reader, broker):
    """A truncated upload hydrates into a FLAC of the wrong length, and the
    only signal is the function that measured the audio disagreeing with the
    hydrator that described it."""
    codec = JsonCodec()
    partition = partition_for(b"call-truncated", RESULTS_PARTITIONS)
    broker.add(
        INTERNAL_TOPIC,
        partition_for(b"call-truncated", INTERNAL_PARTITIONS),
        codec.encode_reference(
            AudioReference(call_id="call-truncated", object_key="x.flac", duration_seconds=300.0)
        ),
    )
    broker.add(
        RESULTS_TOPIC,
        partition,
        codec.encode_result(
            Result(
                call_id="call-truncated",
                function_id="duration_rms",
                function_version="1.0.0",
                status=Status.SUCCESS,
                input_object_key="x.flac",
                input_offset=1,
                attempt=1,
                payload=json.dumps(
                    {"duration_seconds": 150.0, "reference_duration_seconds": 300.0}
                ).encode(),
            )
        ),
    )

    trace = reader.find_call("call-truncated")
    assert trace.duration_disagreement == pytest.approx(-150.0)


def test_a_failed_result_carries_its_error(reader, broker):
    codec = JsonCodec()
    partition = partition_for(b"call-failed", RESULTS_PARTITIONS)
    broker.add(
        RESULTS_TOPIC,
        partition,
        codec.encode_result(
            Result(
                call_id="call-failed",
                function_id="flaky_analyzer",
                function_version="1.0.0",
                status=Status.FAILED,
                input_object_key="x.flac",
                input_offset=1,
                attempt=3,
                error=ErrorInfo("SYNTHETIC_POISON", "in the broken bucket", retryable=False),
            )
        ),
    )

    (result,) = reader.find_call("call-failed").results
    assert result.status == "FAILED"
    assert result.error_code == "SYNTHETIC_POISON"
    assert result.error_retryable is False
    assert result.attempt == 3


def test_dead_letters_are_read_from_headers(reader, broker):
    """The body is the input message byte for byte so it can be replayed
    (§5.4); everything describing the failure is in the headers."""
    broker.add(
        "faas.dlq.duration_rms",
        0,
        b"the original input bytes",
        headers={
            "faas.function_id": b"duration_rms",
            "faas.function_version": b"1.0.0",
            "faas.group_id": b"duration_rms:1.0.0",
            "faas.error.code": b"DECODE_ERROR",
            "faas.error.message": b"not FLAC",
            "faas.error.retryable": b"false",
            "faas.attempt": b"1",
            "faas.call_id": b"call-9",
            "faas.source.topic": b"faas.audio.internal",
            "faas.source.partition": b"7",
            "faas.source.offset": b"42",
            "faas.failed_at": b"2026-08-14T00:00:00+00:00",
        },
    )

    (dead,) = reader.dead_letters()

    assert dead.error_code == "DECODE_ERROR"
    assert dead.retryable is False
    assert dead.call_id == "call-9"
    # What a replay would need to know.
    assert (dead.source_topic, dead.source_partition, dead.source_offset) == (
        "faas.audio.internal",
        7,
        42,
    )
    assert dead.body_bytes == len(b"the original input bytes")


def test_a_dlq_record_with_no_headers_does_not_crash_the_view(reader, broker):
    """Anything can be produced to a topic. A console that 500s on one odd
    record is useless exactly when it is needed."""
    broker.add("faas.dlq.duration_rms", 0, b"mystery", headers={})

    (dead,) = reader.dead_letters()
    assert dead.error_code == "?"
    assert dead.call_id == ""


# -- config lint -----------------------------------------------------------


def _config(**overrides):
    from faas_sdk.config import FunctionConfig

    fields = dict(
        function_id="duration_rms",
        function_version="1.0.0",
        image="x",
        results_topic=RESULTS_TOPIC,
        results_topic_partitions=200,
        dlq_topic="faas.dlq.duration_rms",
    )
    fields.update(overrides)
    return FunctionConfig(**fields)


def test_lint_is_quiet_when_the_declaration_matches_the_topic(reader):
    reader.declarations = {"duration_rms": _config()}
    assert reader.lint() == []


def test_lint_catches_a_declaration_claiming_more_partitions_than_exist(reader):
    """The failure this exists for: the SDK computes the partition itself and
    passes it explicitly, so producing to partition 150 of a 12-partition topic
    fails on the first result rather than at startup."""
    reader.declarations = {"duration_rms": _config(results_topic_partitions=500)}

    (finding,) = reader.lint()

    assert finding.severity == "error"
    assert "500" in finding.message and "200" in finding.message


def test_lint_warns_when_partitions_would_go_unused(reader):
    reader.declarations = {"duration_rms": _config(results_topic_partitions=50)}

    (finding,) = reader.lint()

    assert finding.severity == "warning"
    assert "never receive" in finding.message


def test_lint_flags_a_missing_dlq_topic(reader):
    reader.declarations = {"duration_rms": _config(dlq_topic="faas.dlq.nope")}

    (finding,) = reader.lint()

    assert finding.severity == "warning"
    assert "nowhere to go" in finding.message
