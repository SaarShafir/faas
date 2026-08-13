"""The sink loop: land every result, poison to the DLQ, low-water-mark commits."""

from __future__ import annotations

from faas_sdk.codec import JsonCodec
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.models import ErrorInfo, InboundMessage, Status
from faas_sdk.testing import FakeClock, FakeConsumer, FakeProducer
from faas_sink.config import SinkConfig
from faas_sink.sink import SinkRunner
from faas_sink.store import SqliteResultsStore

RESULTS_TOPIC = "faas.results"
KEY = "call-1:duration_rms:1.0.0"


def _config(**overrides):
    fields = dict(
        results_topic=RESULTS_TOPIC,
        dlq_topic="faas.dlq.sink",
        commit_interval_seconds=0.0,
        poll_timeout_seconds=0.0,
    )
    fields.update(overrides)
    return SinkConfig(**fields)


def _result_message(offset: int = 0, value: bytes | None = None, key: str | bytes | None = None):
    if value is None:
        from faas_sdk.models import Result

        result = Result(
            call_id="call-1",
            function_id="duration_rms",
            function_version="1.0.0",
            status=Status.SUCCESS,
            input_object_key="call-1.flac",
            input_offset=offset,
            attempt=1,
            payload=b"{}",
            payload_content_type="application/json",
        )
        value = JsonCodec().encode_result(result)
    key = key or KEY
    return InboundMessage(
        topic=RESULTS_TOPIC,
        partition=0,
        offset=offset,
        value=value,
        key=key if isinstance(key, bytes) else key.encode(),
    )


def _build(**config_overrides):
    codec = config_overrides.pop("codec", JsonCodec())
    config = _config(**config_overrides)
    consumer = FakeConsumer(assignment=[(RESULTS_TOPIC, 0)])
    producer = FakeProducer()
    clock = FakeClock()
    store = SqliteResultsStore(clock=clock)
    runner = SinkRunner(
        config=config,
        consumer=consumer,
        store=store,
        codec=codec,
        dlq=DeadLetterQueue(config=config, producer=producer, clock=clock),
        clock=clock,
    )
    runner.start()
    return runner, consumer, store, producer


def test_lands_a_result_and_commits_the_offset():
    runner, consumer, store, _ = _build()
    consumer.feed(_result_message(offset=0))
    runner.run_once()
    assert store.count() == 1
    assert store.get(KEY).call_id == "call-1"
    assert consumer.commits == [[((RESULTS_TOPIC, 0), 1)]]


def test_failed_and_skipped_results_land_too():
    """The store is where "no result yet" becomes distinguishable from "no
    result ever" (§5.4) -- dropping FAILED records here would reopen that."""
    from faas_sdk.models import Result

    runner, consumer, store, _ = _build()
    failed = _result_message(offset=0, value=JsonCodec().encode_result(
        Result(
            call_id="call-2", function_id="duration_rms", function_version="1.0.0",
            status=Status.FAILED,
            error=ErrorInfo("CRASH", "boom", retryable=False),
            input_object_key="call-2.flac", input_offset=0, attempt=1,
        )
    ), key=b"call-2:duration_rms:1.0.0")
    skipped = _result_message(offset=1, value=JsonCodec().encode_result(
        Result(
            call_id="call-3", function_id="duration_rms", function_version="1.0.0",
            status=Status.SKIPPED,
            input_object_key="call-3.flac", input_offset=1, attempt=1,
        )
    ), key=b"call-3:duration_rms:1.0.0")
    consumer.feed(failed, skipped)
    runner.run_once()
    runner.run_once()
    assert store.count() == 2
    assert store.get("call-2:duration_rms:1.0.0").status is Status.FAILED
    assert store.get("call-3:duration_rms:1.0.0").status is Status.SKIPPED


def test_poison_goes_to_the_dlq_and_is_committed():
    """An undecodable record must never accrue unbounded lag (§5.4)."""
    runner, consumer, store, producer = _build()
    consumer.feed(_result_message(offset=0, value=b"\x00\xff not a result"))
    runner.run_once()
    assert store.count() == 0
    records = producer.records_for("faas.dlq.sink")
    assert len(records) == 1
    assert records[0].value == b"\x00\xff not a result"
    assert consumer.commits == [[((RESULTS_TOPIC, 0), 1)]]


def test_unspecified_status_is_poison_in_the_sink():
    """The STATUS_UNSPECIFIED decision pays off here: a record whose status was
    never set is a producer bug, and the sink treats it like any other poison --
    DLQ, committed, never landed as a fake success."""
    from faas.v1 import result_pb2

    message = result_pb2.Result()
    message.call_id = "call-1"
    message.function_id = "duration_rms"
    message.function_version = "1.0.0"
    message.input_object_key = "call-1.flac"
    message.input_offset = 0
    message.attempt = 1

    from faas_sdk.codec_protobuf import ProtobufCodec

    runner, consumer, store, producer = _build(codec=ProtobufCodec())
    consumer.feed(_result_message(offset=0, value=message.SerializeToString()))
    runner.run_once()
    assert store.count() == 0
    assert len(producer.records_for("faas.dlq.sink")) == 1


def test_poison_dlq_headers_name_the_offending_function():
    """The key is composite even when the body is poison, so the DLQ record
    says which function's producer wrote the garbage."""
    runner, consumer, store, producer = _build()
    consumer.feed(_result_message(offset=0, value=b"junk", key=b"call-9:rms:2.0.0"))
    runner.run_once()
    (record,) = producer.records_for("faas.dlq.sink")
    headers = dict(record.headers)
    assert headers["faas.call_id"] == b"call-9"
    assert headers["faas.function_id"] == b"rms"
    assert headers["faas.function_version"] == b"2.0.0"
    assert headers["faas.error.code"] == b"DECODE_ERROR"


def test_redelivery_is_a_noop_in_the_store():
    """At-least-once means the same record can arrive twice (restart, rebalance).
    The upsert makes the second landing a no-op, and the ledger has nothing
    new to commit -- the offset was already past it."""
    runner, consumer, store, _ = _build()
    consumer.feed(_result_message(offset=0))
    runner.run_once()
    consumer.feed(_result_message(offset=0))  # redelivered: same offset, same record
    runner.run_once()
    assert store.count() == 1
    assert consumer.commits == [[((RESULTS_TOPIC, 0), 1)]]


def test_commit_interval_throttles_commits():
    runner, consumer, store, _ = _build(commit_interval_seconds=60.0)
    consumer.feed(_result_message(offset=0), _result_message(offset=1))
    runner.run_once()
    assert consumer.commits == []
    runner.clock.advance(60.0)
    runner.run_once()
    assert consumer.commits == [[((RESULTS_TOPIC, 0), 2)]]


def test_revoke_commits_and_forgets_partitions():
    runner, consumer, store, _ = _build()
    consumer.feed(_result_message(offset=0))
    runner.run_once()
    consumer.trigger_revoke([(RESULTS_TOPIC, 0)])
    assert consumer.commits[-1] == [((RESULTS_TOPIC, 0), 1)]
    assert runner.ledger.partitions == []


def test_sink_subscribes_to_the_results_topic_only():
    runner, consumer, _, _ = _build()
    assert consumer.topics == [RESULTS_TOPIC]