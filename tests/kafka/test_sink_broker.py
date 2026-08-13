"""The results sink against a real broker.

The unit suite proves the loop's logic; this proves the loop against the thing
it actually talks to. What it pins:

  - results land in the store, SUCCESS and FAILED alike (the §5.4 distinction
    between "no result yet" and "no result ever" lives here);
  - poison (bytes that are not a result) goes to the sink's DLQ with the
    offending function named via the §6 composite key, and the offset is
    committed -- one bad producer cannot accrue unbounded lag (§5.4);
  - a restart in the same group sees nothing redelivered: committed offsets
    hold, and the store's upsert would make any redelivery a no-op anyway.
"""

from __future__ import annotations

import tempfile
import time

import pytest

from faas_sdk.clock import SystemClock
from faas_sdk.codec import JsonCodec
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.kafka import ConfluentConsumer, ConfluentProducer
from faas_sdk.models import ErrorInfo, OutboundRecord, Result, Status
from faas_sink.config import SinkConfig
from faas_sink.sink import SinkRunner
from faas_sink.store import SqliteResultsStore

pytestmark = pytest.mark.kafka


def _result(call_id: str, status=Status.SUCCESS, payload=b"{}", error=None) -> Result:
    return Result(
        call_id=call_id,
        function_id="duration_rms",
        function_version="1.0.0",
        status=status,
        input_object_key=f"{call_id}.flac",
        input_offset=0,
        attempt=1,
        error=error,
        payload=payload,
        payload_content_type="application/json",
    )


def _publish(bootstrap_servers, topic, *results: Result):
    producer = ConfluentProducer(
        {"bootstrap.servers": bootstrap_servers},
        num_partitions_by_topic={topic: 2},
    )
    for result in results:
        producer.produce(
            OutboundRecord(
                topic=topic,
                key=result.key,
                partition_key=result.partition_key,
                value=JsonCodec().encode_result(result),
                headers={
                    "faas.function_id": result.function_id.encode(),
                    "faas.function_version": result.function_version.encode(),
                    "faas.status": result.status.name.encode(),
                },
            )
        )
    assert producer.flush(30) == 0


def _publish_poison(bootstrap_servers, topic):
    producer = ConfluentProducer(
        {"bootstrap.servers": bootstrap_servers},
        num_partitions_by_topic={topic: 2},
    )
    producer.produce(
        OutboundRecord(
            topic=topic,
            key=b"call-9:rms:2.0.0",
            partition_key=b"call-9",
            value=b"\x00\xff this is not a result",
        )
    )
    assert producer.flush(30) == 0


def _build_sink(bootstrap_servers, config, db_path):
    consumer = ConfluentConsumer(config.consumer_config(bootstrap_servers))
    producer = ConfluentProducer({"bootstrap.servers": bootstrap_servers})
    clock = SystemClock()
    runner = SinkRunner(
        config=config,
        consumer=consumer,
        store=SqliteResultsStore(db_path, clock=clock),
        codec=JsonCodec(),
        dlq=DeadLetterQueue(config=config, producer=producer, clock=clock),
        clock=clock,
    )
    runner.start()
    return runner


def _read_all(bootstrap_servers, topic, timeout=30):
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"drain-{topic}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    seen = []
    deadline = time.monotonic() + timeout
    idle_until = None
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                if idle_until is None:
                    idle_until = time.monotonic() + 3
                elif time.monotonic() > idle_until:
                    break
                continue
            if not message.error():
                seen.append(message)
                idle_until = None
    finally:
        consumer.close()
    return seen


def _committed_offsets(bootstrap_servers, group, topic, partitions=2):
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group})
    try:
        committed = consumer.committed(
            [TopicPartition(topic, p) for p in range(partitions)], timeout=30
        )
        return {tp.partition: tp.offset for tp in committed}
    finally:
        consumer.close()


def test_sink_lands_results_and_dlqs_poison(bootstrap_servers, topic_factory, group_id):
    results_topic = topic_factory(prefix="sink-results", partitions=2)
    dlq_topic = topic_factory(prefix="sink-dlq")

    ok = _result("call-1", payload=b'{"duration": 300.0}')
    failed = _result(
        "call-2",
        status=Status.FAILED,
        payload=None,
        error=ErrorInfo("LIBRARY_CRASH", "boom", retryable=False),
    )
    _publish(bootstrap_servers, results_topic, ok, failed)
    _publish_poison(bootstrap_servers, results_topic)

    config = SinkConfig(
        results_topic=results_topic,
        results_topic_partitions=2,
        dlq_topic=dlq_topic,
        group_id=group_id,
        commit_interval_seconds=0.0,
    )

    with tempfile.TemporaryDirectory() as tmp:
        db_path = f"{tmp}/results.db"
        runner = _build_sink(bootstrap_servers, config, db_path)
        try:
            # Everything consumed -- results and poison alike -- is signalled
            # by the committed offsets reaching the end of both partitions.
            # That is the condition to wait on, not the store count: the
            # poison record lands nowhere except the DLQ.
            expected = _end_offsets(bootstrap_servers, results_topic)
            _await_committed(bootstrap_servers, runner, group_id, results_topic, expected)

            landed = runner.store.get(ok.key.decode())
            assert landed is not None and landed.payload == b'{"duration": 300.0}'
            failed_landed = runner.store.get(failed.key.decode())
            assert failed_landed is not None and failed_landed.status is Status.FAILED
            assert runner.store.count() == 2, "poison must never land in the store"

            # The poison record: DLQ'd with the composite key naming the offender.
            dlq_records = _read_all(bootstrap_servers, dlq_topic)
            assert len(dlq_records) == 1
            headers = dict(dlq_records[0].headers())
            assert headers["faas.call_id"] == b"call-9"
            assert headers["faas.function_id"] == b"rms"
            assert headers["faas.function_version"] == b"2.0.0"
            assert headers["faas.error.code"] == b"DECODE_ERROR"
        finally:
            runner.close()

        runner2 = _build_sink(bootstrap_servers, config, db_path)
        time.sleep(3)  # give a redelivery every chance to show up
        for _ in range(10):
            runner2.run_once()
        runner2.close()
        assert runner2.store.count() == 2, "restart must not duplicate landings"
        assert len(_read_all(bootstrap_servers, dlq_topic)) == 1, "poison must not replay"


def _end_offsets(bootstrap_servers, topic, partitions=2):
    """Next-offset-to-read per partition: what a fully-consumed group commits."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"end-{topic}",
            "enable.auto.commit": False,
        }
    )
    try:
        tps = [TopicPartition(topic, p) for p in range(partitions)]
        consumer.assign(tps)
        return {
            tp.partition: consumer.get_watermark_offsets(tp, timeout=30)[1] for tp in tps
        }
    finally:
        consumer.close()


def _committed_offsets(bootstrap_servers, group, topic, partitions=2):
    from confluent_kafka import Consumer, KafkaException, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group})
    try:
        # Joining the group is what discovers the coordinator; without a poll,
        # `committed` can transiently fail with NOT_COORDINATOR while the sink
        # is still joining.
        consumer.poll(1.0)
        try:
            committed = consumer.committed(
                [TopicPartition(topic, p) for p in range(partitions)], timeout=30
            )
        except KafkaException:
            return None
        return {
            # A partition the group never consumed (no records) reports -1001
            # (OFFSET_INVALID); for "fully consumed" that means 0.
            tp.partition: max(tp.offset, 0) for tp in committed
        }
    finally:
        consumer.close()


def _await_committed(bootstrap_servers, runner, group, topic, expected, timeout=60):
    """Drive the sink until its committed offsets reach the end of every
    partition -- the only signal that results *and* poison were all consumed
    (the poison lands nowhere except the DLQ)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        runner.run_once()
        if _committed_offsets(bootstrap_servers, group, topic) == expected:
            return
        time.sleep(0.05)
    raise AssertionError(f"committed offsets never reached {expected}")