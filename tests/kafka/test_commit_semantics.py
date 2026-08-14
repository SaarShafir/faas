"""Low-water-mark commits against a real broker (spec §5.3).

`tests/test_offsets.py` proves the ledger picks the right number. It cannot
prove that number means what we think it means to Kafka -- that "commit N"
makes a restarted consumer resume *at* N rather than after it. An off-by-one
there is either silent data loss or permanent reprocessing, and every unit test
would still be green.

So: drive a real consumer through out-of-order completion, restart it in the
same group, and see which files actually come back.

The pool here is `ManualPool`, deliberately. What is under test is the
consumer, the ledger and the broker; controlling completion order precisely is
the whole point, and a real pool would only add timing noise.
"""

from __future__ import annotations

import time

import pytest

from faas_sdk.clock import SystemClock
from faas_sdk.codec import JsonCodec
from faas_sdk.config import FunctionConfig
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.kafka import ConfluentConsumer, ConfluentProducer
from faas_sdk.models import OutboundRecord, TopicPartition
from faas_sdk.results import ResultEmitter
from faas_sdk.runner import FunctionRunner
from faas_sdk.testing import ManualPool, reference

pytestmark = pytest.mark.kafka

CALLS = ["c0", "c1", "c2"]


def _config(topic, group, results_topic):
    return FunctionConfig(
        function_id="committer",
        function_version="1.0.0",
        image="img",
        input_topic=topic,
        results_topic=results_topic,
        in_flight=len(CALLS),
        retry_budget=1,
        commit_interval_seconds=0.0,
    )


def _publish(bootstrap_servers, topic):
    producer = ConfluentProducer({"bootstrap.servers": bootstrap_servers})
    for call_id in CALLS:
        producer.produce(
            OutboundRecord(
                topic=topic,
                key=call_id.encode(),
                value=JsonCodec().encode_reference(reference(call_id=call_id)),
            )
        )
    assert producer.flush(30) == 0


def _build(bootstrap_servers, config):
    consumer = ConfluentConsumer(
        config.consumer_config(bootstrap_servers, **{"group.id": config.group_id})
    )
    producer = ConfluentProducer({"bootstrap.servers": bootstrap_servers})
    codec = JsonCodec()
    clock = SystemClock()
    pool = ManualPool(max_in_flight=config.in_flight)
    runner = FunctionRunner(
        config=config,
        consumer=consumer,
        pool=pool,
        codec=codec,
        results=ResultEmitter(config=config, producer=producer, codec=codec, clock=clock),
        dlq=DeadLetterQueue(config=config, producer=producer, clock=clock),
        clock=clock,
    )
    runner.start()
    return runner, pool


def _dispatch_all(runner, expected, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and runner.in_flight < expected:
        runner.run_once()
    assert runner.in_flight == expected, f"only dispatched {runner.in_flight}/{expected}"


def _redelivered(bootstrap_servers, config, timeout=30):
    """What a fresh consumer in the same group is handed."""
    consumer = ConfluentConsumer(
        config.consumer_config(bootstrap_servers, **{"group.id": config.group_id})
    )
    consumer.subscribe([config.input_topic])
    offsets = []
    deadline = time.monotonic() + timeout
    idle_until = None
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                if idle_until is None:
                    idle_until = time.monotonic() + 4
                elif time.monotonic() > idle_until:
                    break
                continue
            offsets.append(message.offset)
            idle_until = None
    finally:
        consumer.close()
    return offsets


def test_an_unfinished_file_comes_back_after_a_restart(bootstrap_servers, topic_factory, group_id):
    """The out-of-order case: the last two finish, the first does not.

    Nothing may be committed, because committing anything past offset 0 would
    lose it. All three come back.
    """
    topic = topic_factory(prefix="lwm")
    config = _config(topic, group_id, topic_factory(prefix="lwm-results"))
    _publish(bootstrap_servers, topic)

    runner, pool = _build(bootstrap_servers, config)
    _dispatch_all(runner, len(CALLS))
    pool.succeed("c1")
    pool.succeed("c2")
    runner.run_once()
    runner.close()

    assert _redelivered(bootstrap_servers, config) == [0, 1, 2]


def test_a_finished_prefix_is_not_redelivered(bootstrap_servers, topic_factory, group_id):
    """Complete the first only: the low-water mark advances to 1, so offset 0 is
    durably done and 1 and 2 come back."""
    topic = topic_factory(prefix="lwm-prefix")
    config = _config(topic, group_id, topic_factory(prefix="lwm-prefix-results"))
    _publish(bootstrap_servers, topic)

    runner, pool = _build(bootstrap_servers, config)
    _dispatch_all(runner, len(CALLS))
    pool.succeed("c0")
    runner.run_once()
    runner.close()

    assert _redelivered(bootstrap_servers, config) == [1, 2]


def test_everything_finished_leaves_nothing_to_redeliver(
    bootstrap_servers, topic_factory, group_id
):
    topic = topic_factory(prefix="lwm-all")
    config = _config(topic, group_id, topic_factory(prefix="lwm-all-results"))
    _publish(bootstrap_servers, topic)

    runner, pool = _build(bootstrap_servers, config)
    _dispatch_all(runner, len(CALLS))
    pool.succeed_all()
    runner.run_once()
    runner.close()

    assert _redelivered(bootstrap_servers, config) == []


def test_committing_the_highest_completed_offset_loses_files(
    bootstrap_servers, topic_factory, group_id
):
    """The negative control, and the bug the ledger exists to prevent.

    Commit 3 -- "the highest completed, plus one" -- while offset 0 is still in
    flight, and offsets 0 through 2 are simply gone. Nothing errors; the files
    are never processed and never seen again.

    This is what makes the three tests above meaningful: the same setup, the
    same broker, and the naive commit really does destroy data here.
    """
    topic = topic_factory(prefix="lwm-control")
    config = _config(topic, group_id, topic_factory(prefix="lwm-control-results"))
    _publish(bootstrap_servers, topic)

    runner, pool = _build(bootstrap_servers, config)
    _dispatch_all(runner, len(CALLS))
    pool.succeed("c1")
    pool.succeed("c2")

    # Bypass the ledger and commit as a naive implementation would.
    runner.consumer.commit([(TopicPartition(topic, 0), 3)])
    time.sleep(1)
    runner.consumer.close()

    assert _redelivered(bootstrap_servers, config) == []


def test_the_committed_offset_is_the_next_one_to_read(bootstrap_servers, topic_factory, group_id):
    """Kafka's convention, asserted directly rather than inferred.

    The ledger emits "next offset to consume"; librdkafka's commit takes the
    same. If those two ever disagreed, every restart would silently repeat or
    skip exactly one file per partition.
    """
    from confluent_kafka import Consumer
    from confluent_kafka import TopicPartition as KafkaTopicPartition

    topic = topic_factory(prefix="lwm-convention")
    config = _config(topic, group_id, topic_factory(prefix="lwm-convention-results"))
    _publish(bootstrap_servers, topic)

    runner, pool = _build(bootstrap_servers, config)
    _dispatch_all(runner, len(CALLS))
    pool.succeed("c0")
    runner.run_once()
    runner.close()

    probe = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": config.group_id})
    try:
        (committed,) = probe.committed([KafkaTopicPartition(topic, 0)], timeout=30)
    finally:
        probe.close()

    assert committed.offset == 1, "one file done means the next to read is offset 1"
