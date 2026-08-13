"""The failure the SDK exists to prevent (spec §5.2), against a real broker.

    "Per-file processing takes seconds to minutes. A naive poll loop exceeds
    max.poll.interval.ms, the broker evicts the consumer mid-file, a rebalance
    fires, and another consumer reprocesses the same file -- forever, if the
    function is slow enough. This is the single most likely way this project
    fails in production."

No fake can show this: eviction is a decision the group coordinator makes.

Every assertion here is paired with a negative control. "The consumer was not
evicted" passes trivially if the scenario never stressed the poll interval, so
each case is run twice -- once with the design under test, once with the naive
version -- and the naive one must actually fail.
"""

from __future__ import annotations

import time

import pytest

from faas_sdk.clock import SystemClock
from faas_sdk.codec import JsonCodec
from faas_sdk.config import FunctionConfig
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.kafka import ConfluentConsumer, ConfluentProducer
from faas_sdk.models import OutboundRecord
from faas_sdk.pool import InlineWorkerPool, ProcessWorkerPool
from faas_sdk.results import ResultEmitter
from faas_sdk.runner import FunctionRunner
from faas_sdk.testing import (
    SlowFunction,
    reference,
    slow_function_factory,
    stub_audio_handle_factory,
)
from faas_sdk.testing import stub_audio_handle_factory as audio_handle_factory_stub

pytestmark = pytest.mark.kafka

# Small enough to keep the suite quick, large enough that librdkafka accepts it
# and that an ordinary GC pause cannot trip it.
POLL_INTERVAL_MS = 8000
WORK_SECONDS = 14.0  # comfortably longer than the poll interval


def _config(topic, group, results_topic):
    return FunctionConfig(
        function_id="slow",
        function_version="1.0.0",
        image="img",
        input_topic=topic,
        results_topic=results_topic,
        in_flight=1,
        per_file_timeout_seconds=120,
        retry_budget=1,
        commit_interval_seconds=0.0,
    )


def _publish_one(bootstrap_servers, topic, call_id="c1"):
    producer = ConfluentProducer({"bootstrap.servers": bootstrap_servers})
    producer.produce(
        OutboundRecord(
            topic=topic,
            key=call_id.encode(),
            value=JsonCodec().encode_reference(reference(call_id=call_id)),
        )
    )
    assert producer.flush(30) == 0


def _consumer(bootstrap_servers, config):
    return ConfluentConsumer(
        config.consumer_config(
            bootstrap_servers,
            **{
                "max.poll.interval.ms": POLL_INTERVAL_MS,
                "session.timeout.ms": 6000,
                "heartbeat.interval.ms": 2000,
                "group.id": config.group_id,
            },
        )
    )


def test_a_naive_loop_is_evicted_and_reprocesses_the_file(
    bootstrap_servers, topic_factory, group_id
):
    """The negative control, and the bug in its natural habitat.

    Poll, then block for longer than max.poll.interval.ms, then poll again.
    librdkafka reports the eviction as _MAX_POLL_EXCEEDED and rejoins; because
    nothing was committed, the same file comes back. That is the "forever" loop
    from §5.2, and it is what every other test here is asserting the absence of.

    Uses a raw confluent Consumer rather than our adapter, so it observes what
    librdkafka actually does rather than what we do about it.
    """
    from confluent_kafka import Consumer, KafkaError

    topic = topic_factory(prefix="naive")
    config = _config(topic, group_id, topic + ".results")
    _publish_one(bootstrap_servers, topic)

    consumer = Consumer(
        config.consumer_config(
            bootstrap_servers,
            **{
                "max.poll.interval.ms": POLL_INTERVAL_MS,
                "session.timeout.ms": 6000,
                "heartbeat.interval.ms": 2000,
                "group.id": group_id,
            },
        )
    )
    consumer.subscribe([topic])

    delivered = []
    evicted = False
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline and not (evicted and len(delivered) >= 2):
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._MAX_POLL_EXCEEDED:
                    evicted = True
                continue
            delivered.append(message.offset())
            # The naive part: work happens on the poll thread, and nothing is
            # committed until it finishes.
            time.sleep(WORK_SECONDS)
    finally:
        consumer.close()

    assert evicted, "expected the broker to evict a consumer that stopped polling"
    assert len(delivered) >= 2, "expected the uncommitted file to be redelivered"
    assert delivered[0] == delivered[1] == 0, "the same offset, reprocessed"


def test_the_adapter_survives_an_eviction_instead_of_crashing(
    bootstrap_servers, topic_factory, group_id
):
    """_MAX_POLL_EXCEEDED is an event, not a fatal error.

    Treating every non-EOF error as fatal -- which the adapter used to do --
    turns a recoverable eviction into a crashed pod that drops whatever was in
    flight, making a bad situation strictly worse. It is counted and logged
    loudly instead, because a non-zero count means files are being reprocessed.
    """
    topic = topic_factory(prefix="eviction")
    config = _config(topic, group_id, topic + ".results")
    _publish_one(bootstrap_servers, topic)

    consumer = _consumer(bootstrap_servers, config)
    consumer.subscribe([topic])

    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline and consumer.max_poll_exceeded == 0:
            # No exception may escape this call, whatever the broker thinks.
            message = consumer.poll(1.0)
            if message is not None:
                time.sleep(WORK_SECONDS)
    finally:
        consumer.close()

    assert consumer.max_poll_exceeded >= 1


def test_the_runner_survives_work_longer_than_the_poll_interval(
    bootstrap_servers, topic_factory, group_id
):
    """The design under test. Same broker settings, same slow work, but the
    poll loop never blocks on it -- so the coordinator never evicts, and the
    file is delivered and committed exactly once."""
    topic = topic_factory(prefix="decoupled")
    results_topic = topic_factory(prefix="decoupled-results")
    config = _config(topic, group_id, results_topic)
    _publish_one(bootstrap_servers, topic)

    consumer = _consumer(bootstrap_servers, config)
    producer = ConfluentProducer({"bootstrap.servers": bootstrap_servers})
    codec = JsonCodec()
    clock = SystemClock()

    pool = ProcessWorkerPool(
        function_factory=slow_function_factory(WORK_SECONDS),
        audio_handle_factory=stub_audio_handle_factory,
        max_in_flight=1,
    )
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

    dispatched = 0
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            runner.run_once()
            dispatched = max(dispatched, len(runner.ledger.partitions))
            if runner.in_flight == 0 and runner.consumer.assignment() and dispatched:
                break
        producer.flush(30)
    finally:
        runner.close()

    # Exactly one delivery: no eviction, therefore no rebalance, therefore no
    # reprocessing. A second delivery here is the §5.2 bug.
    results = _read_all(bootstrap_servers, results_topic)
    assert len(results) == 1, f"expected one result, got {len(results)}"

    # And the offset really is committed at the broker, not just in our ledger.
    assert _committed_offset(bootstrap_servers, config.group_id, topic) == 1


def test_an_inline_pool_blocks_the_poll_loop_and_is_evicted(
    bootstrap_servers, topic_factory, group_id
):
    """Why InlineWorkerPool must not be the production default.

    It runs `process()` synchronously inside `submit()`, so the work happens on
    the poll thread and the loop stops polling for its duration -- structurally
    the naive loop above, wearing the SDK's clothes. It is fine for tests and
    for functions whose per-file time is far below max.poll.interval.ms, and
    unsafe for anything the spec is actually about ("seconds to minutes").

    This test is the reason `bootstrap.build_runner` defaults to a process pool.
    """
    topic = topic_factory(prefix="inline")
    results_topic = topic_factory(prefix="inline-results")
    config = _config(topic, group_id, results_topic)
    _publish_one(bootstrap_servers, topic)

    consumer = _consumer(bootstrap_servers, config)
    producer = ConfluentProducer({"bootstrap.servers": bootstrap_servers})
    codec = JsonCodec()
    clock = SystemClock()

    runner = FunctionRunner(
        config=config,
        consumer=consumer,
        pool=InlineWorkerPool(
            function=SlowFunction(WORK_SECONDS),
            audio_handle_factory=audio_handle_factory_stub(),
            max_in_flight=1,
            clock=clock,
        ),
        codec=codec,
        results=ResultEmitter(config=config, producer=producer, codec=codec, clock=clock),
        dlq=DeadLetterQueue(config=config, producer=producer, clock=clock),
        clock=clock,
    )
    runner.start()

    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline and consumer.max_poll_exceeded == 0:
            runner.run_once()
    finally:
        runner.close()

    assert consumer.max_poll_exceeded >= 1, (
        "expected an inline pool to block the poll loop into an eviction"
    )


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


def _committed_offset(bootstrap_servers, group, topic, partition=0):
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group})
    try:
        (committed,) = consumer.committed([TopicPartition(topic, partition)], timeout=30)
        return committed.offset
    finally:
        consumer.close()
