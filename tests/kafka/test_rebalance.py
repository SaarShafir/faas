"""A real cooperative-sticky rebalance with in-flight work (the P0 gap).

The poll-interval suite covers eviction; the commit suite covers restarts.
Neither moves partitions under a live coordinator while files are in flight --
which is the last part of §5.2 still resting on fakes, and the spec's stated
most-likely failure. This test does it:

  - consumer A holds two partitions with four calls in flight: the first call
    on each partition is finished and committed, the second is not;
  - consumer B joins the group, so cooperative-sticky moves one partition to B;
  - A's `_on_revoke` drains, force-commits, and cancels the in-flight work on
    the moved partition -- the "bounded chance to finish" from §5.2;
  - the finished-and-committed call on the moved partition is *not* redelivered;
  - the unfinished call on the moved partition *is* redelivered to B: at-least-
    once means nothing silently lost, and results are idempotent on the
    composite key (§5.3);
  - every call produces exactly one result.

The pool is `ManualPool`, as in test_commit_semantics: completion order is the
thing under control. A real pool would only add timing noise.
"""

from __future__ import annotations

import time

import pytest

from faas_sdk.clock import SystemClock
from faas_sdk.codec import JsonCodec
from faas_sdk.config import FunctionConfig
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.kafka import ConfluentConsumer, ConfluentProducer
from faas_sdk.models import OutboundRecord, Status
from faas_sdk.partitioner import partition_for
from faas_sdk.results import ResultEmitter
from faas_sdk.runner import FunctionRunner
from faas_sdk.testing import ManualPool, reference

pytestmark = pytest.mark.kafka

PER_PARTITION = 2
DRAIN_SECONDS = 1.0


class RecordingConsumer(ConfluentConsumer):
    """The runner's consumer, with the rebalance events recorded.

    Assertions on what `_on_revoke` was handed need the events themselves,
    which the adapter would otherwise swallow into the runner's callbacks.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.assigned: list[list] = []
        self.revoked: list[list] = []

    def subscribe(self, topics, on_assign=None, on_revoke=None):
        def _recording_assign(partitions):
            self.assigned.append(partitions)
            if on_assign:
                on_assign(partitions)

        def _recording_revoke(partitions):
            self.revoked.append(partitions)
            if on_revoke:
                on_revoke(partitions)

        super().subscribe(topics, on_assign=_recording_assign, on_revoke=_recording_revoke)


def _balanced_calls(num_partitions: int, per_partition: int):
    """Call ids that land with `per_partition` calls on each partition, per the
    murmur2 partitioner the broker uses for keyed produces."""
    by_partition = {p: [] for p in range(num_partitions)}
    calls = []
    for index in range(num_partitions * per_partition * 20):
        call = f"c{index}"
        partition = partition_for(call.encode(), num_partitions)
        if len(by_partition[partition]) < per_partition:
            by_partition[partition].append(call)
            calls.append(call)
        if len(calls) == num_partitions * per_partition:
            break
    assert len(calls) == num_partitions * per_partition, "could not balance call ids"
    return calls, by_partition


def _config(topic, group, results_topic):
    return FunctionConfig(
        function_id="rebalancer",
        function_version="1.0.0",
        image="img",
        input_topic=topic,
        results_topic=results_topic,
        in_flight=4,
        per_file_timeout_seconds=120,
        retry_budget=1,
        commit_interval_seconds=0.0,
        rebalance_drain_seconds=DRAIN_SECONDS,
    )


def _publish(bootstrap_servers, topic, calls):
    producer = ConfluentProducer(
        {"bootstrap.servers": bootstrap_servers},
        num_partitions_by_topic={topic: 2},
    )
    for call in calls:
        producer.produce(
            OutboundRecord(
                topic=topic,
                key=call.encode(),
                partition_key=call.encode(),
                value=JsonCodec().encode_reference(reference(call_id=call)),
            )
        )
    assert producer.flush(30) == 0


def _build(bootstrap_servers, config, group):
    consumer = RecordingConsumer(
        config.consumer_config(bootstrap_servers, **{"group.id": group})
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


def _await_committed(bootstrap_servers, group, topic, *expected):
    """Wait for the ledger's commits to be visible at the coordinator, so the
    async commit cannot race the rebalance the test is about to start."""
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if all(
            _committed_offset(bootstrap_servers, group, topic, partition) == want
            for partition, want in enumerate(expected)
        ):
            return
        time.sleep(0.5)
    raise AssertionError(f"committed offsets never reached {expected}")


def test_in_flight_work_survives_a_cooperative_rebalance(
    bootstrap_servers, topic_factory, group_id
):
    topic = topic_factory(prefix="rebal", partitions=2)
    results_topic = topic_factory(prefix="rebal-results")

    calls, by_partition = _balanced_calls(2, PER_PARTITION)
    finished = {p: by_partition[p][0] for p in by_partition}
    unfinished = {p: by_partition[p][1] for p in by_partition}
    _publish(bootstrap_servers, topic, calls)

    config = _config(topic, group_id, results_topic)
    runner_a, pool_a = _build(bootstrap_servers, config, group_id)
    _dispatch_all(runner_a, len(calls))

    # Finish exactly the first call on each partition: both partitions carry
    # one committed call and one in-flight call into the rebalance.
    for partition in (0, 1):
        pool_a.succeed(finished[partition])
        runner_a.run_once()
    _await_committed(bootstrap_servers, group_id, topic, 1, 1)

    # B joins the group; cooperative-sticky moves one partition to it. The
    # join happens on B's first poll, and A's `_on_revoke` fires on A's.
    runner_b, pool_b = _build(bootstrap_servers, config, group_id)
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        runner_a.run_once()
        runner_b.run_once()
        if (
            len(runner_a.consumer.assignment()) == 1
            and len(runner_b.consumer.assignment()) == 1
            and runner_a.pool.cancelled
        ):
            break
    assert runner_a.pool.cancelled, "the rebalance never moved a partition"

    moved = runner_b.consumer.assignment()[0].partition
    kept = runner_a.consumer.assignment()[0].partition
    assert moved != kept

    # The drain ran, and it was a single-partition cooperative revoke: exactly
    # the in-flight call on the moved partition was cancelled, nothing else.
    assert len(runner_a.consumer.revoked) == 1
    (revoked,) = runner_a.consumer.revoked[0]
    assert revoked.partition == moved
    assert runner_a.pool.cancelled == [unfinished[moved]]
    assert [tp.partition for tp in runner_a.ledger.partitions] == [kept]
    assert len(pool_a.submitted) == len(calls), "A must not have redelivered anything to itself"

    # B is handed only the unfinished call: the finished one was committed and
    # a redelivery would be the data loss the low-water mark exists to prevent.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and not pool_b.submitted:
        runner_b.run_once()
    assert [job.call_id for job in pool_b.submitted] == [unfinished[moved]]

    # A finishes what stayed with it; B finishes what it got. Both commit.
    pool_a.succeed(unfinished[kept])
    runner_a.run_once()
    pool_b.succeed(unfinished[moved])
    runner_b.run_once()
    runner_a.close()
    runner_b.close()

    # Every call produced exactly one result: nothing lost, nothing duplicated.
    records = _read_all(bootstrap_servers, results_topic)
    results = [JsonCodec().decode_result(record.value()) for record in records]
    assert all(result.status is Status.SUCCESS for result in results)
    assert sorted(result.call_id for result in results) == sorted(calls)


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