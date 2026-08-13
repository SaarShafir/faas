"""Our murmur2 against librdkafka's (spec §6).

`tests/test_partitioner.py` can only check determinism, range and spread. None
of those would catch a *wrong* hash -- a subtly broken murmur2 is perfectly
deterministic and evenly spread. The claim `partitioner.py` actually makes is
"this is the same hash the rest of the ecosystem uses", and the only way to
check that is to ask the ecosystem.

Why it matters: §6 splits the record key (composite) from the partition key
(`call_id` alone) so every result for a call lands on one partition and the
aggregator needs no shuffle. If our hash disagreed with librdkafka's, results
written by anything using the default partitioner would scatter, and the
aggregator would silently under-count.
"""

from __future__ import annotations

import uuid

import pytest

from faas_sdk.partitioner import partition_for

pytestmark = pytest.mark.kafka

PARTITIONS = 32
KEYS = [f"call-{uuid.uuid4().hex[:10]}".encode() for _ in range(64)] + [
    b"c1",
    b"a",
    b"",
    b"call-with-a-much-longer-identifier-than-usual",
    bytes(range(32)),
]


def _produce_with_librdkafka(bootstrap_servers, topic, keys):
    """Let librdkafka choose the partition, using its own implementation.

    `murmur2`, not `consistent`. This is a genuine trap: librdkafka's
    `consistent` partitioner is CRC32, and only `murmur2`/`murmur2_random` are
    the Java-Producer-compatible ones. Since the ecosystem this has to interop
    with is Java (Strimzi, Kafka Streams, anything using the default Java
    partitioner), murmur2 is the target, and comparing against `consistent`
    would fail against a perfectly correct implementation.
    """
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers, "partitioner": "murmur2"})
    landed = {}

    def record(err, msg):
        assert err is None, err
        landed[msg.key()] = msg.partition()

    for key in keys:
        producer.produce(topic, key=key, value=b"x", on_delivery=record)
    producer.flush(30)
    return landed


def test_our_partition_matches_librdkafkas_for_every_key(bootstrap_servers, topic_factory):
    topic = topic_factory(partitions=PARTITIONS, prefix="partitioner")

    librdkafka = _produce_with_librdkafka(bootstrap_servers, topic, KEYS)
    assert len(librdkafka) == len(KEYS), "not every message was delivered"

    ours = {key: partition_for(key, PARTITIONS) for key in KEYS}

    mismatches = {
        key: (ours[key], librdkafka[key]) for key in KEYS if ours[key] != librdkafka[key]
    }
    assert mismatches == {}, f"hash disagrees with librdkafka for {len(mismatches)} keys"


def test_the_comparison_would_notice_a_wrong_hash(bootstrap_servers, topic_factory):
    """Negative control.

    If librdkafka's placement were uniform-random rather than a hash we match,
    the test above would still pass roughly 1/32 of the time per key. Perturbing
    our hash must produce disagreement -- otherwise the check above is vacuous.
    """
    topic = topic_factory(partitions=PARTITIONS, prefix="partitioner-control")
    librdkafka = _produce_with_librdkafka(bootstrap_servers, topic, KEYS)

    wrong = {key: partition_for(key + b"\x00", PARTITIONS) for key in KEYS}
    disagreements = sum(1 for key in KEYS if wrong[key] != librdkafka[key])

    assert disagreements > len(KEYS) / 2


def test_the_composite_key_and_the_partition_key_disagree_on_purpose(
    bootstrap_servers, topic_factory
):
    """The §6 arrangement, end to end: two function versions writing results for
    one call use different record keys but must share a partition."""
    from faas_sdk.kafka import ConfluentProducer
    from faas_sdk.models import OutboundRecord

    topic = topic_factory(partitions=PARTITIONS, prefix="composite")
    producer = ConfluentProducer(
        {"bootstrap.servers": bootstrap_servers}, num_partitions_by_topic={topic: PARTITIONS}
    )

    call_id = b"call-shared"
    for version in ("1.0.0", "2.0.0"):
        producer.produce(
            OutboundRecord(
                topic=topic,
                key=b"call-shared:diarization:" + version.encode(),
                value=b"{}",
                partition_key=call_id,
            )
        )
    producer.flush(30)

    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"reader-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    seen = []
    for _ in range(200):
        message = consumer.poll(0.5)
        if message is not None and not message.error():
            seen.append((message.key(), message.partition()))
            if len(seen) == 2:
                break
    consumer.close()

    assert len(seen) == 2
    keys = {key for key, _ in seen}
    partitions = {partition for _, partition in seen}
    assert len(keys) == 2, "the record keys must differ, or compaction loses one"
    assert partitions == {partition_for(call_id, PARTITIONS)}, "both must share a partition"
