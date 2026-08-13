"""confluent-kafka adapters (spec §11).

librdkafka is chosen for its rebalance handling, which is the thing that
actually matters here (§5.2). This module is the only place it appears; the
runner never imports it.

Note the offset convention: the SDK's ledger already yields the *next* offset to
consume, which is exactly what `commit` wants, so nothing is adjusted here.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence

from .models import InboundMessage, OutboundRecord, TopicPartition
from .partitioner import partition_for

log = logging.getLogger(__name__)


class ConfluentConsumer:
    def __init__(self, config: dict):
        from confluent_kafka import Consumer

        self._consumer = Consumer(config)
        # Counts evictions caused by a blocked poll loop. Non-zero means files
        # are being reprocessed; it belongs on a dashboard next to consumer lag.
        self.max_poll_exceeded = 0

    def subscribe(self, topics: Sequence[str], on_assign=None, on_revoke=None) -> None:
        from confluent_kafka import TopicPartition as KafkaTopicPartition  # noqa: F401

        def _assign(consumer, partitions):
            if on_assign:
                on_assign([TopicPartition(p.topic, p.partition) for p in partitions])
            # Cooperative-sticky: incremental_assign, never assign, or every
            # rebalance stops the world for partitions we already own.
            consumer.incremental_assign(partitions)

        def _revoke(consumer, partitions):
            if on_revoke:
                on_revoke([TopicPartition(p.topic, p.partition) for p in partitions])
            consumer.incremental_unassign(partitions)

        self._consumer.subscribe(list(topics), on_assign=_assign, on_revoke=_revoke)

    def poll(self, timeout: float) -> InboundMessage | None:
        message = self._consumer.poll(timeout)
        if message is None:
            return None
        if message.error():
            return self._handle_error(message.error())
        return InboundMessage(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            value=message.value(),
            key=message.key(),
            timestamp_ms=message.timestamp()[1],
            headers=tuple(message.headers() or ()),
        )

    def _handle_error(self, error) -> None:
        """Most librdkafka message errors are events, not failures.

        Raising on all of them -- which this used to do -- turns a recoverable
        eviction into a crashed pod that drops whatever was in flight.
        """
        from confluent_kafka import KafkaError

        code = error.code()

        if code == KafkaError._PARTITION_EOF:
            return None

        if code == KafkaError._MAX_POLL_EXCEEDED:
            # The §5.2 failure, caught in the act: the poll loop blocked for
            # longer than max.poll.interval.ms and the coordinator has evicted
            # us. librdkafka rejoins on the next poll and the uncommitted file
            # is redelivered, so this is survivable -- but it means work is
            # being reprocessed, and left alone it is the "forever" loop.
            # Loud, counted, and not fatal.
            self.max_poll_exceeded += 1
            log.error(
                "max.poll.interval.ms exceeded (%s) -- the poll loop blocked on work. "
                "In-flight files will be reprocessed. This is spec §5.2.",
                error,
            )
            return None

        if error.fatal():
            raise RuntimeError(f"fatal consumer error: {error}")

        log.warning("consumer error (continuing): %s", error)
        return None

    def commit(self, offsets: Sequence[tuple[TopicPartition, int]]) -> None:
        from confluent_kafka import TopicPartition as KafkaTopicPartition

        self._consumer.commit(
            offsets=[KafkaTopicPartition(tp.topic, tp.partition, offset) for tp, offset in offsets],
            asynchronous=True,
        )

    def pause(self, partitions: Iterable[TopicPartition]) -> None:
        self._consumer.pause(_to_kafka(partitions))

    def resume(self, partitions: Iterable[TopicPartition]) -> None:
        self._consumer.resume(_to_kafka(partitions))

    def assignment(self) -> list[TopicPartition]:
        return [TopicPartition(p.topic, p.partition) for p in self._consumer.assignment()]

    def close(self) -> None:
        self._consumer.close()


class ConfluentProducer:
    """Producer that honours the §6 key/partition split.

    The record key is composite, but the partition is computed from the
    partition key (`call_id`) with librdkafka's own murmur2 and passed
    explicitly -- so every result for a call is colocated for the aggregator.
    """

    def __init__(self, config: dict, *, num_partitions_by_topic: dict | None = None):
        from confluent_kafka import Producer

        self._producer = Producer(config)
        self._partitions = num_partitions_by_topic or {}

    def produce(self, record: OutboundRecord) -> None:
        kwargs = {
            "topic": record.topic,
            "key": record.key,
            "value": record.value,
            "headers": list(record.headers.items()) if record.headers else None,
        }
        num_partitions = self._partitions.get(record.topic)
        if record.partition_key and num_partitions:
            kwargs["partition"] = partition_for(record.partition_key, num_partitions)
        self._producer.produce(**kwargs)
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)


def _to_kafka(partitions):
    from confluent_kafka import TopicPartition as KafkaTopicPartition

    return [KafkaTopicPartition(p.topic, p.partition) for p in partitions]
