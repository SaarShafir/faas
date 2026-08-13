"""The sink is platform infra, not a function: env-driven, no §8 declaration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

DEFAULT_RESULTS_TOPIC = "faas.results"
DEFAULT_DLQ_TOPIC = "faas.dlq.sink"
DEFAULT_GROUP_ID = "faas.sink"


@dataclass(frozen=True)
class SinkConfig:
    results_topic: str = DEFAULT_RESULTS_TOPIC
    results_topic_partitions: int = 200
    dlq_topic: str = DEFAULT_DLQ_TOPIC
    group_id: str = DEFAULT_GROUP_ID
    db_path: str = "faas-sink.db"
    poll_timeout_seconds: float = 0.5
    commit_interval_seconds: float = 5.0

    @classmethod
    def from_env(cls) -> SinkConfig:
        return cls(
            results_topic=os.environ.get("FAAS_RESULTS_TOPIC", DEFAULT_RESULTS_TOPIC),
            results_topic_partitions=int(
                os.environ.get("FAAS_RESULTS_TOPIC_PARTITIONS", "200")
            ),
            dlq_topic=os.environ.get("FAAS_SINK_DLQ_TOPIC", DEFAULT_DLQ_TOPIC),
            group_id=os.environ.get("FAAS_SINK_GROUP_ID", DEFAULT_GROUP_ID),
            db_path=os.environ.get("FAAS_SINK_DB", "faas-sink.db"),
            poll_timeout_seconds=float(os.environ.get("FAAS_SINK_POLL_TIMEOUT", "0.5")),
            commit_interval_seconds=float(
                os.environ.get("FAAS_SINK_COMMIT_INTERVAL", "5.0")
            ),
        )

    def consumer_config(self, bootstrap_servers: str, **overrides) -> dict[str, Any]:
        """librdkafka settings for the sink.

        The same shape as FunctionConfig.consumer_config but without the
        headroom: a function's work is seconds-to-minutes of audio processing,
        the sink's is a microsecond SQLite write, so the default
        max.poll.interval.ms applies and there is nothing to protect it from.
        """
        config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 360_000,
            "session.timeout.ms": 45_000,
            "heartbeat.interval.ms": 3_000,
            "max.partition.fetch.bytes": 1_048_576,
            "partition.assignment.strategy": "cooperative-sticky",
        }
        config.update(overrides)
        return config

    def producer_config(self, bootstrap_servers: str, **overrides) -> dict[str, Any]:
        config = {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 20,
        }
        config.update(overrides)
        return config