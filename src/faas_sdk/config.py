"""Function declaration (spec §8): config as code, one PR, zero infra tickets."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_INPUT_TOPIC = "faas.audio.internal"
DEFAULT_RESULTS_TOPIC = "faas.results"
DLQ_TOPIC_PREFIX = "faas.dlq."

# The onboarding floor from spec §8. Not enforced here -- it is a capacity
# review gate -- but the SDK reports throughput-vs-realtime so it is measurable.
REALTIME_SPEEDUP_FLOOR = 25


@dataclass(frozen=True)
class Resources:
    cpu: float | None = None
    memory: str | None = None
    gpu: int | None = None


@dataclass(frozen=True)
class FunctionConfig:
    function_id: str
    function_version: str
    image: str

    input_topic: str = DEFAULT_INPUT_TOPIC
    results_topic: str = DEFAULT_RESULTS_TOPIC
    results_topic_partitions: int = 200
    dlq_topic: str = ""

    # Spec §5.2: keep this small. The preferred default is one consumer per pod
    # with shallow in-flight depth, and the autoscaler scales pod count -- the
    # failure unit is then a pod and commit reasoning stays simple.
    in_flight: int = 1
    per_file_timeout_seconds: float = 300.0
    retry_budget: int = 3
    retry_backoff_seconds: float = 2.0
    retry_backoff_max_seconds: float = 300.0

    poll_timeout_seconds: float = 0.5
    commit_interval_seconds: float = 5.0
    rebalance_drain_seconds: float = 30.0

    payload_schema: str = ""
    resources: Resources = field(default_factory=Resources)

    def __post_init__(self):
        if not self.function_id:
            raise ValueError("function_id is required")
        if not self.function_version:
            raise ValueError("function_version is required")
        if self.in_flight < 1:
            raise ValueError("in_flight must be >= 1")
        if self.retry_budget < 1:
            raise ValueError("retry_budget must be >= 1")
        if self.per_file_timeout_seconds <= 0:
            raise ValueError("per_file_timeout_seconds must be > 0")
        if not self.dlq_topic:
            object.__setattr__(self, "dlq_topic", DLQ_TOPIC_PREFIX + self.function_id)

    @property
    def group_id(self) -> str:
        """Spec §4.3. One consumer group per (function, version): independent
        offsets, lag, scaling and DLQ, and shadow deploys for free."""
        return f"{self.function_id}:{self.function_version}"

    @classmethod
    def from_yaml(cls, path) -> FunctionConfig:
        import yaml

        data = yaml.safe_load(Path(path).read_text()) or {}
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FunctionConfig:
        for required in ("function_id", "function_version", "image"):
            if not data.get(required):
                raise ValueError(f"{required} is required in the function declaration")

        resources = data.get("resources") or {}
        known = {f for f in cls.__dataclass_fields__ if f != "resources"}
        unknown = set(data) - known - {"resources"}
        if unknown:
            raise ValueError(f"unknown keys in function declaration: {sorted(unknown)}")

        kwargs = {k: v for k, v in data.items() if k in known}
        kwargs["function_version"] = str(data["function_version"])
        return cls(
            resources=Resources(
                cpu=resources.get("cpu"),
                memory=resources.get("memory"),
                gpu=resources.get("gpu"),
            ),
            **kwargs,
        )

    def consumer_config(self, bootstrap_servers: str, **overrides) -> dict[str, Any]:
        """librdkafka settings the SDK owns.

        Auto-commit is off because the ledger owns commits (§5.3), and the poll
        interval must cover a full pool of files each burning the whole per-file
        timeout -- otherwise the eviction/rebalance/reprocess loop from §5.2 is
        back, just at a longer period.
        """
        worst_case_ms = int(self.per_file_timeout_seconds * self.in_flight * 1000)
        config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": max(worst_case_ms, 300_000) + 60_000,
            "session.timeout.ms": 45_000,
            "heartbeat.interval.ms": 3_000,
            # Backpressure is pause/resume, so a deep prefetch buys nothing and
            # costs memory per partition.
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
