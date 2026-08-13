"""The seams between the SDK and the outside world.

Everything the runner touches is one of these protocols, so the core logic --
poll/work decoupling, the ledger, retry and DLQ routing -- is testable without
a broker, an object store, or a subprocess. Adapters live in `faas_sdk.kafka`
and `faas_sdk.objectstore`.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import Protocol

from .models import InboundMessage, Job, JobOutcome, OutboundRecord, TopicPartition


class ConsumerPort(Protocol):
    def poll(self, timeout: float) -> InboundMessage | None:
        """Return one record, or None. Must always be cheap: it is what keeps
        the poll interval alive while work is outstanding."""

    def commit(self, offsets: Sequence[tuple[TopicPartition, int]]) -> None: ...

    def pause(self, partitions: Iterable[TopicPartition]) -> None: ...

    def resume(self, partitions: Iterable[TopicPartition]) -> None: ...

    def assignment(self) -> list[TopicPartition]: ...

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Callable | None = None,
        on_revoke: Callable | None = None,
    ) -> None: ...

    def close(self) -> None: ...


class ProducerPort(Protocol):
    def produce(self, record: OutboundRecord) -> None: ...

    def flush(self, timeout: float = 10.0) -> int: ...


class ObjectStorePort(Protocol):
    def get(self, key: str) -> bytes: ...

    def put(self, key: str, body: bytes, content_type: str = "") -> None: ...


class WorkerPool(Protocol):
    """The bounded in-flight pool (spec §5.2).

    Concurrency here is independent of partition count: 200 partitions x 20
    in-flight is 4,000 concurrent files with no repartitioning.
    """

    max_in_flight: int

    def submit(self, job: Job) -> None: ...

    def poll_completed(self) -> list[JobOutcome]:
        """Non-blocking drain of finished work."""

    def in_flight(self) -> int: ...

    def cancel(self, job_id: str) -> bool:
        """Best-effort. Returns True if the job will not produce an outcome."""

    def close(self, timeout: float = 30.0) -> None: ...


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def now(self): ...

    def sleep(self, seconds: float) -> None: ...
