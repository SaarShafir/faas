"""Low-water-mark commit ledger (spec §5.3).

With N files in flight they complete out of order. Committing the highest
completed offset silently drops every unfinished file below it on crash. So the
committable offset is the *lowest still-incomplete* offset -- everything below
it is durably done.

Kafka commits the next offset to consume, so:
  - if anything is pending, commit min(pending);
  - otherwise commit max(completed) + 1.

This yields at-least-once. Duplicates on restart are expected and fine:
results are idempotent on (call_id, function_id, function_version).
"""

from __future__ import annotations

from .models import TopicPartition

__all__ = ["OffsetLedger", "TopicPartition"]


class _PartitionLedger:
    __slots__ = ("_pending", "_high_completed", "_emitted")

    def __init__(self, first_offset: int):
        self._pending: set[int] = set()
        self._high_completed: int | None = None
        # Committing the first offset we ever saw is a no-op (it is already the
        # next offset to read), so treat it as the starting floor and suppress it.
        self._emitted: int = first_offset

    def start(self, offset: int) -> None:
        self._pending.add(offset)

    def complete(self, offset: int) -> None:
        self._pending.remove(offset)  # KeyError on an offset we never started
        if self._high_completed is None or offset > self._high_completed:
            self._high_completed = offset

    def committable(self) -> int | None:
        if self._pending:
            candidate = min(self._pending)
        elif self._high_completed is not None:
            candidate = self._high_completed + 1
        else:
            return None

        if candidate <= self._emitted:
            return None
        self._emitted = candidate
        return candidate

    @property
    def in_flight(self) -> int:
        return len(self._pending)


class OffsetLedger:
    """Per-partition low-water-mark tracking across the whole assignment."""

    def __init__(self) -> None:
        self._partitions: dict[TopicPartition, _PartitionLedger] = {}

    def start(self, tp: TopicPartition, offset: int) -> None:
        ledger = self._partitions.get(tp)
        if ledger is None:
            ledger = self._partitions[tp] = _PartitionLedger(first_offset=offset)
        ledger.start(offset)

    def complete(self, tp: TopicPartition, offset: int) -> None:
        ledger = self._partitions.get(tp)
        if ledger is None:
            # Partition was revoked while the work was in flight. The new owner
            # owns the offset now; dropping this is correct, not an error.
            return
        ledger.complete(offset)

    def drain_committable(self) -> list[tuple[TopicPartition, int]]:
        """Offsets safe to commit right now. Advances the ledger's floor."""
        out = []
        for tp, ledger in self._partitions.items():
            offset = ledger.committable()
            if offset is not None:
                out.append((tp, offset))
        return out

    def revoke(self, partitions) -> None:
        for tp in partitions:
            self._partitions.pop(_as_tp(tp), None)

    def in_flight_for(self, tp: TopicPartition) -> int:
        ledger = self._partitions.get(_as_tp(tp))
        return ledger.in_flight if ledger else 0

    @property
    def in_flight(self) -> int:
        return sum(ledger.in_flight for ledger in self._partitions.values())

    @property
    def partitions(self):
        return list(self._partitions)


def _as_tp(value) -> TopicPartition:
    if isinstance(value, TopicPartition):
        return value
    topic, partition = value
    return TopicPartition(topic, partition)
