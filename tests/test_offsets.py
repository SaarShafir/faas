"""Low-water-mark commit ledger (spec §5.3).

The invariant under test: we never commit an offset that would skip a file
still in flight. With N files in flight completing out of order, the committable
offset is the *lowest* incomplete offset -- not the highest completed one.
"""

import pytest

from faas_sdk.offsets import OffsetLedger, TopicPartition

TP = TopicPartition("faas.audio.internal", 0)
TP1 = TopicPartition("faas.audio.internal", 1)


def test_idle_ledger_has_nothing_to_commit():
    ledger = OffsetLedger()
    assert ledger.drain_committable() == []


def test_first_message_in_flight_commits_nothing():
    ledger = OffsetLedger()
    ledger.start(TP, 10)
    # Committing 10 would be a no-op (10 is the next offset to read anyway),
    # so the ledger suppresses it rather than emitting commit churn.
    assert ledger.drain_committable() == []


def test_completion_advances_past_the_completed_offset():
    ledger = OffsetLedger()
    ledger.start(TP, 10)
    ledger.complete(TP, 10)
    # Kafka commits the *next* offset to consume.
    assert ledger.drain_committable() == [(TP, 11)]


def test_out_of_order_completion_holds_at_the_low_water_mark():
    ledger = OffsetLedger()
    for offset in (10, 11, 12):
        ledger.start(TP, offset)

    ledger.complete(TP, 12)
    # 10 and 11 are still in flight: committing 13 here would silently skip
    # them on crash. This is the whole point of the ledger.
    assert ledger.drain_committable() == []

    ledger.complete(TP, 10)
    assert ledger.drain_committable() == [(TP, 11)]

    ledger.complete(TP, 11)
    assert ledger.drain_committable() == [(TP, 13)]


def test_ledger_never_regresses():
    ledger = OffsetLedger()
    for offset in (10, 11):
        ledger.start(TP, offset)
    ledger.complete(TP, 10)
    ledger.complete(TP, 11)
    assert ledger.drain_committable() == [(TP, 12)]

    # A late duplicate completion must not walk the committed offset backwards.
    ledger.start(TP, 12)
    ledger.complete(TP, 12)
    assert ledger.drain_committable() == [(TP, 13)]
    assert ledger.drain_committable() == []


def test_gaps_in_the_offset_sequence_are_not_treated_as_pending():
    """Compaction or transactional markers leave holes; they are not our files."""
    ledger = OffsetLedger()
    ledger.start(TP, 10)
    ledger.start(TP, 15)
    ledger.complete(TP, 10)
    assert ledger.drain_committable() == [(TP, 15)]


def test_partitions_are_tracked_independently():
    ledger = OffsetLedger()
    ledger.start(TP, 10)
    ledger.start(TP1, 100)
    ledger.complete(TP1, 100)

    assert ledger.drain_committable() == [(TP1, 101)]
    assert ledger.in_flight == 1


def test_drain_is_idempotent_between_completions():
    ledger = OffsetLedger()
    ledger.start(TP, 10)
    ledger.complete(TP, 10)
    assert ledger.drain_committable() == [(TP, 11)]
    assert ledger.drain_committable() == []


def test_revoke_drops_partition_state():
    ledger = OffsetLedger()
    ledger.start(TP, 10)
    ledger.start(TP1, 100)
    ledger.revoke([TP])

    assert ledger.in_flight == 1
    # A completion for a revoked partition is a no-op, not a crash: the work
    # was already handed to whoever picked the partition up.
    ledger.complete(TP, 10)
    assert ledger.drain_committable() == []


def test_completing_an_unstarted_offset_is_a_programming_error():
    ledger = OffsetLedger()
    ledger.start(TP, 10)
    with pytest.raises(KeyError):
        ledger.complete(TP, 11)


def test_in_flight_depth_reflects_started_minus_completed():
    ledger = OffsetLedger()
    assert ledger.in_flight == 0
    ledger.start(TP, 10)
    ledger.start(TP, 11)
    assert ledger.in_flight == 2
    ledger.complete(TP, 10)
    assert ledger.in_flight == 1
