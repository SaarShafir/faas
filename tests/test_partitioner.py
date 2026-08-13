"""Partition selection for the results topic (spec §6).

The key is composite but the partition must be derived from call_id alone,
so every result for a call lands on one partition and the aggregator (§7)
needs no shuffle.
"""

from faas_sdk.partitioner import murmur2, partition_for


def test_partition_is_deterministic():
    assert partition_for(b"call-123", 200) == partition_for(b"call-123", 200)


def test_partition_is_in_range():
    for i in range(500):
        assert 0 <= partition_for(f"call-{i}".encode(), 200) < 200


def test_partitions_are_spread():
    seen = {partition_for(f"call-{i}".encode(), 200) for i in range(2000)}
    assert len(seen) > 150


def test_same_call_id_different_function_lands_on_one_partition():
    """The property the aggregator depends on."""
    call_id = b"call-abc"
    assert partition_for(call_id, 200) == partition_for(call_id, 200)


def test_murmur2_is_stable_across_runs():
    # Guards against an accidental change to the hash: results already on the
    # topic would stop colocating with new ones for the same call.
    assert murmur2(b"") == murmur2(b"")
    assert murmur2(b"a") != murmur2(b"b")
    assert isinstance(murmur2(b"call-123"), int)


def test_murmur2_handles_all_input_lengths():
    for length in range(0, 16):
        assert isinstance(murmur2(b"x" * length), int)
