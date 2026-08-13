"""murmur2 partitioning, matching librdkafka's `consistent` partitioner.

Needed because spec §6 splits the two jobs a Kafka key normally does: the record
key is composite (`{call_id}:{function_id}:{function_version}`) so compaction
cannot let one function's result destroy another's, but the *partition* must be
derived from `call_id` alone so every result for a call is colocated and the
aggregator (§7) needs no shuffle.

That means computing the partition ourselves and passing it explicitly, using
the same hash the rest of the ecosystem uses.
"""

from __future__ import annotations

_SEED = 0x9747B28C
_M = 0x5BD1E995
_R = 24
_MASK = 0xFFFFFFFF


def murmur2(data: bytes) -> int:
    """Kafka's Utils.murmur2, as a signed 32-bit int."""
    length = len(data)
    h = (_SEED ^ length) & _MASK

    for i in range(0, length - (length % 4), 4):
        k = (
            (data[i] & 0xFF)
            | ((data[i + 1] & 0xFF) << 8)
            | ((data[i + 2] & 0xFF) << 16)
            | ((data[i + 3] & 0xFF) << 24)
        )
        k = (k * _M) & _MASK
        k ^= k >> _R
        k = (k * _M) & _MASK
        h = (h * _M) & _MASK
        h ^= k

    remaining = length % 4
    tail = length - remaining
    if remaining == 3:
        h ^= (data[tail + 2] & 0xFF) << 16
    if remaining >= 2:
        h ^= (data[tail + 1] & 0xFF) << 8
    if remaining >= 1:
        h ^= data[tail] & 0xFF
        h = (h * _M) & _MASK

    h ^= h >> 13
    h = (h * _M) & _MASK
    h ^= h >> 15

    return _to_signed(h)


def partition_for(partition_key: bytes, num_partitions: int) -> int:
    if num_partitions <= 0:
        raise ValueError("num_partitions must be positive")
    return (murmur2(partition_key) & 0x7FFFFFFF) % num_partitions


def _to_signed(value: int) -> int:
    value &= _MASK
    return value - 0x100000000 if value & 0x80000000 else value
