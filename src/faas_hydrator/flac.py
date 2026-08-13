"""Reading FLAC STREAMINFO.

The reference (§4.2) states sample rate, channels and duration, and every
function downstream trusts those without re-deriving them. So they are read out
of the bytes ffmpeg actually produced rather than echoed back from the flags we
passed -- the two disagree exactly when something has gone wrong.

Layout, from the FLAC format spec:

    "fLaC"                     4 bytes
    METADATA_BLOCK_HEADER      4 bytes   (1 bit last-block, 7 bits type, 24 bits length)
    STREAMINFO                34 bytes   (must be the first block)
        min/max block size     4 bytes
        min/max frame size     6 bytes
        sample rate           20 bits
        channels - 1           3 bits
        bits per sample - 1    5 bits
        total samples         36 bits
        MD5                   16 bytes
"""

from __future__ import annotations

from dataclasses import dataclass

MAGIC = b"fLaC"
STREAMINFO_BLOCK_TYPE = 0
STREAMINFO_LENGTH = 34
_PACKED_OFFSET = 4 + 4 + 10  # magic + block header + block/frame sizes
_PACKED_LENGTH = 8


class MalformedFlacError(ValueError):
    """Not a FLAC stream, or not one we can read the header of."""


@dataclass(frozen=True)
class StreamInfo:
    sample_rate: int
    channels: int
    bits_per_sample: int
    total_samples: int

    @property
    def duration_known(self) -> bool:
        # Zero means "unknown" in the format, not "empty". It is what ffmpeg
        # leaves behind when it cannot seek back to patch the header -- writing
        # to a pipe, for instance.
        return self.total_samples > 0

    @property
    def duration_seconds(self) -> float:
        if not self.duration_known or not self.sample_rate:
            return 0.0
        return self.total_samples / self.sample_rate


def read_streaminfo(data: bytes) -> StreamInfo:
    if len(data) < 4 or not data.startswith(MAGIC):
        raise MalformedFlacError("missing fLaC magic")
    if len(data) < _PACKED_OFFSET + _PACKED_LENGTH:
        raise MalformedFlacError("truncated before end of STREAMINFO")

    block_type = data[4] & 0x7F
    if block_type != STREAMINFO_BLOCK_TYPE:
        raise MalformedFlacError(f"first metadata block is type {block_type}, not STREAMINFO")

    length = int.from_bytes(data[5:8], "big")
    if length != STREAMINFO_LENGTH:
        raise MalformedFlacError(f"STREAMINFO is {length} bytes, expected {STREAMINFO_LENGTH}")

    packed = int.from_bytes(data[_PACKED_OFFSET : _PACKED_OFFSET + _PACKED_LENGTH], "big")
    return StreamInfo(
        sample_rate=packed >> 44,
        channels=((packed >> 41) & 0b111) + 1,
        bits_per_sample=((packed >> 36) & 0b11111) + 1,
        total_samples=packed & ((1 << 36) - 1),
    )
