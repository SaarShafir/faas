"""FLAC STREAMINFO parsing.

The hydrator publishes sample_rate, channels and duration in the reference
(§4.2), and every downstream function trusts those numbers without re-deriving
them. Reading them out of the bytes the Audio API served -- rather than assuming
canonical form because that is what upstream promises -- is what makes the
reference true.

It is also how a header with no duration is caught: an encoder writing FLAC to
a pipe cannot seek back to fill in total_samples, so it leaves zero, and the
reference would claim every such call is 0 seconds long.
"""

import pytest

from faas_hydrator.flac import MalformedFlacError, read_streaminfo
from faas_hydrator.testing import flac_bytes


def test_reads_canonical_parameters():
    info = read_streaminfo(
        flac_bytes(sample_rate=16000, channels=1, bits=16, total_samples=4800000)
    )

    assert info.sample_rate == 16000
    assert info.channels == 1
    assert info.bits_per_sample == 16
    assert info.total_samples == 4800000


def test_derives_duration():
    info = read_streaminfo(flac_bytes(sample_rate=16000, total_samples=16000 * 300))
    assert info.duration_seconds == 300.0


def test_reads_non_canonical_parameters_faithfully():
    """The parser reports what is there; deciding whether that is canonical is
    the hydrator's job, and conflating the two hides real mismatches."""
    info = read_streaminfo(flac_bytes(sample_rate=44100, channels=2, bits=24))

    assert info.sample_rate == 44100
    assert info.channels == 2
    assert info.bits_per_sample == 24


def test_unknown_total_samples_is_flagged():
    """Zero means "unknown" in the format -- exactly what a pipe-written FLAC
    contains. It must not be mistaken for a zero-length recording."""
    info = read_streaminfo(flac_bytes(total_samples=0))

    assert info.total_samples == 0
    assert info.duration_known is False
    assert info.duration_seconds == 0.0


def test_known_duration_is_flagged():
    assert read_streaminfo(flac_bytes(total_samples=16000)).duration_known is True


def test_maximum_total_samples_fits():
    """total_samples is 36 bits; a naive 32-bit read truncates long files."""
    biggest = (1 << 36) - 1
    assert read_streaminfo(flac_bytes(total_samples=biggest)).total_samples == biggest


@pytest.mark.parametrize(
    "data,reason",
    [
        (b"", "empty"),
        (b"RIFF....WAVE", "wrong magic"),
        (b"fLaC", "truncated before the metadata header"),
        (b"fLaC\x00\x00\x00\x22" + b"\x00" * 10, "truncated streaminfo"),
    ],
)
def test_malformed_input_is_rejected(data, reason):
    with pytest.raises(MalformedFlacError):
        read_streaminfo(data)


def test_streaminfo_must_be_the_first_metadata_block():
    """Per the format it always is. If it is not, we are not looking at what we
    think we are looking at."""
    data = bytearray(flac_bytes())
    data[4] = 0x04  # VORBIS_COMMENT instead of STREAMINFO
    with pytest.raises(MalformedFlacError, match="STREAMINFO"):
        read_streaminfo(bytes(data))
