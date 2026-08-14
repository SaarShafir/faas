"""ffmpeg transcode to canonical FLAC (spec §4.1).

Input formats are a zoo; ffmpeg handles that so the SDK only ever decodes
canonical FLAC via libsndfile. What is worth testing without ffmpeg installed
is everything around the call: the flags that define "canonical", the flag that
keeps metadata out of the FLAC tags, and the mapping from ffmpeg failure to
retry-or-DLQ.
"""

import shutil
import subprocess

import pytest

from faas_hydrator.testing import FakeFfmpeg, flac_bytes
from faas_hydrator.transcode import (
    CANONICAL_BITS,
    CANONICAL_CHANNELS,
    CANONICAL_SAMPLE_RATE,
    Transcoder,
)
from faas_sdk.errors import PoisonMessageError, TransientError


@pytest.fixture
def ffmpeg():
    return FakeFfmpeg(output=flac_bytes(total_samples=16000 * 300))


@pytest.fixture
def transcoder(ffmpeg):
    return Transcoder(run=ffmpeg)


def test_produces_flac_and_reports_what_it_actually_made(transcoder):
    audio = transcoder.to_canonical_flac(b"any old input")

    assert audio.sample_rate == CANONICAL_SAMPLE_RATE
    assert audio.channels == CANONICAL_CHANNELS
    assert audio.bits_per_sample == CANONICAL_BITS
    assert audio.duration_seconds == 300.0
    assert audio.data.startswith(b"fLaC")


def test_canonical_form_is_on_the_command_line(transcoder, ffmpeg):
    transcoder.to_canonical_flac(b"input")
    argv = ffmpeg.argv

    assert _flag(argv, "-ar") == str(CANONICAL_SAMPLE_RATE)
    assert _flag(argv, "-ac") == str(CANONICAL_CHANNELS)
    assert _flag(argv, "-sample_fmt") == "s16"
    assert _flag(argv, "-f") == "flac"


def test_metadata_is_stripped_from_the_flac(transcoder, ffmpeg):
    """Spec §4.1: metadata lives in the Kafka reference only, so the schema can
    evolve without rewriting objects. Enforced by a flag, not by hoping the
    input had no tags."""
    transcoder.to_canonical_flac(b"input")
    assert _flag(ffmpeg.argv, "-map_metadata") == "-1"


def test_only_the_first_audio_stream_is_taken(transcoder, ffmpeg):
    """The zoo includes multi-stream containers and cover art."""
    transcoder.to_canonical_flac(b"input")
    assert _flag(ffmpeg.argv, "-map") == "0:a:0"
    assert "-vn" in ffmpeg.argv


@pytest.mark.parametrize("level", [0, 1, 2, 3])
def test_compression_level_is_configurable_within_the_cheap_range(level, ffmpeg):
    Transcoder(run=ffmpeg, compression_level=level).to_canonical_flac(b"input")
    assert _flag(ffmpeg.argv, "-compression_level") == str(level)


@pytest.mark.parametrize("level", [-1, 4, 8, 12])
def test_expensive_compression_levels_are_refused(level, ffmpeg):
    """Spec §4.1: decode speed is compression-level-independent, so levels above
    3 buy a few percent of size for real encode CPU. Do not pay it."""
    with pytest.raises(ValueError, match="compression_level"):
        Transcoder(run=ffmpeg, compression_level=level)


def test_stdin_is_closed_so_ffmpeg_cannot_block_on_a_prompt(transcoder, ffmpeg):
    transcoder.to_canonical_flac(b"input")
    assert "-nostdin" in ffmpeg.argv


def test_input_goes_through_a_file_not_a_pipe(transcoder, ffmpeg):
    """Piping breaks any format needing a seek to read its header -- MP4's moov
    atom being the obvious one -- and the input formats are a zoo."""
    transcoder.to_canonical_flac(b"input")
    assert ffmpeg.input_path is not None
    assert ffmpeg.stdin_bytes is None


def test_output_goes_through_a_file_so_the_header_is_complete(transcoder, ffmpeg):
    """A pipe-written FLAC cannot be seeked back to, so ffmpeg leaves
    total_samples at zero and every reference would claim 0 seconds."""
    transcoder.to_canonical_flac(b"input")
    assert ffmpeg.output_path is not None
    assert "pipe:1" not in ffmpeg.argv


def test_scratch_files_are_cleaned_up(transcoder, ffmpeg):
    import os

    transcoder.to_canonical_flac(b"input")

    assert not os.path.exists(ffmpeg.input_path)
    assert not os.path.exists(ffmpeg.output_path)


def test_a_flac_with_unknown_duration_is_rejected(ffmpeg):
    """Rather than publishing a reference that lies about duration."""
    ffmpeg.output = flac_bytes(total_samples=0)
    with pytest.raises(TransientError, match="duration"):
        Transcoder(run=ffmpeg).to_canonical_flac(b"input")


def test_a_non_canonical_result_is_rejected(ffmpeg):
    """Belt and braces: the whole SDK assumes 16 kHz mono, so a silent ffmpeg
    behaviour change must not reach the topic."""
    ffmpeg.output = flac_bytes(sample_rate=44100, channels=2)
    with pytest.raises(TransientError, match="canonical"):
        Transcoder(run=ffmpeg).to_canonical_flac(b"input")


def test_ffmpeg_failure_is_poison_not_a_retry(ffmpeg):
    """A file ffmpeg cannot decode will not decode on the third attempt either.
    Retrying it just delays the DLQ and burns Audio API quota."""
    ffmpeg.returncode = 1
    ffmpeg.stderr = b"Invalid data found when processing input"

    with pytest.raises(PoisonMessageError) as excinfo:
        Transcoder(run=ffmpeg).to_canonical_flac(b"not audio")
    assert excinfo.value.retryable is False
    assert "Invalid data" in str(excinfo.value)


def test_ffmpeg_timeout_is_retryable(ffmpeg):
    ffmpeg.raises = subprocess.TimeoutExpired(cmd="ffmpeg", timeout=1)

    with pytest.raises(TransientError) as excinfo:
        Transcoder(run=ffmpeg).to_canonical_flac(b"input")
    assert excinfo.value.retryable is True


def test_a_missing_ffmpeg_binary_is_not_a_message_failure(ffmpeg):
    """DLQ-ing live traffic because the image is broken would be the wrong
    response: every message would fail identically. Crash the pod instead."""
    ffmpeg.raises = FileNotFoundError("ffmpeg")

    with pytest.raises(RuntimeError, match="ffmpeg"):
        Transcoder(run=ffmpeg).to_canonical_flac(b"input")


def test_empty_output_is_rejected(ffmpeg):
    ffmpeg.output = b""
    with pytest.raises(TransientError):
        Transcoder(run=ffmpeg).to_canonical_flac(b"input")


needs_ffmpeg = pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="ffmpeg not installed")


def _wav(seconds=2, rate=44100, layout="stereo", metadata=None):
    argv = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        f"anullsrc=r={rate}:cl={layout}",
        "-t",
        str(seconds),
    ]
    for key, value in (metadata or {}).items():
        argv += ["-metadata", f"{key}={value}"]
    argv += ["-f", "wav", "pipe:1"]
    return subprocess.run(argv, capture_output=True, check=True).stdout


@needs_ffmpeg
def test_real_ffmpeg_round_trip():
    """The test that proves the flags mean what the fake assumes they mean."""
    audio = Transcoder().to_canonical_flac(_wav(seconds=2, rate=44100, layout="stereo"))

    assert audio.sample_rate == CANONICAL_SAMPLE_RATE
    assert audio.channels == CANONICAL_CHANNELS
    assert audio.bits_per_sample == CANONICAL_BITS
    assert audio.duration_seconds == pytest.approx(2.0, abs=0.05)


@needs_ffmpeg
def test_real_ffmpeg_strips_metadata_from_the_object():
    """Spec §4.1, proven rather than asserted.

    Checking that -map_metadata is on the command line only tests our own argv.
    This checks the bytes: a tag present in the input must not survive into the
    object, because metadata lives in the Kafka reference and nowhere else.
    """
    tagged = _wav(metadata={"title": "SECRET-TENANT-NAME", "artist": "acme-corp"})

    audio = Transcoder().to_canonical_flac(tagged)

    assert b"SECRET-TENANT-NAME" not in audio.data
    assert b"acme-corp" not in audio.data


@needs_ffmpeg
def test_real_ffmpeg_output_carries_a_usable_duration():
    """The non-seekable-output bug, checked against the real encoder: a
    pipe-written FLAC would leave total_samples at zero here."""
    audio = Transcoder().to_canonical_flac(_wav(seconds=3))

    assert audio.duration_seconds == pytest.approx(3.0, abs=0.05)


@needs_ffmpeg
@pytest.mark.parametrize("level", [0, 3])
def test_real_ffmpeg_accepts_the_permitted_compression_levels(level):
    audio = Transcoder(compression_level=level).to_canonical_flac(_wav(seconds=1))
    assert audio.duration_seconds == pytest.approx(1.0, abs=0.05)


@needs_ffmpeg
def test_real_ffmpeg_rejects_input_that_is_not_audio():
    """The poison path, end to end: DLQ on the first attempt, no retries."""
    with pytest.raises(PoisonMessageError):
        Transcoder().to_canonical_flac(b"this is not audio, it is a text file")


def _flag(argv, name):
    return argv[argv.index(name) + 1]
