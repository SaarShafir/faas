"""ffmpeg -> canonical FLAC (spec §4.1).

Canonical is 16 kHz, 16-bit, mono, compression level 0-3. Decode speed is
compression-level-independent, so higher levels only cost encode CPU for a few
percent of size -- the spec says do not pay it, and levels above 3 are refused.

Both ends go through files rather than pipes, deliberately:
  - input, because any format needing a seek to read its header (MP4's moov
    atom is the classic) cannot be piped, and the input formats are a zoo;
  - output, because ffmpeg cannot seek back to patch total_samples into the
    STREAMINFO header of a non-seekable stream, and the reference would then
    claim every call is 0 seconds long.

Scratch files land in the system temp dir. On OpenShift that must be an
emptyDir writable by GID 0 -- see the random-UID gotcha in §11.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from dataclasses import dataclass

from faas_sdk.errors import PoisonMessageError, TransientError

from .flac import MalformedFlacError, read_streaminfo

CANONICAL_SAMPLE_RATE = 16000
CANONICAL_CHANNELS = 1
CANONICAL_BITS = 16
MAX_COMPRESSION_LEVEL = 3


@dataclass(frozen=True)
class FlacAudio:
    data: bytes
    sample_rate: int
    channels: int
    bits_per_sample: int
    duration_seconds: float


class Transcoder:
    def __init__(
        self,
        *,
        ffmpeg: str = "ffmpeg",
        compression_level: int = 0,
        timeout_seconds: float = 120.0,
        run=subprocess.run,
    ):
        if not 0 <= compression_level <= MAX_COMPRESSION_LEVEL:
            raise ValueError(
                f"compression_level must be 0-{MAX_COMPRESSION_LEVEL} (§4.1): "
                f"decode speed does not improve above it, so higher levels are "
                f"encode CPU spent for a few percent of size"
            )
        self.ffmpeg = ffmpeg
        self.compression_level = compression_level
        self.timeout_seconds = timeout_seconds
        self._run = run

    def to_canonical_flac(self, raw: bytes) -> FlacAudio:
        with tempfile.TemporaryDirectory(prefix="faas-hydrate-") as scratch:
            in_path = os.path.join(scratch, "input")
            out_path = os.path.join(scratch, "output.flac")
            with open(in_path, "wb") as handle:
                handle.write(raw)

            self._invoke(self.argv(in_path, out_path))

            try:
                with open(out_path, "rb") as handle:
                    data = handle.read()
            except OSError as exc:
                raise TransientError(
                    f"ffmpeg exited cleanly but produced no output: {exc}",
                    code="TRANSCODE_NO_OUTPUT",
                ) from exc

        return self._verify(data)

    def argv(self, in_path: str, out_path: str) -> list:
        return [
            self.ffmpeg,
            "-hide_banner",
            "-nostdin",  # never block waiting on a console prompt
            "-loglevel",
            "error",
            "-y",
            "-i",
            in_path,
            "-map",
            "0:a:0",  # first audio stream only; the zoo includes multi-stream files
            "-vn",  # and cover art
            "-ac",
            str(CANONICAL_CHANNELS),
            "-ar",
            str(CANONICAL_SAMPLE_RATE),
            "-sample_fmt",
            "s16",
            "-compression_level",
            str(self.compression_level),
            # §4.1: metadata lives in the Kafka reference only. Stripping it
            # here is what keeps that true regardless of what the input carried.
            "-map_metadata",
            "-1",
            "-f",
            "flac",
            out_path,
        ]

    def _invoke(self, argv: list) -> None:
        try:
            completed = self._run(argv, capture_output=True, timeout=self.timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            raise TransientError(
                f"ffmpeg timed out after {self.timeout_seconds}s",
                code="TRANSCODE_TIMEOUT",
            ) from exc
        except FileNotFoundError as exc:
            # Not a message failure: every message would fail identically, so
            # DLQ-ing live traffic would be the wrong response. Let the pod die.
            raise RuntimeError(
                f"ffmpeg binary not found at {self.ffmpeg!r}; the image is broken"
            ) from exc

        if completed.returncode != 0:
            stderr = (completed.stderr or b"").decode("utf-8", "replace").strip()
            # A file ffmpeg cannot decode will not decode on the third attempt
            # either; retrying only delays the DLQ and burns Audio API quota.
            raise PoisonMessageError(
                f"ffmpeg failed (exit {completed.returncode}): {stderr}",
                code="TRANSCODE_FAILED",
            )

    def _verify(self, data: bytes) -> FlacAudio:
        if not data:
            raise TransientError("ffmpeg produced an empty file", code="TRANSCODE_NO_OUTPUT")

        try:
            info = read_streaminfo(data)
        except MalformedFlacError as exc:
            raise TransientError(
                f"ffmpeg output is not readable FLAC: {exc}", code="TRANSCODE_BAD_OUTPUT"
            ) from exc

        if not info.duration_known:
            raise TransientError(
                "FLAC header carries no duration; the reference would claim 0 seconds",
                code="TRANSCODE_NO_DURATION",
            )

        if (
            info.sample_rate != CANONICAL_SAMPLE_RATE
            or info.channels != CANONICAL_CHANNELS
            or info.bits_per_sample != CANONICAL_BITS
        ):
            # The whole SDK assumes canonical form; a silent ffmpeg behaviour
            # change must not reach the topic.
            raise TransientError(
                f"not canonical: got {info.sample_rate} Hz / {info.channels} ch / "
                f"{info.bits_per_sample} bit, want {CANONICAL_SAMPLE_RATE} / "
                f"{CANONICAL_CHANNELS} / {CANONICAL_BITS}",
                code="TRANSCODE_NOT_CANONICAL",
            )

        return FlacAudio(
            data=data,
            sample_rate=info.sample_rate,
            channels=info.channels,
            bits_per_sample=info.bits_per_sample,
            duration_seconds=info.duration_seconds,
        )
