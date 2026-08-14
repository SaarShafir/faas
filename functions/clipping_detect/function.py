"""Clipping: how much of the signal is pinned at full scale.

Worth its own function because clipping invalidates everything downstream. A
transcript from a clipped recording is unreliable and a loudness number from one
is meaningless, so this is the check that says whether to trust the rest.

A single sample at full scale is not clipping -- a sine that peaks exactly at
1.0 touches it twice a cycle. A *run* of consecutive samples at full scale is,
because that is a waveform whose top has been cut off flat.
"""

from __future__ import annotations

import json

import numpy as np

from faas_sdk import FunctionResult

# 16-bit audio decoded to float32 lands one LSB short of 1.0, so an exact
# comparison would find nothing. This is that tolerance, not an arbitrary fudge.
FULL_SCALE = 0.999

# Three consecutive samples: at 16 kHz that is 187 microseconds of flat top,
# which no natural waveform produces at full scale.
MIN_RUN = 3

SCHEMA_VERSION = "1"


class ClippingDetect:
    function_id = "clipping_detect"
    function_version = "1.0.0"

    def process(self, ref, audio) -> FunctionResult | None:
        samples = audio.samples()
        if samples.size == 0:
            return FunctionResult.skip()

        pinned = np.abs(samples) >= FULL_SCALE
        runs = _runs(pinned)
        clipped_runs = [(start, length) for start, length in runs if length >= MIN_RUN]
        clipped_samples = int(sum(length for _, length in clipped_runs))

        payload = {
            "clipped": bool(clipped_runs),
            "clipped_sample_ratio": clipped_samples / samples.size,
            "clipped_seconds": clipped_samples / audio.sample_rate,
            "clip_events": len(clipped_runs),
            "longest_clip_samples": max((length for _, length in clipped_runs), default=0),
            # Reported separately: samples at full scale that are *not* part of
            # a run. A high count with no events means the file is hot but
            # intact, which is a different conversation with the caller.
            "full_scale_samples": int(pinned.sum()),
            "peak": float(np.abs(samples).max()),
        }
        return FunctionResult(
            payload=json.dumps(payload).encode(),
            schema_version=SCHEMA_VERSION,
            content_type="application/json",
        )


def _runs(flags) -> list[tuple[int, int]]:
    """Start and length of every consecutive True run, vectorised.

    A Python loop over 4.8M samples -- a 5-minute file -- is seconds of work
    per call, which at the platform's throughput target is not affordable.
    """
    if not flags.any():
        return []
    padded = np.concatenate(([False], flags, [False]))
    edges = np.flatnonzero(padded[1:] != padded[:-1])
    starts, ends = edges[::2], edges[1::2]
    return list(zip(starts.tolist(), (ends - starts).tolist(), strict=True))


def main() -> None:
    from faas_sdk import run

    run(ClippingDetect)


if __name__ == "__main__":
    main()
