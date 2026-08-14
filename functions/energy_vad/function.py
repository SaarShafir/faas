"""Energy-based voice activity detection: where the talking is.

An energy VAD, not a model -- no weights to ship, no GPU, no inference server.
It is wrong on noisy audio in the way every energy VAD is wrong, and that is an
acceptable trade for a function that costs a mean per frame.

Two details do most of the work. The threshold is relative to the file's own
noise floor rather than absolute, because a quiet recording and a loud one have
the same speech/silence structure at completely different levels. And short
gaps inside a segment are bridged, because the pause between two words in a
sentence is not the end of the utterance -- without that, one sentence comes out
as fourteen segments.
"""

from __future__ import annotations

import json

import numpy as np

from faas_sdk import FunctionResult

FRAME_MS = 20.0

# 12 dB above the noise floor. Low enough for a quiet talker, high enough that
# line noise does not register as speech.
THRESHOLD_OVER_NOISE_DB = 12.0

# Gaps shorter than this are inside an utterance, not between two.
BRIDGE_GAP_MS = 300.0

# Anything shorter than this is a click, a breath or a door.
MIN_SEGMENT_MS = 200.0

SCHEMA_VERSION = "1"


class EnergyVad:
    function_id = "energy_vad"
    function_version = "1.0.0"

    def process(self, ref, audio) -> FunctionResult | None:
        samples = audio.samples()
        frame = max(1, int(audio.sample_rate * FRAME_MS / 1000))
        if samples.size < frame * 10:
            return FunctionResult.skip()

        usable = samples[: samples.size - samples.size % frame]
        frames = usable.reshape(-1, frame).astype(np.float64)
        power = np.square(frames).mean(axis=1)

        noise = float(np.percentile(power, 10))
        if noise <= 0:
            # Digital silence in the quiet frames: fall back to an absolute
            # floor, or every frame above zero would count as speech.
            threshold = 10 ** (-50.0 / 10)
        else:
            threshold = noise * 10 ** (THRESHOLD_OVER_NOISE_DB / 10)

        active = power > threshold
        # Both thresholds are declared in milliseconds because that is how you
        # reason about speech; frames are the unit they have to be applied in.
        gap_frames = int(BRIDGE_GAP_MS / FRAME_MS)
        min_segment_frames = max(1, int(MIN_SEGMENT_MS / FRAME_MS))

        active = _bridge(active, gap_frames)
        segments = [
            (start, length) for start, length in _runs(active) if length >= min_segment_frames
        ]

        frame_seconds = frame / audio.sample_rate
        speech_frames = sum(length for _, length in segments)
        lengths = [length * frame_seconds for _, length in segments]

        payload = {
            "speech_ratio": speech_frames / active.size,
            "speech_seconds": speech_frames * frame_seconds,
            "segments": len(segments),
            "mean_segment_seconds": float(np.mean(lengths)) if lengths else 0.0,
            "longest_segment_seconds": float(max(lengths)) if lengths else 0.0,
            "first_speech_seconds": float(segments[0][0] * frame_seconds) if segments else -1.0,
            "threshold_dbfs": float(10 * np.log10(threshold)) if threshold > 0 else -100.0,
            "frames": int(active.size),
        }
        return FunctionResult(
            payload=json.dumps(payload).encode(),
            schema_version=SCHEMA_VERSION,
            content_type="application/json",
        )


def _bridge(active, max_gap: int):
    """Fill runs of False shorter than `max_gap`."""
    if max_gap <= 0:
        return active
    bridged = active.copy()
    for start, length in _runs(~active):
        # A gap at either end is leading or trailing silence, not a pause
        # inside an utterance -- bridging those would invent speech.
        if length <= max_gap and start > 0 and start + length < active.size:
            bridged[start : start + length] = True
    return bridged


def _runs(flags) -> list[tuple[int, int]]:
    if not flags.any():
        return []
    padded = np.concatenate(([False], flags, [False]))
    edges = np.flatnonzero(padded[1:] != padded[:-1])
    starts, ends = edges[::2], edges[1::2]
    return list(zip(starts.tolist(), (ends - starts).tolist(), strict=True))


def main() -> None:
    from faas_sdk import run

    run(EnergyVad)


if __name__ == "__main__":
    main()
