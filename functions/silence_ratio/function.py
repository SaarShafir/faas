"""How much of a call is silence.

The first function anyone asks for on a call-centre corpus: hold music trimmed,
dead air measured, one-sided recordings spotted. Frame-based rather than
per-sample, because a single sample below the threshold is not silence -- a zero
crossing is.

Every threshold here is a judgement call, so they are named constants with the
reasoning attached rather than magic numbers buried in an expression.
"""

from __future__ import annotations

import json

import numpy as np

from faas_sdk import FunctionResult

# -50 dBFS: below the noise floor of a phone line but well above true digital
# silence, so a quiet room still counts as silence and a faint talker does not.
SILENCE_DBFS = -50.0
SILENCE_AMPLITUDE = 10 ** (SILENCE_DBFS / 20)

# 20 ms at 16 kHz. Short enough to resolve the gaps between words, long enough
# that a single glottal closure does not read as a pause.
FRAME_MS = 20.0

SCHEMA_VERSION = "1"


class SilenceRatio:
    function_id = "silence_ratio"
    function_version = "1.0.0"

    def process(self, ref, audio) -> FunctionResult | None:
        samples = audio.samples()
        if samples.size == 0:
            return FunctionResult.skip()

        frame = max(1, int(audio.sample_rate * FRAME_MS / 1000))
        # Trailing partial frame dropped rather than zero-padded: padding would
        # invent silence at the end of every file.
        usable = samples[: samples.size - samples.size % frame]
        if usable.size == 0:
            usable = samples

        frames = usable.reshape(-1, frame)
        rms = np.sqrt(np.square(frames.astype(np.float64)).mean(axis=1))
        quiet = rms < SILENCE_AMPLITUDE

        frame_seconds = frame / audio.sample_rate
        payload = {
            "silence_ratio": float(quiet.mean()),
            "silent_seconds": float(quiet.sum() * frame_seconds),
            "longest_silence_seconds": float(_longest_run(quiet) * frame_seconds),
            "leading_silence_seconds": float(_leading_run(quiet) * frame_seconds),
            "trailing_silence_seconds": float(_leading_run(quiet[::-1]) * frame_seconds),
            "frames": int(frames.shape[0]),
            "threshold_dbfs": SILENCE_DBFS,
        }
        return FunctionResult(
            payload=json.dumps(payload).encode(),
            schema_version=SCHEMA_VERSION,
            content_type="application/json",
        )


def _longest_run(flags) -> int:
    longest = current = 0
    for flag in flags:
        current = current + 1 if flag else 0
        longest = max(longest, current)
    return longest


def _leading_run(flags) -> int:
    loud = np.flatnonzero(~flags)
    # All quiet: the run is the whole file, not zero.
    return int(loud[0]) if loud.size else int(flags.size)


def main() -> None:
    from faas_sdk import run

    run(SilenceRatio)


if __name__ == "__main__":
    main()
