"""duration + RMS -- the reference function (spec §13 step 4).

Deliberately trivial. Its purpose is to be the contract test for the platform:
if a call can go in as an Audio API response and come out as a Result on the
results topic with correct numbers, everything between works, and functions
2..N are the same shape with a different `process`.

Note what is not here: no Kafka, no offsets, no retries, no S3, no
serialization, no metrics. That is the whole point of §5.1 -- a function author
writes an algorithm and declares it in function.yaml, and the platform is
somebody else's problem.
"""

from __future__ import annotations

import json
import math

import numpy as np

from faas_sdk import FunctionResult

# log10(0) is -inf, which JSON cannot represent -- a strict reader downstream
# would fail to parse the payload. Digital silence reports this floor instead.
SILENCE_DBFS = -120.0

SCHEMA_VERSION = "1"


class DurationRms:
    function_id = "duration_rms"
    function_version = "1.0.0"

    def process(self, ref, audio) -> FunctionResult | None:
        samples = audio.samples()

        if samples.size == 0:
            # A real decode of a real object that happens to hold nothing.
            # SKIPPED says so; FAILED would send it to the DLQ for a replay
            # that cannot help.
            return FunctionResult.skip()

        # float64 for the accumulation: a 5-minute file is 4.8M samples, and
        # float32 summation loses low-order bits well before that.
        squares = np.square(samples.astype(np.float64))
        rms = float(math.sqrt(squares.mean()))
        peak = float(np.abs(samples).max())

        payload = {
            # Measured from the audio, not taken from the reference: trusting
            # the hydrator's claim would leave the function unable to notice a
            # truncated object. Both are reported so a mismatch is visible
            # rather than silently resolved.
            "duration_seconds": samples.size / audio.sample_rate,
            "reference_duration_seconds": ref.duration_seconds,
            "sample_rate": audio.sample_rate,
            "samples": int(samples.size),
            "rms": rms,
            "peak": peak,
            "dbfs": 20 * math.log10(rms) if rms > 0 else SILENCE_DBFS,
        }
        return FunctionResult(
            payload=json.dumps(payload).encode(),
            schema_version=SCHEMA_VERSION,
            content_type="application/json",
        )


def main() -> None:
    from faas_sdk import run

    run(DurationRms)


if __name__ == "__main__":
    main()
