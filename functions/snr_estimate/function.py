"""Signal-to-noise ratio, estimated without a reference signal.

There is no clean copy to compare against, so the noise floor is taken from the
quietest frames in the file and the signal from the loudest. That works because
of how calls actually sound -- speech is intermittent, so the gaps between words
*are* a sample of the noise -- and it fails on a file with no gaps, which is
what `confidence` below is for.

The estimate is deliberately a percentile split rather than a VAD: energy_vad
already owns the segmentation, and duplicating it here would mean two functions
disagreeing about where speech starts.
"""

from __future__ import annotations

import json

import numpy as np

from faas_sdk import FunctionResult

FRAME_MS = 20.0

# The quietest 10% of frames are the noise floor, the loudest 10% the signal.
# Wider than a median split, which on sparse speech would put half the silence
# in the signal bucket.
NOISE_PERCENTILE = 10
SIGNAL_PERCENTILE = 90

# Reported when there is no measurable noise floor at all -- a synthetic file,
# or one that has been noise-gated. Not infinity, which no JSON reader wants.
MAX_SNR_DB = 100.0

SCHEMA_VERSION = "1"


class SnrEstimate:
    function_id = "snr_estimate"
    function_version = "1.0.0"

    def process(self, ref, audio) -> FunctionResult | None:
        samples = audio.samples()
        frame = max(1, int(audio.sample_rate * FRAME_MS / 1000))
        if samples.size < frame * 10:
            # Fewer than ten frames makes a percentile split meaningless.
            return FunctionResult.skip()

        usable = samples[: samples.size - samples.size % frame]
        frames = usable.reshape(-1, frame).astype(np.float64)
        power = np.square(frames).mean(axis=1)

        noise = float(np.percentile(power, NOISE_PERCENTILE))
        signal = float(np.percentile(power, SIGNAL_PERCENTILE))

        if signal <= 0:
            return FunctionResult.skip()

        if noise <= 0:
            snr_db = MAX_SNR_DB
        else:
            snr_db = min(MAX_SNR_DB, 10 * np.log10(signal / noise))

        # How separated the two populations are. A file with no quiet gaps
        # gives noise ~ signal and a meaningless SNR; saying so is more useful
        # than reporting 0 dB as though it were measured.
        spread = float(np.log10(signal + 1e-20) - np.log10(noise + 1e-20))
        payload = {
            "snr_db": float(snr_db),
            "noise_floor_dbfs": float(10 * np.log10(noise)) if noise > 0 else -MAX_SNR_DB,
            "signal_dbfs": float(10 * np.log10(signal)),
            "confidence": float(min(1.0, spread / 3.0)),
            "frames": int(frames.shape[0]),
        }
        return FunctionResult(
            payload=json.dumps(payload).encode(),
            schema_version=SCHEMA_VERSION,
            content_type="application/json",
        )


def main() -> None:
    from faas_sdk import run

    run(SnrEstimate)


if __name__ == "__main__":
    main()
