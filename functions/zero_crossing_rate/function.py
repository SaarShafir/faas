"""Zero-crossing rate -- the cheapest useful discriminator there is.

No FFT, one comparison and one diff over the signal. High ZCR means noise or
fricatives, low ZCR means voiced speech or tone, and the ratio between the two
separates speech from hold music without a model. It earns its place in the mix
by being the function whose cost is dominated by fetching and decoding the file
rather than by anything it computes -- which makes it the one that will show
platform overhead first.
"""

from __future__ import annotations

import json

import numpy as np

from faas_sdk import FunctionResult

# 20 ms frames, matching silence_ratio, so per-frame numbers from the two
# functions line up for whoever joins them downstream.
FRAME_MS = 20.0

# Below this a frame is silence, and the ZCR of silence is noise about zero --
# including it would swamp the mean with meaningless crossings.
SILENCE_AMPLITUDE = 10 ** (-50.0 / 20)

SCHEMA_VERSION = "1"


class ZeroCrossingRate:
    function_id = "zero_crossing_rate"
    function_version = "1.0.0"

    def process(self, ref, audio) -> FunctionResult | None:
        samples = audio.samples()
        if samples.size < 2:
            return FunctionResult.skip()

        frame = max(2, int(audio.sample_rate * FRAME_MS / 1000))
        usable = samples[: samples.size - samples.size % frame]
        if usable.size < frame:
            usable = samples[: (samples.size // 2) * 2]
            frame = usable.size

        frames = usable.reshape(-1, frame).astype(np.float32)

        # signbit rather than > 0: it puts -0.0 and +0.0 on opposite sides,
        # which np.sign collapses, and digital silence is full of signed zeros.
        crossings = np.diff(np.signbit(frames), axis=1).sum(axis=1)
        rate_per_second = crossings / (frame / audio.sample_rate)

        rms = np.sqrt(np.square(frames.astype(np.float64)).mean(axis=1))
        voiced = rms >= SILENCE_AMPLITUDE

        payload = {
            "zcr_per_second": float(rate_per_second.mean()),
            "zcr_std": float(rate_per_second.std()),
            # The number that actually discriminates: silence excluded.
            "voiced_zcr_per_second": (
                float(rate_per_second[voiced].mean()) if voiced.any() else 0.0
            ),
            "voiced_frame_ratio": float(voiced.mean()),
            "frames": int(frames.shape[0]),
        }
        return FunctionResult(
            payload=json.dumps(payload).encode(),
            schema_version=SCHEMA_VERSION,
            content_type="application/json",
        )


def main() -> None:
    from faas_sdk import run

    run(ZeroCrossingRate)


if __name__ == "__main__":
    main()
