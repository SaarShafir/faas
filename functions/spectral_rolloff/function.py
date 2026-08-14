"""Spectral rolloff and flatness -- bandwidth, and whether it is tone or noise.

Rolloff is the frequency below which 85% of the energy sits: it says where the
recording's usable bandwidth actually ends, which is how you catch audio that
was 8 kHz narrowband long before it reached you. The hydrator resamples
everything to 16 kHz, so sample rate alone can no longer tell you that -- the
spectrum can.

Flatness (geometric mean over arithmetic mean) separates tonal from noisy: near
0 for a sine, near 1 for white noise. Together they distinguish hold music from
a dead line from a real conversation.
"""

from __future__ import annotations

import json

import numpy as np

from faas_sdk import FunctionResult

FRAME = 512
HOP = 256

ROLLOFF_FRACTION = 0.85

SILENCE_AMPLITUDE = 10 ** (-50.0 / 20)

# Floor under the log in the geometric mean: log(0) is -inf, and one empty bin
# would take the whole flatness figure to zero. -200 dB is far below anything
# 16-bit audio can represent, so it changes no real measurement.
EPSILON = 1e-10

SCHEMA_VERSION = "1"


class SpectralRolloff:
    function_id = "spectral_rolloff"
    function_version = "1.0.0"

    def process(self, ref, audio) -> FunctionResult | None:
        samples = audio.samples()
        if samples.size < FRAME:
            return FunctionResult.skip()

        frames = np.lib.stride_tricks.sliding_window_view(samples, FRAME)[::HOP]
        loud = np.sqrt(np.square(frames.astype(np.float64)).mean(axis=1)) >= SILENCE_AMPLITUDE
        if not loud.any():
            # Every frame is silence. Rolloff of silence is not a small number,
            # it is undefined -- SKIPPED says so, where 0 Hz would be a lie
            # that averages into someone's dashboard.
            return FunctionResult.skip()

        windowed = frames[loud] * np.hanning(FRAME).astype(np.float32)
        spectrum = np.abs(np.fft.rfft(windowed, axis=1)).astype(np.float64)
        freqs = np.fft.rfftfreq(FRAME, d=1.0 / audio.sample_rate)

        cumulative = np.cumsum(spectrum, axis=1)
        totals = cumulative[:, -1:]
        # searchsorted per row would need a loop; this is the same thing as a
        # single vectorised comparison, which matters at 4,700 frames a file.
        crossed = cumulative >= ROLLOFF_FRACTION * totals
        rolloff_bin = np.argmax(crossed, axis=1)
        rolloff_hz = freqs[rolloff_bin]

        power = np.square(spectrum) + EPSILON
        flatness = np.exp(np.log(power).mean(axis=1)) / power.mean(axis=1)

        payload = {
            "rolloff_hz": float(rolloff_hz.mean()),
            "rolloff_p90_hz": float(np.percentile(rolloff_hz, 90)),
            "flatness": float(flatness.mean()),
            "measured_frames": int(rolloff_hz.size),
            "frames": int(frames.shape[0]),
            "rolloff_fraction": ROLLOFF_FRACTION,
            # A file whose rolloff sits far under Nyquist was band-limited
            # before it got here, whatever its current sample rate says.
            "band_limited": bool(rolloff_hz.mean() < 0.75 * audio.sample_rate / 2),
        }
        return FunctionResult(
            payload=json.dumps(payload).encode(),
            schema_version=SCHEMA_VERSION,
            content_type="application/json",
        )


def main() -> None:
    from faas_sdk import run

    run(SpectralRolloff)


if __name__ == "__main__":
    main()
