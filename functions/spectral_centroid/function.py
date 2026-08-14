"""Spectral centroid -- where the energy sits in the spectrum.

The FFT-heavy member of the set, and deliberately so: at 16 kHz a 5-minute file
is roughly 4,700 frames of 512-point real FFT, which is real work rather than a
mean over an array. Its throughput should sit visibly below the cheap functions'
in the stress run, and if it does not, the bottleneck is elsewhere -- which is
exactly the thing worth learning.

Centroid tracks perceived brightness: hold music sits high, a telephone voice
sits around 500-1500 Hz, and a dead line sits wherever its noise floor is.
"""

from __future__ import annotations

import json

import numpy as np

from faas_sdk import FunctionResult

# 32 ms at 16 kHz = 512 samples, a power of two so the FFT hits its fast path.
FRAME = 512
HOP = 256

SILENCE_AMPLITUDE = 10 ** (-50.0 / 20)

SCHEMA_VERSION = "1"


class SpectralCentroid:
    function_id = "spectral_centroid"
    function_version = "1.0.0"

    def process(self, ref, audio) -> FunctionResult | None:
        samples = audio.samples()
        if samples.size < FRAME:
            # Shorter than one analysis window: there is no spectrum to speak
            # of, and padding one into existence would be inventing data.
            return FunctionResult.skip()

        frames = np.lib.stride_tricks.sliding_window_view(samples, FRAME)[::HOP]

        # Hann window: without it every frame boundary is a discontinuity, and
        # the spectral leakage from those edges lands as broadband energy that
        # drags the centroid upward on quiet frames.
        windowed = frames * np.hanning(FRAME).astype(np.float32)
        spectrum = np.abs(np.fft.rfft(windowed, axis=1))

        freqs = np.fft.rfftfreq(FRAME, d=1.0 / audio.sample_rate)
        magnitude = spectrum.sum(axis=1)

        # A frame of pure silence has zero magnitude, and its centroid is 0/0.
        # Masking is the only honest answer; nan_to_num would report 0 Hz, which
        # reads as "all energy at DC" rather than "no energy".
        voiced = magnitude > 0
        centroids = np.zeros(frames.shape[0], dtype=np.float64)
        centroids[voiced] = (spectrum[voiced] @ freqs) / magnitude[voiced]

        loud = np.sqrt(np.square(frames.astype(np.float64)).mean(axis=1)) >= SILENCE_AMPLITUDE
        measured = centroids[voiced & loud]

        payload = {
            "centroid_hz": float(measured.mean()) if measured.size else 0.0,
            "centroid_std_hz": float(measured.std()) if measured.size else 0.0,
            "centroid_p10_hz": float(np.percentile(measured, 10)) if measured.size else 0.0,
            "centroid_p90_hz": float(np.percentile(measured, 90)) if measured.size else 0.0,
            "measured_frames": int(measured.size),
            "frames": int(frames.shape[0]),
            "nyquist_hz": float(audio.sample_rate / 2),
        }
        return FunctionResult(
            payload=json.dumps(payload).encode(),
            schema_version=SCHEMA_VERSION,
            content_type="application/json",
        )


def main() -> None:
    from faas_sdk import run

    run(SpectralCentroid)


if __name__ == "__main__":
    main()
