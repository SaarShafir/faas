"""The nine functions added for the stress run, over real audio.

The corpus is canonical mono 16 kHz FLAC, exactly as the Audio API serves it
and exactly as the hydrator stores it, so what these functions see here is
byte-identical to what they see in the local stack.

What is checked is the property each function exists to measure, on a file
chosen because it has that property in a known amount. Not exact numbers for
their own sake: `silence_ratio` on digital silence must be 1.0 because anything
else means the function is broken, while `spectral_centroid` on pink noise is
asserted only to sit in a sane band, because the exact value depends on the
noise realisation and pinning it would be pinning the random seed.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("numpy")
pytest.importorskip("soundfile")

from faas_sdk.models import AudioReference, Status  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
CORPUS = REPO / "corpus"

pytestmark = pytest.mark.skipif(
    not (CORPUS / "manifest.json").exists(),
    reason="corpus not generated -- run `python -m stress.corpus --out corpus`",
)


class _Handle:
    """The slice of AudioHandle a function actually touches."""

    def __init__(self, samples, sample_rate: int):
        self._samples = samples
        self.sample_rate = sample_rate

    def samples(self):
        return self._samples


@pytest.fixture(scope="module")
def hydrated():
    """Corpus file -> decoded samples.

    No transcode step, because there is not one in the platform either: the
    corpus file *is* the object the hydrator would have stored. Module-scoped
    because decoding five minutes of audio repeatedly is the slow part.
    """
    import soundfile

    cache: dict[str, _Handle] = {}

    def load(audio_id: str) -> _Handle:
        if audio_id not in cache:
            source = CORPUS / f"{audio_id}.flac"
            samples, rate = soundfile.read(source, dtype="float32")
            cache[audio_id] = _Handle(samples, rate)
        return cache[audio_id]

    return load


def _ref(call_id: str = "call-1") -> AudioReference:
    return AudioReference(call_id=call_id, object_key=f"{call_id}.flac", duration_seconds=30.0)


def _payload(result) -> dict:
    assert result is not None, "expected a result, got SKIPPED"
    return json.loads(result.payload)


# -- the analyzers ---------------------------------------------------------


def test_silence_ratio_is_total_on_digital_silence(hydrated):
    from functions.silence_ratio.function import SilenceRatio

    payload = _payload(SilenceRatio().process(_ref(), hydrated("digital-silence-30s")))
    assert payload["silence_ratio"] == 1.0
    assert payload["longest_silence_seconds"] == pytest.approx(30, abs=0.5)


def test_silence_ratio_is_near_zero_on_continuous_tone(hydrated):
    from functions.silence_ratio.function import SilenceRatio

    payload = _payload(SilenceRatio().process(_ref(), hydrated("tone-440")))
    assert payload["silence_ratio"] < 0.01


def test_silence_ratio_finds_the_gaps_in_gated_audio(hydrated):
    """The speech-like file is bursts separated by silence, so the answer has
    to be somewhere in the middle -- a function that always says 0 or 1 would
    pass both tests above."""
    from functions.silence_ratio.function import SilenceRatio

    payload = _payload(SilenceRatio().process(_ref(), hydrated("speech-like")))
    assert 0.2 < payload["silence_ratio"] < 0.8


def test_clipping_is_found_where_it_was_put(hydrated):
    from functions.clipping_detect.function import ClippingDetect

    payload = _payload(ClippingDetect().process(_ref(), hydrated("clipped-hot-master")))
    assert payload["clipped"] is True
    # The generator drives a sine 2.5x into a 16-bit container, so most of the
    # waveform is flat-topped.
    assert payload["clipped_sample_ratio"] > 0.5
    assert payload["clip_events"] > 100


def test_a_clean_tone_is_not_reported_as_clipped(hydrated):
    """The test that matters: a 0.7-amplitude sine peaks well below full scale
    and must not trip the detector."""
    from functions.clipping_detect.function import ClippingDetect

    payload = _payload(ClippingDetect().process(_ref(), hydrated("tone-440")))
    assert payload["clipped"] is False
    assert payload["clipped_sample_ratio"] == 0.0


def test_zero_crossing_rate_matches_the_tone_frequency(hydrated):
    """A 440 Hz sine crosses zero 880 times a second, by definition. This is
    the one function in the set with an exact expected answer."""
    from functions.zero_crossing_rate.function import ZeroCrossingRate

    payload = _payload(ZeroCrossingRate().process(_ref(), hydrated("tone-440")))
    assert payload["zcr_per_second"] == pytest.approx(880, rel=0.05)


def test_noise_crosses_zero_far_more_often_than_a_tone(hydrated):
    from functions.zero_crossing_rate.function import ZeroCrossingRate

    function = ZeroCrossingRate()
    tone = _payload(function.process(_ref(), hydrated("tone-440")))
    noise = _payload(function.process(_ref(), hydrated("pink-noise")))
    assert noise["zcr_per_second"] > 2 * tone["zcr_per_second"]


def test_spectral_centroid_lands_on_the_tone(hydrated):
    from functions.spectral_centroid.function import SpectralCentroid

    payload = _payload(SpectralCentroid().process(_ref(), hydrated("tone-440")))
    # Hann windowing spreads a pure tone across neighbouring bins, so the
    # centroid sits near 440 Hz rather than exactly on it.
    assert payload["centroid_hz"] == pytest.approx(440, rel=0.15)


def test_spectral_centroid_is_higher_for_noise_than_for_a_low_tone(hydrated):
    from functions.spectral_centroid.function import SpectralCentroid

    function = SpectralCentroid()
    tone = _payload(function.process(_ref(), hydrated("tone-440")))
    noise = _payload(function.process(_ref(), hydrated("pink-noise")))
    assert noise["centroid_hz"] > tone["centroid_hz"]


def test_rolloff_sees_band_limiting_the_sample_rate_no_longer_shows(hydrated):
    """The point of the function: the phone file arrives at 16 kHz like
    everything else, so its sample rate says wideband and only the spectrum
    knows the content was band-limited upstream."""
    from functions.spectral_rolloff.function import SpectralRolloff

    payload = _payload(SpectralRolloff().process(_ref(), hydrated("phone-narrowband")))
    assert payload["band_limited"] is True
    # Nothing survives above the original 4 kHz Nyquist.
    assert payload["rolloff_p90_hz"] < 4200


def test_flatness_separates_tone_from_noise(hydrated):
    from functions.spectral_rolloff.function import SpectralRolloff

    function = SpectralRolloff()
    tone = _payload(function.process(_ref(), hydrated("tone-440")))
    noise = _payload(function.process(_ref(), hydrated("pink-noise")))
    assert tone["flatness"] < noise["flatness"]


def test_snr_is_high_for_gated_audio_and_low_for_steady_noise(hydrated):
    """Gated bursts have real silence between them, so the noise floor is
    genuinely measurable. Steady noise has no gaps and the estimate collapses --
    which is what `confidence` is for."""
    from functions.snr_estimate.function import SnrEstimate

    function = SnrEstimate()
    gated = _payload(function.process(_ref(), hydrated("speech-like")))
    steady = _payload(function.process(_ref(), hydrated("pink-noise")))

    assert gated["snr_db"] > steady["snr_db"]
    assert steady["confidence"] < gated["confidence"]


def test_vad_finds_speech_in_the_gated_file_and_none_in_silence(hydrated):
    from functions.energy_vad.function import EnergyVad

    function = EnergyVad()
    speech = _payload(function.process(_ref(), hydrated("speech-like")))
    assert speech["segments"] > 1
    assert 0.2 < speech["speech_ratio"] < 0.9

    silent = _payload(function.process(_ref(), hydrated("digital-silence-30s")))
    assert silent["segments"] == 0
    assert silent["speech_ratio"] == 0.0


def test_vad_bridges_gaps_rather_than_splitting_every_word(hydrated):
    """The gated file switches at 0.7 Hz, so roughly 30 bursts in 45 seconds.
    Without gap bridging a real recording fragments into far more segments than
    it has utterances; this pins that the bridging is doing something."""
    from functions.energy_vad.function import EnergyVad

    payload = _payload(EnergyVad().process(_ref(), hydrated("speech-like")))
    assert payload["segments"] < 60
    assert payload["mean_segment_seconds"] > 0.2


# -- the stressors ---------------------------------------------------------


def test_the_flaky_function_fails_the_same_calls_every_time(tmp_path, monkeypatch):
    """Determinism is the whole point: a difference between two stress runs has
    to mean the platform changed, not the dice."""
    monkeypatch.setenv("FAAS_FLAKY_STATE_DIR", str(tmp_path))
    from functions.flaky_analyzer.function import _bucket

    first = [_bucket(f"call-{i}") for i in range(50)]
    second = [_bucket(f"call-{i}") for i in range(50)]
    assert first == second
    assert all(0.0 <= b < 1.0 for b in first)


def test_the_flaky_function_sends_poison_straight_to_the_dlq(tmp_path, monkeypatch):
    from faas_sdk.errors import PoisonMessageError
    from functions.flaky_analyzer.function import FlakyAnalyzer, _bucket

    monkeypatch.setattr("functions.flaky_analyzer.function.STATE_DIR", str(tmp_path), raising=False)
    poison = next(f"call-{i}" for i in range(500) if _bucket(f"call-{i}") < 0.05)

    with pytest.raises(PoisonMessageError):
        FlakyAnalyzer().process(_ref(poison), None)


def test_the_flaky_function_recovers_on_the_second_attempt(tmp_path, monkeypatch, hydrated):
    """The transient bucket must fail once and then succeed. If it failed every
    attempt it would reach the DLQ looking exactly like poison, and the run
    would prove nothing about recovery."""
    from faas_sdk.errors import TransientError
    from functions.flaky_analyzer import function as module

    monkeypatch.setattr(module, "STATE_DIR", str(tmp_path))
    transient = next(f"call-{i}" for i in range(500) if 0.05 <= module._bucket(f"call-{i}") < 0.20)
    audio = hydrated("tone-440")

    with pytest.raises(TransientError):
        module.FlakyAnalyzer().process(_ref(transient), audio)

    assert _payload(module.FlakyAnalyzer().process(_ref(transient), audio))["verdict"] == "ok"


def test_the_slow_function_burns_roughly_what_it_promises(hydrated, monkeypatch):
    """Its cost has to scale with audio length, or it cannot be aimed at the
    timeout the way the declaration assumes."""
    monkeypatch.setenv("FAAS_BURN_SECONDS_PER_MINUTE", "1")
    from functions.slow_burner import function as module

    monkeypatch.setattr(module, "BURN_SECONDS_PER_MINUTE", 1.0)
    payload = _payload(module.SlowBurner().process(_ref(), hydrated("digital-silence-30s")))

    # 30 seconds of audio at 1 s/minute is half a second of work.
    assert payload["budget_seconds"] == pytest.approx(0.5, abs=0.05)
    assert payload["burned_seconds"] >= payload["budget_seconds"]
    assert payload["iterations"] > 0


def test_every_function_returns_a_result_the_sdk_can_stamp(hydrated):
    """One pass over all ten with the same file: whatever else they compute,
    the platform only cares that `process` hands back something with a payload,
    a schema version and a status it can put in a §6 envelope."""
    from functions.clipping_detect.function import ClippingDetect
    from functions.duration_rms.function import DurationRms
    from functions.energy_vad.function import EnergyVad
    from functions.silence_ratio.function import SilenceRatio
    from functions.snr_estimate.function import SnrEstimate
    from functions.spectral_centroid.function import SpectralCentroid
    from functions.spectral_rolloff.function import SpectralRolloff
    from functions.zero_crossing_rate.function import ZeroCrossingRate

    audio = hydrated("speech-like")
    for factory in (
        DurationRms,
        SilenceRatio,
        ClippingDetect,
        ZeroCrossingRate,
        SpectralCentroid,
        SpectralRolloff,
        SnrEstimate,
        EnergyVad,
    ):
        function = factory()
        result = function.process(_ref(), audio)
        assert result is not None, f"{function.function_id} skipped real audio"
        assert result.status is Status.SUCCESS
        assert result.schema_version
        assert json.loads(result.payload), f"{function.function_id} returned an empty payload"
