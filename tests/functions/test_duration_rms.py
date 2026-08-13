"""The reference function (spec §13 step 4).

Trivial on purpose. Its job is to be the contract test: if this works end to
end, the platform works, and functions 2..N are the same shape. The end-to-end
half lives in test_contract.py; this file covers the algorithm and the payload.
"""

import json
import math

import numpy as np
import pytest

from faas_sdk.models import Status
from faas_sdk.testing import StubAudioHandle, reference
from functions.duration_rms.function import SILENCE_DBFS, DurationRms


@pytest.fixture
def function():
    return DurationRms()


def _run(function, samples, sample_rate=16000, duration_seconds=None):
    array = np.asarray(samples, dtype="float32")
    ref = reference(
        call_id="c1",
        duration_seconds=(
            duration_seconds if duration_seconds is not None else len(array) / sample_rate
        ),
    )
    handle = StubAudioHandle(ref, samples=array)
    result = function.process(ref, handle)
    return result, (json.loads(result.payload) if result and result.payload else None)


def test_identifies_itself(function):
    assert function.function_id == "duration_rms"
    assert function.function_version == "1.0.0"


def test_measures_rms_of_a_known_signal(function):
    """A full-scale sine has RMS 1/sqrt(2) regardless of frequency."""
    t = np.arange(16000) / 16000
    _, payload = _run(function, np.sin(2 * np.pi * 440 * t))

    assert payload["rms"] == pytest.approx(1 / math.sqrt(2), abs=1e-3)


def test_measures_rms_of_a_constant_signal(function):
    """RMS of a constant is its magnitude -- the simplest case to get wrong by
    forgetting the square root or averaging before squaring."""
    _, payload = _run(function, np.full(1000, 0.5))

    assert payload["rms"] == pytest.approx(0.5, abs=1e-6)


def test_rms_is_not_mean_amplitude(function):
    """Half the signal at +1 and half at -1: mean amplitude is 0, RMS is 1."""
    _, payload = _run(function, np.array([1.0, -1.0] * 500))

    assert payload["rms"] == pytest.approx(1.0, abs=1e-6)


def test_reports_peak(function):
    _, payload = _run(function, np.array([0.1, -0.9, 0.3]))
    assert payload["peak"] == pytest.approx(0.9, abs=1e-6)


def test_measures_duration_from_the_audio_not_from_the_reference(function):
    """The reference is the hydrator's claim; this is the measurement. Trusting
    the claim would make the function unable to detect a truncated object."""
    _, payload = _run(function, np.zeros(8000), duration_seconds=999.0)

    assert payload["duration_seconds"] == pytest.approx(0.5, abs=1e-6)


def test_reports_the_reference_duration_alongside_it(function):
    """So a mismatch is visible downstream instead of being silently resolved
    in favour of one or the other."""
    _, payload = _run(function, np.zeros(8000), duration_seconds=999.0)

    assert payload["reference_duration_seconds"] == 999.0


def test_silence_reports_a_floor_not_negative_infinity(function):
    """log10(0) is -inf, and JSON has no way to represent it -- the payload
    would be unparseable by a strict reader."""
    _, payload = _run(function, np.zeros(1000))

    assert payload["rms"] == 0.0
    assert payload["dbfs"] == SILENCE_DBFS
    assert json.dumps(payload)  # round-trips through strict JSON


def test_dbfs_of_a_half_scale_signal(function):
    _, payload = _run(function, np.full(1000, 0.5))
    assert payload["dbfs"] == pytest.approx(-6.02, abs=0.01)


def test_an_empty_object_is_skipped_not_failed(function):
    """Zero samples is a real decode of a real object -- there is simply nothing
    to measure. SKIPPED says that; FAILED would send it to the DLQ for replay
    that cannot help."""
    result, payload = _run(function, np.array([]))

    assert result.status is Status.SKIPPED
    assert payload is None


def test_payload_is_declared_json(function):
    result, _ = _run(function, np.zeros(100))

    assert result.status is Status.SUCCESS
    assert result.content_type == "application/json"
    assert result.schema_version == "1"


def test_payload_is_small_enough_to_inline(function):
    """Well under the 256 KB claim-check threshold (§6), so this function never
    exercises the S3 path -- which is what makes it a clean contract test."""
    from faas_sdk.results import INLINE_PAYLOAD_LIMIT_BYTES

    result, _ = _run(function, np.zeros(16000 * 300))
    assert len(result.payload) < INLINE_PAYLOAD_LIMIT_BYTES


def test_audio_is_decoded_once(function):
    ref = reference(call_id="c1")
    handle = StubAudioHandle(ref, samples=np.zeros(1000, dtype="float32"))

    function.process(ref, handle)

    assert handle.reads == 1
