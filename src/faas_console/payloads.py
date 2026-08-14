"""Turning a function's payload into something you can read at a glance.

Every function here returns numbers about audio, and a wall of JSON hides the
one number that matters. A silence ratio of 0.93 and a silence ratio of 0.07
look identical in a code block and could not be more different in a review.

The rules this follows:

  - **Never hide the JSON.** Every rendering is additive; the raw payload stays
    one click away. A visualisation that quietly disagreed with the data would
    be worse than no visualisation.
  - **Degrade to something useful.** An unrecognised payload still gets its
    numbers laid out, so a function written next week is never unviewable. The
    per-function knowledge below is a bonus, not a requirement.
  - **Say what the number means, not just what it is.** "dbfs -63.0" is a fact;
    "-63 dBFS, near the noise floor" is the thing the reader wanted.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field


@dataclass
class Figure:
    """One rendered number."""

    label: str
    value: str
    # 0..1, drives a bar. None means the value has no natural scale.
    fraction: float | None = None
    note: str = ""
    tone: str = ""  # "", "good", "warn", "bad"


@dataclass
class Rendering:
    headline: str = ""
    figures: list[Figure] = field(default_factory=list)
    # Fractions of a timeline, as (start, width, kind) in 0..1.
    timeline: list[tuple] = field(default_factory=list)
    timeline_caption: str = ""
    raw: str = ""


def render(function_id: str, payload: bytes | str | None) -> Rendering:
    if not payload:
        return Rendering(headline="No payload")

    text = payload.decode("utf-8", "replace") if isinstance(payload, bytes) else payload
    try:
        data = json.loads(text)
    except ValueError:
        return Rendering(headline="Not JSON", raw=text[:4000])
    if not isinstance(data, dict):
        return Rendering(raw=json.dumps(data, indent=2)[:4000])

    renderer = _RENDERERS.get(function_id, _generic)
    rendering = renderer(data)
    rendering.raw = json.dumps(data, indent=2)
    return rendering


def _pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _dbfs_tone(dbfs: float) -> str:
    if dbfs > -3:
        return "bad"
    if dbfs < -50:
        return "warn"
    return "good"


# -- per function ----------------------------------------------------------


def _duration_rms(data: dict) -> Rendering:
    measured = data.get("duration_seconds", 0.0)
    claimed = data.get("reference_duration_seconds", 0.0)
    dbfs = data.get("dbfs", -120.0)

    figures = [
        Figure("Duration", f"{measured:.1f}s", note=f"reference claims {claimed:.1f}s"),
        Figure("Level", f"{dbfs:.1f} dBFS", fraction=_level_fraction(dbfs), tone=_dbfs_tone(dbfs)),
        Figure("Peak", f"{data.get('peak', 0):.3f}", fraction=min(1.0, data.get("peak", 0))),
        Figure("Sample rate", f"{data.get('sample_rate', 0)} Hz"),
    ]

    disagreement = measured - claimed if claimed else 0.0
    if abs(disagreement) > 1:
        # The truncated-upload case: it hydrates successfully into a FLAC of
        # the wrong length, and this is the only place it shows.
        figures.insert(
            1,
            Figure(
                "Disagreement",
                f"{disagreement:+.1f}s",
                tone="bad",
                note="measured audio does not match what the reference claims",
            ),
        )

    return Rendering(headline=f"{measured:.1f}s at {dbfs:.0f} dBFS", figures=figures)


def _silence_ratio(data: dict) -> Rendering:
    ratio = data.get("silence_ratio", 0.0)
    leading = data.get("leading_silence_seconds", 0.0)
    trailing = data.get("trailing_silence_seconds", 0.0)
    silent = data.get("silent_seconds", 0.0)
    total = silent / ratio if ratio else 0.0

    timeline = []
    if total > 0:
        if leading:
            timeline.append((0.0, min(1.0, leading / total), "silence"))
        if trailing:
            timeline.append((max(0.0, 1 - trailing / total), min(1.0, trailing / total), "silence"))

    return Rendering(
        headline=f"{_pct(ratio)} silence",
        figures=[
            Figure(
                "Silence",
                _pct(ratio),
                fraction=ratio,
                tone="warn" if ratio > 0.6 else "good",
            ),
            Figure("Longest gap", f"{data.get('longest_silence_seconds', 0):.1f}s"),
            Figure("Leading", f"{leading:.1f}s", note="dead air before anything happens"),
            Figure("Trailing", f"{trailing:.1f}s"),
        ],
        timeline=timeline,
        timeline_caption="leading and trailing silence" if timeline else "",
    )


def _clipping_detect(data: dict) -> Rendering:
    clipped = data.get("clipped", False)
    ratio = data.get("clipped_sample_ratio", 0.0)
    return Rendering(
        headline="Clipped" if clipped else "Not clipped",
        figures=[
            Figure(
                "Clipped samples",
                _pct(ratio),
                fraction=ratio,
                tone="bad" if clipped else "good",
                note="a flat-topped waveform: everything measured after this is suspect"
                if clipped
                else "",
            ),
            Figure("Clip events", str(data.get("clip_events", 0))),
            Figure("Longest run", f"{data.get('longest_clip_samples', 0)} samples"),
            Figure("Peak", f"{data.get('peak', 0):.3f}", fraction=min(1.0, data.get("peak", 0))),
        ],
    )


def _energy_vad(data: dict) -> Rendering:
    ratio = data.get("speech_ratio", 0.0)
    segments = data.get("segments", 0)
    return Rendering(
        headline=f"{_pct(ratio)} speech in {segments} segment{'s' if segments != 1 else ''}",
        figures=[
            Figure("Speech", _pct(ratio), fraction=ratio, tone="good" if ratio > 0.1 else "warn"),
            Figure("Segments", str(segments)),
            Figure("Mean segment", f"{data.get('mean_segment_seconds', 0):.2f}s"),
            Figure(
                "First speech",
                f"{data.get('first_speech_seconds', -1):.1f}s"
                if data.get("first_speech_seconds", -1) >= 0
                else "never",
                note="how long the caller waited before anything was said",
            ),
        ],
        timeline=[(0.0, ratio, "speech")] if ratio else [],
        timeline_caption="share of the call with speech in it" if ratio else "",
    )


def _spectral_centroid(data: dict) -> Rendering:
    centroid = data.get("centroid_hz", 0.0)
    nyquist = data.get("nyquist_hz", 8000.0) or 8000.0
    return Rendering(
        headline=f"{centroid:.0f} Hz centroid",
        figures=[
            Figure(
                "Centroid",
                f"{centroid:.0f} Hz",
                fraction=min(1.0, centroid / nyquist),
                note="brightness: telephone speech sits around 500-1500 Hz",
            ),
            Figure(
                "Spread (p10-p90)",
                f"{data.get('centroid_p10_hz', 0):.0f}-{data.get('centroid_p90_hz', 0):.0f} Hz",
            ),
            Figure(
                "Measured frames",
                f"{data.get('measured_frames', 0)} of {data.get('frames', 0)}",
            ),
        ],
    )


def _spectral_rolloff(data: dict) -> Rendering:
    rolloff = data.get("rolloff_hz", 0.0)
    band_limited = data.get("band_limited", False)
    flatness = data.get("flatness", 0.0)
    return Rendering(
        headline=f"{rolloff:.0f} Hz rolloff",
        figures=[
            Figure(
                "Rolloff (85%)",
                f"{rolloff:.0f} Hz",
                fraction=min(1.0, rolloff / 8000),
                tone="warn" if band_limited else "good",
                note="was narrowband before it got here -- the hydrator resampled it, "
                "so only the spectrum knows"
                if band_limited
                else "",
            ),
            Figure(
                "Flatness",
                f"{flatness:.3f}",
                fraction=min(1.0, flatness),
                note="0 is a tone, 1 is white noise",
            ),
        ],
    )


def _snr_estimate(data: dict) -> Rendering:
    snr = data.get("snr_db", 0.0)
    confidence = data.get("confidence", 0.0)
    return Rendering(
        headline=f"{snr:.1f} dB SNR",
        figures=[
            Figure(
                "SNR",
                f"{snr:.1f} dB",
                fraction=min(1.0, max(0.0, snr / 60)),
                tone="good" if snr > 20 else "warn",
            ),
            Figure("Noise floor", f"{data.get('noise_floor_dbfs', 0):.1f} dBFS"),
            Figure(
                "Confidence",
                _pct(confidence),
                fraction=confidence,
                tone="warn" if confidence < 0.4 else "good",
                note="low means the file has no quiet gaps, so the estimate is a guess"
                if confidence < 0.4
                else "",
            ),
        ],
    )


def _zero_crossing_rate(data: dict) -> Rendering:
    zcr = data.get("zcr_per_second", 0.0)
    return Rendering(
        headline=f"{zcr:.0f} crossings/s",
        figures=[
            Figure("Rate", f"{zcr:.0f}/s", fraction=min(1.0, zcr / 4000)),
            Figure("Voiced only", f"{data.get('voiced_zcr_per_second', 0):.0f}/s"),
            Figure(
                "Voiced frames",
                _pct(data.get("voiced_frame_ratio", 0.0)),
                fraction=data.get("voiced_frame_ratio", 0.0),
            ),
        ],
    )


def _slow_burner(data: dict) -> Rendering:
    return Rendering(
        headline=f"burned {data.get('burned_seconds', 0):.1f}s on purpose",
        figures=[
            Figure("Burned", f"{data.get('burned_seconds', 0):.1f}s", tone="warn"),
            Figure("Budget", f"{data.get('budget_seconds', 0):.1f}s"),
            Figure("Audio", f"{data.get('audio_minutes', 0):.1f} min"),
        ],
    )


def _flaky_analyzer(data: dict) -> Rendering:
    bucket = data.get("bucket", 0.0)
    return Rendering(
        headline=f"bucket {bucket:.3f} -- {data.get('verdict', '?')}",
        figures=[
            Figure(
                "Bucket",
                f"{bucket:.3f}",
                fraction=bucket,
                note="a stable hash of call_id: under 0.05 never succeeds, "
                "under 0.20 fails once and then works",
            )
        ],
    )


def _level_fraction(dbfs: float) -> float:
    """-60 dBFS to 0 dBFS across the bar. Below -60 is effectively silence."""
    return min(1.0, max(0.0, (dbfs + 60) / 60))


def _generic(data: dict) -> Rendering:
    """Anything unrecognised. A function added next week still renders."""
    figures = []
    for key, value in list(data.items())[:12]:
        if isinstance(value, bool):
            figures.append(Figure(key.replace("_", " "), "yes" if value else "no"))
        elif isinstance(value, (int, float)):
            fraction = value if 0 <= value <= 1 and isinstance(value, float) else None
            figures.append(Figure(key.replace("_", " "), f"{value:g}", fraction=fraction))
        elif isinstance(value, str):
            figures.append(Figure(key.replace("_", " "), value[:60]))
    return Rendering(headline="", figures=figures)


_RENDERERS = {
    "duration_rms": _duration_rms,
    "silence_ratio": _silence_ratio,
    "clipping_detect": _clipping_detect,
    "energy_vad": _energy_vad,
    "spectral_centroid": _spectral_centroid,
    "spectral_rolloff": _spectral_rolloff,
    "snr_estimate": _snr_estimate,
    "zero_crossing_rate": _zero_crossing_rate,
    "slow_burner": _slow_burner,
    "flaky_analyzer": _flaky_analyzer,
}
