"""The audio corpus for the local stack.

Real encoded files -- ffmpeg writes them, libsndfile decodes them on the
function side. Nothing here is a fixture pretending to be audio.

Everything generated here is canonical FLAC (16 kHz, 16-bit, mono), because
that is what the Audio API serves: encoding happens upstream of this platform,
and the hydrator stores what it is given without re-encoding. A corpus in a zoo
of input formats would be testing a stage that no longer exists.

The content is synthetic, and the point of each entry is a property that breaks
something: silence has no RMS to speak of, clipped audio saturates, a DC offset
skews every mean, narrowband is the phone case, a truncated file has a header
that disagrees with its own frames and a zero-byte one has no header at all. A
corpus of ten clean pop songs would prove considerably less.

Drop your own recordings into `samples/` and they join the corpus as-is -- see
`_discover_real_samples`. They must already be canonical FLAC; anything else is
what the hydrator's DLQ is for.

    python -m stress.corpus --out corpus
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

FFMPEG = os.environ.get("FAAS_FFMPEG", "ffmpeg")

CONTENT_TYPE = "audio/flac"

# Canonical form (§4.1), and the only thing the Audio API serves. Every
# generated entry goes through these flags, so the corpus cannot drift from
# what the platform actually receives.
CANONICAL_ARGS = ["-ac", "1", "-ar", "16000", "-sample_fmt", "s16", "-c:a", "flac"]


@dataclass
class Entry:
    """One corpus file, plus what the stack is supposed to do with it."""

    audio_id: str
    filename: str
    description: str
    # Everything the Audio API serves is canonical FLAC; the field stays so the
    # stub can set a Content-Type header without special-casing.
    content_type: str = CONTENT_TYPE
    # "hydrates" -- expected to come out the far end as a Result.
    # "poison"   -- expected to fail hydration and land in the hydrator DLQ.
    # "missing"  -- deliberately absent from the store, so the Audio API 404s.
    expect: str = "hydrates"
    # Seeder weighting. Bulk files carry the load; the nasty ones are seasoning.
    weight: int = 1
    seconds: float = 0.0
    source: str = "generated"
    notes: dict = field(default_factory=dict)


# -- generated files -------------------------------------------------------
#
# `lavfi` sources, so no input files and no network. Each tuple is
# (entry, ffmpeg input args), and every one of them is encoded with
# CANONICAL_ARGS: the interesting property is in the signal, not in the
# container.

_SPEECH_ENVELOPE = (
    # Noise gated by a slow square wave: bursts of energy separated by silence,
    # which is the only property the VAD and silence-ratio functions care
    # about. It is not speech, and it is not pretending to be.
    "aevalsrc='0.4*random(0)*gt(sin(2*PI*0.7*t),0)':s=16000:d=45"
)


def _generated() -> list[tuple[Entry, list[str]]]:
    return [
        (
            Entry(
                audio_id="tone-440",
                filename="tone-440.flac",
                description="440 Hz sine, 30s -- the boring baseline",
                weight=6,
                seconds=30,
                # 0.7 amplitude sine -> rms = 0.7/sqrt(2). The one file in the
                # corpus whose numbers are known in closed form.
                notes={"expected_peak": 0.7, "expected_rms": 0.495},
            ),
            # aevalsrc rather than `sine`, here and below: ffmpeg 9.0's sine
            # generator emits at -18 dBFS, older builds at full scale, and a
            # corpus whose levels depend on the ffmpeg version is a corpus that
            # silently means something different on someone else's machine.
            # An explicit amplitude is also self-documenting.
            ["-f", "lavfi", "-i", "aevalsrc='0.7*sin(2*PI*440*t)':s=16000:d=30"],
        ),
        (
            Entry(
                audio_id="speech-like",
                filename="speech-like.flac",
                description="gated noise bursts, 45s -- envelope for the VAD",
                weight=8,
                seconds=45,
            ),
            ["-f", "lavfi", "-i", _SPEECH_ENVELOPE],
        ),
        (
            Entry(
                audio_id="phone-narrowband",
                filename="phone-narrowband.flac",
                description="300-3400 Hz band-limited noise -- the phone case",
                weight=6,
                seconds=40,
            ),
            # Band-limited content at the canonical rate: what a call recorded
            # over a phone line still looks like after upstream resampled it.
            [
                "-f",
                "lavfi",
                "-i",
                "anoisesrc=color=pink:sample_rate=16000:duration=40",
                "-af",
                "highpass=f=300,lowpass=f=3400",
            ],
        ),
        (
            Entry(
                audio_id="pink-noise",
                filename="pink-noise.flac",
                description="pink noise, 60s -- broadband, the spectral counterweight to a tone",
                weight=5,
                seconds=60,
            ),
            ["-f", "lavfi", "-i", "anoisesrc=color=pink:sample_rate=16000:duration=60"],
        ),
        (
            Entry(
                audio_id="digital-silence-30s",
                filename="digital-silence-30s.flac",
                description="true digital silence -- RMS 0, log10(0), the SKIPPED path",
                weight=2,
                seconds=30,
                notes={"expected_rms": 0.0},
            ),
            ["-f", "lavfi", "-i", "anullsrc=r=16000:cl=mono:d=30"],
        ),
        (
            Entry(
                audio_id="clipped-hot-master",
                filename="clipped-hot-master.flac",
                description="sine driven 2.5x into hard clipping -- saturated at full scale",
                weight=2,
                seconds=20,
                notes={"expected_clipping": True},
            ),
            # Amplitude 2.5 into a 16-bit container: the encoder hard-clips it,
            # which is exactly how a real over-hot master arrives.
            ["-f", "lavfi", "-i", "aevalsrc='2.5*sin(2*PI*220*t)':s=16000:d=20"],
        ),
        (
            Entry(
                audio_id="dc-offset-drift",
                filename="dc-offset-drift.flac",
                description="+0.3 DC offset under a 200 Hz tone -- skews every mean-based figure",
                weight=2,
                seconds=25,
                notes={"expected_dc_offset": 0.3},
            ),
            ["-f", "lavfi", "-i", "aevalsrc='0.3+0.4*sin(2*PI*200*t)':s=16000:d=25"],
        ),
        (
            Entry(
                audio_id="long-call-5min",
                filename="long-call-5min.flac",
                description="5 minutes -- the spec's typical call, and the load unit",
                weight=4,
                seconds=300,
            ),
            [
                "-f",
                "lavfi",
                "-i",
                "aevalsrc='0.35*random(0)*gt(sin(2*PI*0.4*t),-0.2)':s=16000:d=300",
            ],
        ),
        (
            Entry(
                audio_id="very-quiet-minus-60db",
                filename="very-quiet-minus-60db.flac",
                description="-60 dBFS tone -- near the noise floor without being silent",
                weight=2,
                seconds=20,
                # -60 dBFS peak. RMS of a sine sits 3 dB below its peak, so the
                # dbfs a function reports from RMS is -63, not -60.
                notes={"expected_peak_dbfs": -60.0, "expected_rms_dbfs": -63.0},
            ),
            # 0.001 amplitude is -60 dBFS by definition. Going through
            # `volume=-60dB` instead would stack on top of whatever level the
            # generator happened to produce.
            ["-f", "lavfi", "-i", "aevalsrc='0.001*sin(2*PI*1000*t)':s=16000:d=20"],
        ),
    ]


def _damaged(out_dir: Path) -> list[Entry]:
    """Files that exist and are meant to go wrong. The DLQ path needs traffic
    too -- and one of these turns out not to fail at all, which is the more
    interesting case."""
    entries = []

    # A real FLAC with its second half removed. The header survives, so it
    # hydrates *successfully* -- and its STREAMINFO still claims 300 seconds
    # while only ~150 of them decode. Now that the hydrator copies bytes rather
    # than re-encoding them, that lie is carried into the reference verbatim,
    # which makes this the nastier case rather than a decode failure.
    #
    # Nothing in the pipeline compares the duration a function measures against
    # the one the reference claims -- except duration_rms, which reports both
    # precisely so this is visible rather than silently resolved. That makes
    # this entry the end-to-end test of that decision.
    source = out_dir / "long-call-5min.flac"
    if source.exists():
        raw = source.read_bytes()
        truncated = out_dir / "truncated-midstream.flac"
        truncated.write_bytes(raw[: len(raw) // 2])
        entries.append(
            Entry(
                audio_id="truncated-midstream",
                filename=truncated.name,
                description=(
                    "5-minute FLAC cut in half -- header still claims 300s, so measured "
                    "duration disagrees with the reference's claim"
                ),
                expect="hydrates",
                weight=1,
                seconds=150,
                source="damaged",
                notes={"duration_disagrees_with_reference": True},
            )
        )

    empty = out_dir / "zero-bytes.flac"
    empty.write_bytes(b"")
    entries.append(
        Entry(
            audio_id="zero-bytes",
            filename=empty.name,
            description="zero-length object -- no header at all",
            expect="poison",
            weight=1,
            source="damaged",
        )
    )

    not_audio = out_dir / "html-error-page.flac"
    not_audio.write_bytes(b"<!doctype html>\n<html><body><h1>502 Bad Gateway</h1></body></html>\n")
    entries.append(
        Entry(
            audio_id="html-error-page",
            filename=not_audio.name,
            description="an upstream error page served as audio -- the likeliest real incident",
            expect="poison",
            weight=1,
            source="damaged",
        )
    )

    # No file written for this one on purpose: the Audio API 404s it, which is
    # a different poison path from bytes that are not FLAC.
    entries.append(
        Entry(
            audio_id="never-existed",
            filename="",
            content_type="",
            description="no object at all -- Audio API 404, poison, straight to the DLQ",
            expect="missing",
            weight=1,
            source="damaged",
        )
    )
    return entries


def _discover_real_samples(samples_dir: Path, out_dir: Path) -> list[Entry]:
    """Any FLAC you dropped in `samples/`, copied in verbatim.

    Deliberately no re-encoding, no normalising, no renaming beyond the audio
    id: whatever is odd about a real recording is the reason it is worth having
    in the corpus, and the Audio API would have handed it over untouched too.
    Non-canonical files are left in place rather than converted -- the hydrator
    would reject them, and quietly fixing them here would hide that.
    """
    if not samples_dir.is_dir():
        return []

    entries = []
    for path in sorted(samples_dir.iterdir()):
        if path.suffix.lower() != ".flac":
            continue
        audio_id = f"real-{path.stem.lower().replace(' ', '-')}"
        destination = out_dir / f"{audio_id}.flac"
        shutil.copyfile(path, destination)
        entries.append(
            Entry(
                audio_id=audio_id,
                filename=destination.name,
                description=f"real recording dropped into samples/ ({path.name})",
                # Weighted heavily: if you went to the trouble of supplying real
                # audio, the run should mostly be that.
                weight=10,
                source="samples",
            )
        )
    return entries


def build(out_dir: Path, samples_dir: Path, force: bool = False) -> list[Entry]:
    if shutil.which(FFMPEG) is None:
        raise SystemExit(
            f"ffmpeg not found as {FFMPEG!r}. The corpus is real encoded audio, so there "
            f"is no useful fallback -- install ffmpeg or set FAAS_FFMPEG."
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    entries: list[Entry] = []

    for entry, input_args in _generated():
        target = out_dir / entry.filename
        if target.exists() and not force:
            print(f"  = {entry.filename} (exists)")
        else:
            _run_ffmpeg(input_args, target)
            print(f"  + {entry.filename}")
        entries.append(entry)

    entries.extend(_damaged(out_dir))
    real = _discover_real_samples(samples_dir, out_dir)
    entries.extend(real)

    manifest = out_dir / "manifest.json"
    manifest.write_text(json.dumps([asdict(e) for e in entries], indent=2) + "\n")

    hydrates = sum(1 for e in entries if e.expect == "hydrates")
    print(
        f"\n{len(entries)} entries -> {manifest} "
        f"({hydrates} expected to hydrate, {len(entries) - hydrates} expected to fail, "
        f"{len(real)} from samples/)"
    )
    return entries


def _run_ffmpeg(input_args: list[str], target: Path) -> None:
    command = [
        FFMPEG,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        *input_args,
        *CANONICAL_ARGS,
        str(target),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"ffmpeg failed for {target.name}:\n{result.stderr.strip()}")


def load_manifest(out_dir: Path) -> list[Entry]:
    data = json.loads((out_dir / "manifest.json").read_text())
    return [Entry(**item) for item in data]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("corpus"))
    parser.add_argument("--samples", type=Path, default=Path("samples"))
    parser.add_argument("--force", action="store_true", help="regenerate files that already exist")
    args = parser.parse_args(argv)

    build(args.out.resolve(), args.samples.resolve(), force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())
