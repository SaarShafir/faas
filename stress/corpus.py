"""The audio corpus for the local stack.

Real encoded files -- ffmpeg writes them, ffmpeg reads them back in the
hydrator, libsndfile decodes the FLAC on the function side. Nothing here is a
fixture pretending to be audio.

The content is synthetic, and the point of each entry is a property that breaks
something: silence has no RMS to speak of, clipped audio saturates, a DC offset
skews every mean, 8 kHz narrowband is the phone case, a truncated file makes
ffmpeg fail mid-decode and a zero-byte one fails before it starts. A corpus of
ten clean pop songs would prove considerably less.

Drop your own recordings into `samples/` and they join the corpus as-is, at
whatever rate and format they already are -- see `_discover_real_samples`.

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

# Extensions we hand to the Audio API as-is. ffmpeg in the hydrator decides
# whether it can actually decode them; that is the hydrator's job, not ours.
SAMPLE_SUFFIXES = {".wav", ".mp3", ".flac", ".m4a", ".ogg", ".opus", ".aac", ".wma"}

CONTENT_TYPES = {
    ".wav": "audio/wav",
    ".mp3": "audio/mpeg",
    ".flac": "audio/flac",
    ".m4a": "audio/mp4",
    ".ogg": "audio/ogg",
    ".opus": "audio/opus",
    ".aac": "audio/aac",
    ".wma": "audio/x-ms-wma",
}


@dataclass
class Entry:
    """One corpus file, plus what the stack is supposed to do with it."""

    audio_id: str
    filename: str
    content_type: str
    description: str
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
# (entry, ffmpeg input args, ffmpeg output args).

_SPEECH_ENVELOPE = (
    # Noise gated by a slow square wave: bursts of energy separated by silence,
    # which is the only property the VAD and silence-ratio functions care
    # about. It is not speech, and it is not pretending to be.
    "aevalsrc='0.4*random(0)*gt(sin(2*PI*0.7*t),0)':s=16000:d=45"
)


def _generated() -> list[tuple[Entry, list[str], list[str]]]:
    return [
        (
            Entry(
                audio_id="tone-440-stereo-44k",
                filename="tone-440-stereo-44k.wav",
                content_type="audio/wav",
                description="440 Hz sine, stereo, 44.1 kHz, 30s -- the boring baseline",
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
            # An explicit amplitude is also self-documenting. Two expressions
            # means two real channels: generating mono and letting `-ac 2`
            # upmix costs 3 dB per channel, which would make the closed-form
            # numbers above wrong.
            [
                "-f",
                "lavfi",
                "-i",
                "aevalsrc='0.7*sin(2*PI*440*t)|0.7*sin(2*PI*440*t)':s=44100:d=30",
            ],
            ["-c:a", "pcm_s16le"],
        ),
        (
            Entry(
                audio_id="speech-like-16k-mono",
                filename="speech-like-16k-mono.mp3",
                content_type="audio/mpeg",
                description="gated noise bursts, 16 kHz mono mp3, 45s -- envelope for the VAD",
                weight=8,
                seconds=45,
            ),
            ["-f", "lavfi", "-i", _SPEECH_ENVELOPE],
            ["-ac", "1", "-ar", "16000", "-c:a", "libmp3lame", "-b:a", "64k"],
        ),
        (
            Entry(
                audio_id="phone-8k-narrowband",
                filename="phone-8k-narrowband.mp3",
                content_type="audio/mpeg",
                description="300-3400 Hz noise at 8 kHz -- phone case, upsampled by hydrator",
                weight=6,
                seconds=40,
            ),
            ["-f", "lavfi", "-i", "anoisesrc=color=pink:sample_rate=8000:duration=40"],
            [
                "-af",
                "highpass=f=300,lowpass=f=3400",
                "-ac",
                "1",
                "-ar",
                "8000",
                "-c:a",
                "libmp3lame",
                "-b:a",
                "32k",
            ],
        ),
        (
            Entry(
                audio_id="pink-noise-48k-stereo",
                filename="pink-noise-48k-stereo.flac",
                content_type="audio/flac",
                description="pink noise, 48 kHz stereo FLAC, 60s -- lossless, still transcoded",
                weight=5,
                seconds=60,
            ),
            ["-f", "lavfi", "-i", "anoisesrc=color=pink:sample_rate=48000:duration=60"],
            ["-ac", "2", "-c:a", "flac"],
        ),
        (
            Entry(
                audio_id="digital-silence-30s",
                filename="digital-silence-30s.flac",
                content_type="audio/flac",
                description="true digital silence -- RMS 0, log10(0), the SKIPPED path",
                weight=2,
                seconds=30,
                notes={"expected_rms": 0.0},
            ),
            ["-f", "lavfi", "-i", "anullsrc=r=16000:cl=mono:d=30"],
            ["-c:a", "flac"],
        ),
        (
            Entry(
                audio_id="clipped-hot-master",
                filename="clipped-hot-master.wav",
                content_type="audio/wav",
                description="sine driven 20x into hard clipping -- saturated samples at full scale",
                weight=2,
                seconds=20,
                notes={"expected_clipping": True},
            ),
            # Amplitude 2.5 into a 16-bit container: the encoder hard-clips it,
            # which is exactly how a real over-hot master arrives.
            ["-f", "lavfi", "-i", "aevalsrc='2.5*sin(2*PI*220*t)':s=44100:d=20"],
            ["-ac", "1", "-c:a", "pcm_s16le"],
        ),
        (
            Entry(
                audio_id="dc-offset-drift",
                filename="dc-offset-drift.wav",
                content_type="audio/wav",
                description="+0.3 DC offset under a 200 Hz tone -- skews every mean-based figure",
                weight=2,
                seconds=25,
                notes={"expected_dc_offset": 0.3},
            ),
            [
                "-f",
                "lavfi",
                "-i",
                "aevalsrc='0.3+0.4*sin(2*PI*200*t)':s=44100:d=25",
            ],
            ["-ac", "1", "-c:a", "pcm_s16le"],
        ),
        (
            Entry(
                audio_id="stereo-imbalance",
                filename="stereo-imbalance.wav",
                content_type="audio/wav",
                description="left channel 10x the right -- the channel-balance case",
                weight=2,
                seconds=25,
            ),
            # Two expressions means two channels, so the imbalance is in the
            # source rather than a downmix filter that a later ffmpeg might
            # interpret differently.
            [
                "-f",
                "lavfi",
                "-i",
                "aevalsrc='0.6*sin(2*PI*330*t)|0.06*sin(2*PI*330*t)':s=44100:d=25",
            ],
            ["-c:a", "pcm_s16le"],
        ),
        (
            Entry(
                audio_id="long-call-5min",
                filename="long-call-5min.mp3",
                content_type="audio/mpeg",
                description="5 minutes at 16 kHz -- the spec's typical call, and the load unit",
                weight=4,
                seconds=300,
            ),
            [
                "-f",
                "lavfi",
                "-i",
                "aevalsrc='0.35*random(0)*gt(sin(2*PI*0.4*t),-0.2)':s=16000:d=300",
            ],
            ["-ac", "1", "-ar", "16000", "-c:a", "libmp3lame", "-b:a", "64k"],
        ),
        (
            Entry(
                audio_id="very-quiet-minus-60db",
                filename="very-quiet-minus-60db.flac",
                content_type="audio/flac",
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
            ["-c:a", "flac"],
        ),
    ]


def _damaged(out_dir: Path) -> list[Entry]:
    """Files that exist and are meant to go wrong. The DLQ path needs traffic
    too -- and one of these turns out not to fail at all, which is the more
    interesting case."""
    entries = []

    # A real mp3 with its second half removed. Written expecting a decode
    # failure; it is not one. ffmpeg and libsndfile both resync and return the
    # ~150s that survive, with a warning nobody sees.
    #
    # Keeping it, reclassified, because it is the nastier bug: a half-uploaded
    # object hydrates *successfully* into a FLAC of the wrong length. Nothing
    # in the pipeline compares the duration a function measures against the one
    # the reference claims -- except duration_rms, which reports both precisely
    # so this is visible rather than silently resolved. That makes this entry
    # the end-to-end test of that decision.
    source = out_dir / "long-call-5min.mp3"
    if source.exists():
        raw = source.read_bytes()
        truncated = out_dir / "truncated-midstream.mp3"
        truncated.write_bytes(raw[: len(raw) // 2])
        entries.append(
            Entry(
                audio_id="truncated-midstream",
                filename=truncated.name,
                content_type="audio/mpeg",
                description=(
                    "5-minute mp3 cut in half -- decodes fine to ~150s, so measured "
                    "duration disagrees with the reference's claim"
                ),
                expect="hydrates",
                weight=1,
                seconds=150,
                source="damaged",
                notes={"duration_disagrees_with_reference": True},
            )
        )

    empty = out_dir / "zero-bytes.mp3"
    empty.write_bytes(b"")
    entries.append(
        Entry(
            audio_id="zero-bytes",
            filename=empty.name,
            content_type="audio/mpeg",
            description="zero-length object -- fails before decoding starts",
            expect="poison",
            weight=1,
            source="damaged",
        )
    )

    not_audio = out_dir / "html-error-page.mp3"
    not_audio.write_bytes(b"<!doctype html>\n<html><body><h1>502 Bad Gateway</h1></body></html>\n")
    entries.append(
        Entry(
            audio_id="html-error-page",
            filename=not_audio.name,
            content_type="audio/mpeg",
            description="an upstream error page served as audio -- the likeliest real incident",
            expect="poison",
            weight=1,
            source="damaged",
        )
    )

    # No file written for this one on purpose: the Audio API 404s it, which is
    # PoisonMessageError rather than a transcode failure -- a different path.
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
    """Anything you dropped in `samples/`, copied in verbatim.

    Deliberately no transcoding, no normalising, no renaming beyond the audio
    id: whatever is odd about a real recording is the reason it is worth having
    in the corpus.
    """
    if not samples_dir.is_dir():
        return []

    entries = []
    for path in sorted(samples_dir.iterdir()):
        if path.suffix.lower() not in SAMPLE_SUFFIXES:
            continue
        audio_id = f"real-{path.stem.lower().replace(' ', '-')}"
        destination = out_dir / f"{audio_id}{path.suffix.lower()}"
        shutil.copyfile(path, destination)
        entries.append(
            Entry(
                audio_id=audio_id,
                filename=destination.name,
                content_type=CONTENT_TYPES.get(path.suffix.lower(), "application/octet-stream"),
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

    for entry, input_args, output_args in _generated():
        target = out_dir / entry.filename
        if target.exists() and not force:
            print(f"  = {entry.filename} (exists)")
        else:
            _run_ffmpeg(input_args, output_args, target)
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


def _run_ffmpeg(input_args: list[str], output_args: list[str], target: Path) -> None:
    command = [
        FFMPEG,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        *input_args,
        *output_args,
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
