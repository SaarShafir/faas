"""Run a function against real audio, without deploying it.

This is the fast feedback loop a function author does not otherwise have: today
the only way to find out what `process()` returns for a 5-minute narrowband
call is to build an image, push it, and read the results topic. Here it is a
button.

**This executes arbitrary Python behind an HTTP request.** That is the whole
feature and it is also the reason it is off unless `FAAS_CONSOLE_SANDBOX=1`,
and the reason the console must never be exposed without authentication once
it is on. Three things narrow the blast radius:

  - a subprocess, so an infinite loop or a segfault costs a process rather than
    the console;
  - a wall-clock timeout, killed rather than waited on;
  - the same canonical mono 16 kHz FLAC a real function receives -- the corpus
    file unchanged, since the hydrator stores what the Audio API served -- so
    what you see here is what the platform will do.

It is not a security boundary. A subprocess with a timeout stops accidents, not
an attacker: edited code runs with the console's own privileges. Anyone who can
reach this page can run code in this container, which is exactly why the flag
exists and why it stays off by default.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

ENABLED = os.environ.get("FAAS_CONSOLE_SANDBOX", "").strip().lower() in ("1", "true", "yes")
CORPUS_DIR = Path(os.environ.get("FAAS_CORPUS_DIR", "/corpus"))
TIMEOUT_SECONDS = float(os.environ.get("FAAS_CONSOLE_SANDBOX_TIMEOUT", "60"))


class SandboxDisabled(RuntimeError):
    pass


@dataclass
class RunResult:
    ok: bool
    status: str = ""
    payload: str = ""
    schema_version: str = ""
    content_type: str = ""
    seconds: float = 0.0
    audio_seconds: float = 0.0
    stdout: str = ""
    error: str = ""

    @property
    def realtime_multiple(self) -> float | None:
        """§8's onboarding floor is 25x. Worth knowing before the capacity
        review rather than during it."""
        if self.seconds <= 0 or not self.audio_seconds:
            return None
        return self.audio_seconds / self.seconds


def available_audio() -> list[dict]:
    """Corpus entries that can actually be fed to a function.

    The deliberately broken ones are excluded: they fail in the hydrator, which
    is upstream of anything a function sees, so offering them here would only
    ever produce a confusing error about a file that was never meant to decode.
    """
    manifest = CORPUS_DIR / "manifest.json"
    if not manifest.exists():
        return []
    try:
        entries = json.loads(manifest.read_text())
    except ValueError:
        return []
    return [
        {
            "audio_id": e["audio_id"],
            "filename": e.get("filename", ""),
            "description": e.get("description", ""),
            "seconds": e.get("seconds", 0),
        }
        for e in entries
        if e.get("expect") == "hydrates" and e.get("filename")
    ]


# The child process. Kept as source rather than a module so the sandbox has no
# import path into the console itself -- it decodes and calls `process`, and can
# do nothing else by accident.
_CHILD = '''
import io, json, sys, time, types

spec = json.loads(sys.stdin.read())

from faas_hydrator.flac import read_streaminfo
import soundfile

# Corpus files are canonical FLAC already, exactly as the Audio API serves
# them, so the sandbox hands the function the same bytes the hydrator would
# have stored. Duration comes from STREAMINFO for the same reason it does in
# the reference: it is what the platform would have claimed.
flac = open(spec["audio_path"], "rb").read()
info = read_streaminfo(flac)
samples, sample_rate = soundfile.read(io.BytesIO(flac), dtype="float32")

from faas_sdk.models import AudioReference

class Handle:
    """The slice of AudioHandle a function touches, over the real decode."""
    def __init__(self):
        self.sample_rate = sample_rate
    def samples(self):
        return samples
    def bytes(self):
        return flac

module = types.ModuleType("sandboxed_function")
module.__dict__["__name__"] = "sandboxed_function"
exec(compile(spec["source"], "<console>", "exec"), module.__dict__)

factory = None
for value in vars(module).values():
    if isinstance(value, type) and hasattr(value, "process") and hasattr(value, "function_id"):
        factory = value
        break
if factory is None:
    print(json.dumps({"ok": False, "error": "no class with process() and function_id found"}))
    raise SystemExit(0)

ref = AudioReference(
    call_id="sandbox",
    object_key="sandbox.flac",
    sample_rate=sample_rate,
    duration_seconds=info.duration_seconds,
)

started = time.monotonic()
result = factory().process(ref, Handle())
elapsed = time.monotonic() - started

if result is None:
    out = {"ok": True, "status": "SKIPPED", "payload": "", "seconds": elapsed}
else:
    out = {
        "ok": True,
        "status": result.status.name,
        "payload": result.payload.decode("utf-8", "replace") if result.payload else "",
        "schema_version": result.schema_version,
        "content_type": result.content_type,
        "seconds": elapsed,
    }
out["audio_seconds"] = samples.size / sample_rate if sample_rate else 0.0
print("\\n__FAAS_RESULT__" + json.dumps(out))
'''


def run(source: str, audio_id: str) -> RunResult:
    """Execute `source` against one corpus file and return what it produced."""
    if not ENABLED:
        raise SandboxDisabled(
            "the sandbox is off. It executes arbitrary Python behind an HTTP "
            "request, so it is opt-in: set FAAS_CONSOLE_SANDBOX=1, and never on "
            "a console that is reachable without authentication."
        )

    entry = next((a for a in available_audio() if a["audio_id"] == audio_id), None)
    if entry is None:
        return RunResult(ok=False, error=f"no corpus audio called {audio_id!r}")

    audio_path = CORPUS_DIR / entry["filename"]
    spec = json.dumps({"source": source, "audio_path": str(audio_path)})

    with tempfile.TemporaryDirectory(prefix="faas-sandbox-") as scratch:
        child = Path(scratch) / "child.py"
        child.write_text(textwrap.dedent(_CHILD))
        try:
            completed = subprocess.run(
                [sys.executable, str(child)],
                input=spec,
                capture_output=True,
                text=True,
                timeout=TIMEOUT_SECONDS,
                cwd=scratch,
            )
        except subprocess.TimeoutExpired:
            # Killed, not waited on. A function that cannot finish inside the
            # timeout here would be dead-lettered by its own
            # per_file_timeout_seconds in production anyway.
            return RunResult(
                ok=False,
                error=f"still running after {TIMEOUT_SECONDS:.0f}s -- killed. "
                f"In production this file would have hit per_file_timeout_seconds "
                f"and gone to the DLQ.",
            )

    stdout = completed.stdout
    marker = "__FAAS_RESULT__"
    if marker not in stdout:
        # The traceback is the useful part, and it is the whole reason to run
        # this in the console rather than in a pod nobody can see.
        return RunResult(
            ok=False,
            stdout=stdout.strip(),
            error=(completed.stderr or "the function produced no result").strip()[-4000:],
        )

    body, _, encoded = stdout.rpartition(marker)
    payload = json.loads(encoded)
    return RunResult(
        ok=payload.get("ok", False),
        status=payload.get("status", ""),
        payload=payload.get("payload", ""),
        schema_version=payload.get("schema_version", ""),
        content_type=payload.get("content_type", ""),
        seconds=payload.get("seconds", 0.0),
        audio_seconds=payload.get("audio_seconds", 0.0),
        stdout=body.strip(),
        error=payload.get("error", ""),
    )
