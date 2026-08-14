"""A stand-in for the Audio API (spec §4.1), serving the generated corpus.

Stdlib only, because this runs in a container that should need nothing beyond
the SDK image. It implements exactly the contract `AudioApiClient` codes
against, which is a small one: 200 with bytes, 404/410 for a recording that is
genuinely gone, anything else transient.

The fault injection is the reason this is not just `python -m http.server`.
`FAAS_STUB_ERROR_RATE` makes a share of requests fail with a 503, which the
client turns into `TransientError` and the runner retries with backoff --
without it, a stress run never exercises the retry path at all, and the whole
retry/backoff/DLQ ladder stays theoretical.

    python -m stress.audio_api --corpus corpus --port 8080
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .corpus import Entry, load_manifest

log = logging.getLogger("audio-api")


class Corpus:
    def __init__(self, root: Path):
        self.root = root
        self.entries: dict[str, Entry] = {}
        for entry in load_manifest(root):
            self.entries[entry.audio_id] = entry
        self.hits: dict[str, int] = {}
        self._lock = threading.Lock()

    def read(self, audio_id: str) -> tuple[bytes, str] | None:
        entry = self.entries.get(audio_id)
        if entry is None or not entry.filename:
            return None
        path = self.root / entry.filename
        if not path.exists():
            return None
        with self._lock:
            self.hits[audio_id] = self.hits.get(audio_id, 0) + 1
        return path.read_bytes(), entry.content_type or "application/octet-stream"


class Handler(BaseHTTPRequestHandler):
    corpus: Corpus
    error_rate: float = 0.0
    latency_ms: float = 0.0
    # Recordings the Audio API reports as deleted rather than merely absent.
    # 410 is the §12 deletion case and must stay distinguishable from a 404.
    gone: set = frozenset()

    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's interface
        path = self.path.split("?")[0]

        if path == "/healthz":
            return self._send(200, b'{"status":"ok"}', "application/json")

        if path == "/manifest":
            body = json.dumps(
                [
                    {"audio_id": e.audio_id, "expect": e.expect, "weight": e.weight}
                    for e in self.corpus.entries.values()
                ]
            ).encode()
            return self._send(200, body, "application/json")

        if path == "/stats":
            return self._send(200, json.dumps(self.corpus.hits).encode(), "application/json")

        if not path.startswith("/audio/"):
            return self._send(404, b"not found", "text/plain")

        audio_id = path[len("/audio/") :]

        if self.latency_ms:
            # Jittered, because a constant delay is the one latency profile
            # that never shows up in production.
            time.sleep(random.uniform(0, 2 * self.latency_ms) / 1000.0)

        if self.error_rate and random.random() < self.error_rate:
            # 503 -> TransientError -> the runner's retry ladder. The file is
            # fine; this is the flaky-upstream case the retry budget exists for.
            return self._send(503, b"upstream busy", "text/plain")

        if audio_id in self.gone:
            return self._send(410, b"recording deleted", "text/plain")

        found = self.corpus.read(audio_id)
        if found is None:
            return self._send(404, b"no such recording", "text/plain")

        body, content_type = found
        return self._send(200, body, content_type)

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        # The default logs every request to stderr, which at stress-test rates
        # is both noise and a measurable share of the process's time.
        if log.isEnabledFor(logging.DEBUG):
            log.debug(fmt, *args)


def serve(corpus_dir: Path, port: int, error_rate: float, latency_ms: float, gone: set) -> None:
    Handler.corpus = Corpus(corpus_dir)
    Handler.error_rate = error_rate
    Handler.latency_ms = latency_ms
    Handler.gone = frozenset(gone)

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    log.info(
        "serving %d recordings on :%d (error_rate=%.2f latency_ms=%.0f gone=%s)",
        len(Handler.corpus.entries),
        port,
        error_rate,
        latency_ms,
        sorted(gone) or "none",
    )
    server.serve_forever()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--corpus", type=Path, default=Path(os.environ.get("FAAS_CORPUS_DIR", "corpus"))
    )
    parser.add_argument("--port", type=int, default=int(os.environ.get("FAAS_STUB_PORT", "8080")))
    parser.add_argument(
        "--error-rate",
        type=float,
        default=float(os.environ.get("FAAS_STUB_ERROR_RATE", "0")),
        help="share of requests answered with 503, to exercise the retry ladder",
    )
    parser.add_argument(
        "--latency-ms",
        type=float,
        default=float(os.environ.get("FAAS_STUB_LATENCY_MS", "0")),
        help="mean added latency, jittered uniformly over [0, 2x]",
    )
    parser.add_argument(
        "--gone",
        default=os.environ.get("FAAS_STUB_GONE", ""),
        help="comma-separated audio ids to answer with 410 (the §12 deletion case)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=os.environ.get("FAAS_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    gone = {g.strip() for g in args.gone.split(",") if g.strip()}
    serve(args.corpus.resolve(), args.port, args.error_rate, args.latency_ms, gone)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
