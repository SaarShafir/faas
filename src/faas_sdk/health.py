"""Readiness and liveness, for a platform that has to decide when to kill a pod.

Without these the only open port is `/metrics`, and a runner that has wedged its
poll loop or lost every partition serves metrics quite happily. OpenShift then
has no way to tell a working pod from a stuck one, so a stuck one stays in the
rotation for ever.

The two probes answer genuinely different questions and conflating them is the
usual mistake:

  - **Liveness: is the poll loop still turning?** If it has not run in longer
    than a poll interval plus a wide margin, the process is wedged and only a
    restart will help. This must *not* fail merely because work is slow --
    killing a pod that is legitimately busy with a 5-minute file turns a slow
    call into a redelivered one, which is the §5.2 failure by another route.
  - **Readiness: does this pod hold any partitions?** During a rebalance it
    holds none through no fault of its own, so this is a routing signal, not a
    health one, and it must never gate a restart.

The endpoint is deliberately not the metrics port. `/metrics` answering 200 is
what makes the "healthy but stuck" case invisible in the first place.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger(__name__)

# How stale the poll loop may get before liveness fails. Generous on purpose:
# `max.poll.interval.ms` is already per_file_timeout x in_flight + 60s, and a
# probe that fires sooner than the broker's own eviction would kill pods the
# coordinator still considers healthy.
DEFAULT_STALE_AFTER = 300.0


class HealthState:
    """What the probes read. Updated by the runner, read by the HTTP server."""

    def __init__(self, stale_after: float = DEFAULT_STALE_AFTER, clock=time.monotonic):
        self._clock = clock
        self.stale_after = stale_after
        self._lock = threading.Lock()
        self._last_loop = clock()
        self._partitions = 0
        self._started = False
        self._stopping = False

    # -- written by the runner --------------------------------------------

    def loop_ran(self) -> None:
        with self._lock:
            self._last_loop = self._clock()

    def assigned(self, count: int) -> None:
        with self._lock:
            self._partitions = count
            self._started = True

    def stopping(self) -> None:
        """Drain has begun. Readiness fails immediately so nothing new is
        routed here, while liveness stays true so the drain is not cut short by
        a restart."""
        with self._lock:
            self._stopping = True

    # -- read by the probes ------------------------------------------------

    def liveness(self) -> tuple[bool, dict]:
        with self._lock:
            age = self._clock() - self._last_loop
            alive = age < self.stale_after
            return alive, {
                "alive": alive,
                "seconds_since_poll": round(age, 1),
                "stale_after": self.stale_after,
            }

    def readiness(self) -> tuple[bool, dict]:
        with self._lock:
            ready = self._started and not self._stopping and self._partitions > 0
            return ready, {
                "ready": ready,
                "partitions": self._partitions,
                "subscribed": self._started,
                "stopping": self._stopping,
            }


class _Handler(BaseHTTPRequestHandler):
    state: HealthState

    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's interface
        path = self.path.split("?")[0]
        if path in ("/healthz", "/livez"):
            ok, body = self.state.liveness()
        elif path in ("/readyz", "/ready"):
            ok, body = self.state.readiness()
        else:
            ok, body = False, {"error": "not found"}

        payload = json.dumps(body).encode()
        self.send_response(200 if ok else 503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        # Kubelet probes every few seconds per pod; logging each one buries
        # everything else.
        if log.isEnabledFor(logging.DEBUG):
            log.debug(fmt, *args)


def serve(state: HealthState, port: int | None = None) -> ThreadingHTTPServer | None:
    """Start the probe server on a daemon thread.

    Returns None when disabled or unable to bind. A pod that cannot serve
    probes is worse off, but a pod that refuses to start because of it cannot
    process audio at all -- the same trade metrics and events make.
    """
    port = port if port is not None else int(os.environ.get("FAAS_HEALTH_PORT", "8080"))
    if port <= 0:
        return None

    _Handler.state = state
    try:
        server = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    except OSError as exc:
        log.error("could not bind the health port %d (%s); probes are unavailable", port, exc)
        return None

    threading.Thread(target=server.serve_forever, daemon=True, name="faas-health").start()
    log.info("health on :%d (/healthz, /readyz)", port)
    return server
