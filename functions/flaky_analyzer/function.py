"""A function that fails on purpose, deterministically.

Not an analyzer either. It exists so the retry ladder and the DLQ carry real
traffic during a stress run, and so the two failure *kinds* stay distinguishable
end to end:

  - `TransientError` must be retried with backoff and should eventually
    succeed, because the flakiness here is per-attempt.
  - `PoisonMessageError` must go straight to the DLQ on the first attempt,
    without burning the retry budget -- and must do so for the same call every
    single time.

Determinism is the point. The failure is a hash of `call_id`, not a random
draw, so a rerun of the same corpus produces the same failures and a difference
between two runs means something changed in the platform rather than in the
dice. Transient failures use the attempt count as well, so they clear on retry
exactly as a real flaky dependency would.
"""

from __future__ import annotations

import hashlib
import json
import os

from faas_sdk import FunctionResult
from faas_sdk.errors import PoisonMessageError, TransientError

# Share of calls that fail transiently on their first attempt and succeed after.
TRANSIENT_RATE = float(os.environ.get("FAAS_FLAKY_TRANSIENT_RATE", "0.15"))

# Share of calls that can never succeed. These are the ones that must appear in
# the DLQ exactly once each, having consumed exactly one attempt.
POISON_RATE = float(os.environ.get("FAAS_FLAKY_POISON_RATE", "0.05"))

SCHEMA_VERSION = "1"


STATE_DIR = os.environ.get("FAAS_FLAKY_STATE_DIR", "/tmp/faas-flaky-state")


def _first_attempt(call_id: str) -> bool:
    """True the first time a call is seen by this pod, False afterwards.

    A marker file rather than an in-memory set: `ProcessWorkerPool` spawns
    workers, and a retry can land in a different one, where a set would be
    empty and every attempt would look like the first.

    Scoped to the pod, so a rebalance that moves the partition elsewhere resets
    it. That is a fair model of a flaky dependency anyway, and the alternative
    is this test function owning shared state, which no function should.
    """
    marker = os.path.join(STATE_DIR, hashlib.sha256(call_id.encode()).hexdigest()[:32])
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        # O_EXCL makes the check-and-create atomic, so two workers racing on the
        # same call cannot both decide they are first.
        fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o660)
    except FileExistsError:
        return False
    except OSError:
        # Read-only or unwritable scratch: degrade to always-succeeds rather
        # than failing the run for a reason that has nothing to do with the
        # platform under test.
        return False
    os.close(fd)
    return True


def _bucket(call_id: str) -> float:
    """A stable [0, 1) position for a call id.

    hashlib rather than hash(): Python's hash is salted per process, so with it
    the same call would fail in one worker and pass in another, and nothing
    about a run would be reproducible.
    """
    digest = hashlib.sha256(call_id.encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


class FlakyAnalyzer:
    function_id = "flaky_analyzer"
    function_version = "1.0.0"

    def process(self, ref, audio) -> FunctionResult | None:
        position = _bucket(ref.call_id)

        if position < POISON_RATE:
            raise PoisonMessageError(
                f"{ref.call_id} is in the permanently-broken bucket",
                code="SYNTHETIC_POISON",
            )

        if position < POISON_RATE + TRANSIENT_RATE and _first_attempt(ref.call_id):
            # Fail once, then succeed -- otherwise this bucket exhausts its
            # retry budget and lands in the DLQ looking exactly like the poison
            # bucket, and the run proves nothing about recovery.
            #
            # The marker on disk is here because a function cannot see its own
            # attempt count: §5.1 hands `process` only (ref, audio), and the
            # attempt lives on the §6 envelope, which is deliberately the SDK's.
            # A real function has no way to say "this is my last attempt, return
            # something degraded rather than nothing".
            raise TransientError(
                f"{ref.call_id} is in the flaky bucket (first attempt)",
                code="SYNTHETIC_TRANSIENT",
            )

        # Touch the audio so this function has the same fetch and decode cost as
        # a real one; a failure path that skips the download is not the failure
        # path being tested.
        samples = audio.samples()
        payload = {
            "bucket": position,
            "samples": int(samples.size),
            "verdict": "ok",
        }
        return FunctionResult(
            payload=json.dumps(payload).encode(),
            schema_version=SCHEMA_VERSION,
            content_type="application/json",
        )


def main() -> None:
    from faas_sdk import run

    run(FlakyAnalyzer)


if __name__ == "__main__":
    main()
