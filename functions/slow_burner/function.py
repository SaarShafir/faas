"""A function that takes too long, on purpose.

Not an analyzer. This exists so the stress run exercises the timeout path, which
is otherwise the least-tested branch in the platform and the one with the
nastiest known limitation: `ProcessWorkerPool` cannot interrupt a worker
mid-file, so it abandons the outcome and lets the runner retry or DLQ while the
worker keeps burning CPU until it returns. That behaviour is described in
STATUS.md but has never been watched under load with a real broker holding
partitions.

It burns CPU rather than sleeping. A sleeping worker releases the GIL and looks
nothing like a model that is genuinely grinding, and it is the grinding case
that pins a core and starves everything sharing it.

`FAAS_BURN_SECONDS_PER_MINUTE` sets the cost per minute of audio. The default of
20 against a 15s timeout means the 5-minute file times out and the short ones do
not -- a mix, not a wall.
"""

from __future__ import annotations

import json
import os
import time

import numpy as np

from faas_sdk import FunctionResult

BURN_SECONDS_PER_MINUTE = float(os.environ.get("FAAS_BURN_SECONDS_PER_MINUTE", "20"))

SCHEMA_VERSION = "1"


class SlowBurner:
    function_id = "slow_burner"
    function_version = "1.0.0"

    def process(self, ref, audio) -> FunctionResult | None:
        samples = audio.samples()
        if samples.size == 0:
            return FunctionResult.skip()

        minutes = samples.size / audio.sample_rate / 60.0
        budget = minutes * BURN_SECONDS_PER_MINUTE

        started = time.monotonic()
        iterations = 0
        # A real matmul, so the GIL is held and released the way numpy holds and
        # releases it under actual load. A `while True: pass` would be a
        # different kind of busy.
        block = np.random.rand(128, 128)
        while time.monotonic() - started < budget:
            block = block @ block
            # Renormalise: 128x128 matmuls overflow to inf in about 20 rounds,
            # and inf @ inf is nan, which numpy computes far faster than real
            # numbers -- the burn would quietly stop burning.
            block /= np.abs(block).max() or 1.0
            iterations += 1

        payload = {
            "burned_seconds": time.monotonic() - started,
            "budget_seconds": budget,
            "iterations": iterations,
            "audio_minutes": minutes,
        }
        return FunctionResult(
            payload=json.dumps(payload).encode(),
            schema_version=SCHEMA_VERSION,
            content_type="application/json",
        )


def main() -> None:
    from faas_sdk import run

    run(SlowBurner)


if __name__ == "__main__":
    main()
