"""Time, injected.

Retry backoff, per-file timeouts and commit intervals are all clock-driven. If
the clock is not a seam, testing any of them means sleeping.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)
