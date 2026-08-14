"""Kill things while the stack is working, and record exactly when.

Run from the host, against the compose project -- it drives `docker compose`
rather than touching Kafka, because the failure being modelled is a pod dying,
which is the failure OpenShift actually produces (evictions, node drains, OOM
kills, rolling deploys).

Two kinds, and the difference matters:

  - **SIGKILL**: no drain, no rebalance callback, no commit. Everything in
    flight is lost and must be redelivered to whoever picks the partitions up.
    This is the one that tells you whether the offset ledger is honest.
  - **SIGTERM**: the runner's own shutdown path -- stop polling, finish what is
    in flight, commit, exit. A clean restart should produce *no* reprocessing,
    and any duplicate after one is a bug in the drain.

The timeline it writes is the point. A duplicate in the monitor's report means
nothing on its own; a duplicate whose call was in flight when a container was
SIGKILLed at 14:02:11 is a redelivery working as designed.

    python -m stress.chaos --service spectral-centroid --signal SIGKILL --after 30
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("chaos")

COMPOSE_DIR = Path(__file__).resolve().parent.parent / "compose"


def _compose(*args: str) -> str:
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=COMPOSE_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"docker compose {' '.join(args)} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(
    *,
    service: str,
    signal: str,
    after: float,
    down_for: float,
    rounds: int,
    runs_dir: Path,
) -> list[dict]:
    events = []

    for round_index in range(rounds):
        if after:
            log.info("waiting %.0fs before round %d", after, round_index + 1)
            time.sleep(after)

        log.info("%s %s", signal, service)
        events.append({"at": _now(), "event": "kill", "service": service, "signal": signal})
        # `kill` hits every replica of the service. For a service with more than
        # one, that is a full outage rather than a rebalance -- which is worth
        # knowing when reading the timeline afterwards.
        _compose("kill", "-s", signal, service)

        time.sleep(down_for)

        log.info("restarting %s", service)
        # `up -d` rather than `start`: it recreates anything compose considers
        # missing, which `start` silently will not.
        _compose("up", "-d", service)
        events.append({"at": _now(), "event": "restart", "service": service})

    runs_dir.mkdir(parents=True, exist_ok=True)
    path = runs_dir / f"chaos-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    path.write_text(json.dumps({"events": events}, indent=2))
    log.info("timeline -> %s", path)
    return events


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service", default="spectral-centroid")
    parser.add_argument("--signal", default="SIGKILL", choices=["SIGKILL", "SIGTERM"])
    parser.add_argument(
        "--after", type=float, default=30.0, help="seconds to wait before each kill"
    )
    parser.add_argument("--down-for", type=float, default=15.0)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument(
        "--runs", type=Path, default=Path(__file__).resolve().parent.parent / "runs"
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run(
        service=args.service,
        signal=args.signal,
        after=args.after,
        down_for=args.down_for,
        rounds=args.rounds,
        runs_dir=args.runs,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
