"""Publish call records onto the input topic (spec §4.1).

Stands in for whatever owns call metadata in production. It writes the two
fields the hydrator's `JsonSourceDecoder` is configured to read plus a few
extras, on purpose: §4.2's passthrough is supposed to carry fields the platform
does not model, and a seeder that writes only the modelled fields would never
test that.

Draws from the corpus by weight, so most of the load is ordinary audio and the
awkward files appear at a realistic rate rather than a synthetic 50%.

    python -m stress.seed --calls 500 --rate 20

Writes a run file naming every call it produced and the audio behind it. The
monitor needs that to tell "this call is missing" from "this call was never
sent", which is the whole difference between a real loss and a slow consumer.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .corpus import load_manifest

log = logging.getLogger("seeder")

INPUT_TOPIC = os.environ.get("FAAS_INPUT_TOPIC", "faas.calls.raw")


def build_producer(bootstrap: str):
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            # Same guarantees the SDK's own producer asks for. A seeder that
            # silently dropped records would make every loss investigation
            # start in the wrong place.
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "compression.type": "zstd",
        }
    )


def seed(
    *,
    bootstrap: str,
    corpus_dir: Path,
    calls: int,
    rate: float,
    run_dir: Path,
    seed_value: int,
    topic: str = INPUT_TOPIC,
) -> dict:
    entries = [e for e in load_manifest(corpus_dir) if e.weight > 0]
    if not entries:
        raise SystemExit(f"no corpus entries in {corpus_dir}")

    # Seeded RNG: two runs with the same seed send the same audio in the same
    # order, so a difference between runs is the platform's, not the corpus's.
    rng = random.Random(seed_value)
    weights = [e.weight for e in entries]

    producer = build_producer(bootstrap)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    records = []

    interval = 1.0 / rate if rate > 0 else 0.0
    started = time.monotonic()
    delivery_errors = []

    def on_delivery(err, msg):
        if err is not None:
            delivery_errors.append(str(err))

    for index in range(calls):
        entry = rng.choices(entries, weights=weights, k=1)[0]
        call_id = f"{run_id}-{index:06d}-{uuid.uuid4().hex[:8]}"
        record = {
            "call_id": call_id,
            "audio_id": entry.audio_id,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            # Unmodelled fields, carried through §4.2's passthrough. Nothing in
            # the platform reads these; that is the point.
            "tenant": rng.choice(["acme", "globex", "initech"]),
            "channel": rng.choice(["inbound", "outbound"]),
            "agent_id": f"agent-{rng.randint(1, 50)}",
        }
        producer.produce(
            topic=topic,
            key=call_id.encode(),
            value=json.dumps(record).encode(),
            on_delivery=on_delivery,
        )
        producer.poll(0)

        records.append(
            {
                "call_id": call_id,
                "audio_id": entry.audio_id,
                "expect": entry.expect,
                "seconds": entry.seconds,
            }
        )

        if interval:
            # Absolute schedule rather than sleep(interval): sleeping the
            # interval each time makes the real rate drift below the target by
            # however long produce() took.
            target = started + (index + 1) * interval
            drift = target - time.monotonic()
            if drift > 0:
                time.sleep(drift)

        if (index + 1) % 100 == 0:
            log.info("produced %d/%d", index + 1, calls)

    outstanding = producer.flush(60)
    elapsed = time.monotonic() - started

    if outstanding:
        raise SystemExit(f"{outstanding} records never left the producer")
    if delivery_errors:
        raise SystemExit(f"{len(delivery_errors)} delivery failures, first: {delivery_errors[0]}")

    run = {
        "run_id": run_id,
        "topic": topic,
        "calls": calls,
        "target_rate": rate,
        "actual_rate": calls / elapsed if elapsed else 0.0,
        "elapsed_seconds": elapsed,
        "seed": seed_value,
        "audio_seconds": sum(r["seconds"] for r in records),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "records": records,
    }

    run_dir.mkdir(parents=True, exist_ok=True)
    run_file = run_dir / f"seed-{run_id}.json"
    run_file.write_text(json.dumps(run, indent=2))
    # Stable name so the monitor can find the latest run without being told.
    (run_dir / "latest-seed.json").write_text(json.dumps(run, indent=2))

    log.info(
        "seeded %d calls in %.1fs (%.1f/s, %.0f seconds of audio) -> %s",
        calls,
        elapsed,
        run["actual_rate"],
        run["audio_seconds"],
        run_file,
    )
    return run


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calls", type=int, default=500)
    parser.add_argument(
        "--rate",
        type=float,
        default=20.0,
        help="calls per second; 0 for as fast as the producer will go",
    )
    parser.add_argument("--corpus", type=Path, default=Path("/corpus"))
    parser.add_argument("--runs", type=Path, default=Path("/runs"))
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=os.environ.get("FAAS_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    seed(
        bootstrap=args.bootstrap,
        corpus_dir=args.corpus,
        calls=args.calls,
        rate=args.rate,
        run_dir=args.runs,
        seed_value=args.seed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
