"""Watch a stress run and say what actually happened.

There is no results sink yet -- that is build-order step 5 -- so this consumes
the results topic directly, in its own consumer group, alongside every DLQ.

What it reports, and why each one is here:

  - **Per-call accounting.** Exactly one result per (call, function). Duplicates
    mean redelivery that the ledger did not absorb; missing means the pipeline
    lost something or is still catching up, and the seed file is what tells
    those two apart.
  - **Throughput and realtime speedup.** §8 sets an onboarding floor of 25x
    realtime. This is the number that says whether a function clears it.
  - **Lag.** Committed offset against the high water mark, per consumer group,
    which is what the autoscaler would scale on (§5.5, §11) and the first place
    a stuck function shows up.
  - **DLQ traffic by error code.** A run with an empty DLQ has not tested the
    DLQ; the interesting question is whether what landed there is what was
    supposed to.

    python -m stress.monitor --expect-from /runs/latest-seed.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("monitor")

RESULTS_TOPIC = os.environ.get("FAAS_RESULTS_TOPIC", "faas.results")
INTERNAL_TOPIC = os.environ.get("FAAS_INTERNAL_TOPIC", "faas.audio.internal")

FUNCTIONS = [
    "duration_rms",
    "silence_ratio",
    "clipping_detect",
    "zero_crossing_rate",
    "spectral_centroid",
    "spectral_rolloff",
    "snr_estimate",
    "energy_vad",
    "slow_burner",
    "flaky_analyzer",
]

DLQ_TOPICS = ["faas.dlq.hydrator"] + [f"faas.dlq.{f}" for f in FUNCTIONS]

# §8's onboarding floor: a function must process audio at least this much faster
# than realtime to be allowed onto the platform.
REALTIME_FLOOR = 25


class FunctionTally:
    def __init__(self):
        self.by_status = defaultdict(int)
        self.calls = set()
        self.duplicates = 0
        self.latencies = []
        self.process_seconds = []
        self.audio_seconds = 0.0
        self.first_seen = None
        self.last_seen = None
        self.max_attempt = 0

    def add(self, result, audio_seconds: float) -> None:
        key = result.call_id
        if key in self.calls:
            self.duplicates += 1
        self.calls.add(key)

        self.by_status[result.status.name] += 1
        self.max_attempt = max(self.max_attempt, result.attempt)
        self.audio_seconds += audio_seconds

        if result.ingested_at and result.completed_at:
            self.latencies.append((result.completed_at - result.ingested_at).total_seconds())
        if result.started_at and result.completed_at:
            self.process_seconds.append((result.completed_at - result.started_at).total_seconds())

        stamp = result.completed_at or datetime.now(timezone.utc)
        self.first_seen = min(self.first_seen or stamp, stamp)
        self.last_seen = max(self.last_seen or stamp, stamp)

    def summary(self) -> dict:
        wall = 0.0
        if self.first_seen and self.last_seen:
            wall = (self.last_seen - self.first_seen).total_seconds()

        return {
            "results": sum(self.by_status.values()),
            "unique_calls": len(self.calls),
            "duplicates": self.duplicates,
            "by_status": dict(self.by_status),
            "max_attempt": self.max_attempt,
            "p50_latency_seconds": _percentile(self.latencies, 50),
            "p95_latency_seconds": _percentile(self.latencies, 95),
            "p50_process_seconds": _percentile(self.process_seconds, 50),
            "results_per_second": (len(self.calls) / wall) if wall > 0 else None,
            # Two numbers that are easy to confuse, so both are reported.
            #
            # Pipeline throughput: audio seconds cleared per wall second over
            # the whole run. It includes queueing, backoff and any idle tail, so
            # it describes how fast the deployment drains a backlog -- not how
            # fast the function is.
            "pipeline_audio_seconds_per_second": (
                (self.audio_seconds / wall) if wall > 0 else None
            ),
            # §8's floor is the per-file number: audio duration over the time
            # the function actually spent on it, which is what the SDK's own
            # REALTIME_MULTIPLE metric records. Checking the floor against
            # pipeline throughput would fail a function for being queued behind
            # other work, which is a deployment property, not a function one.
            "realtime_multiple": self._realtime_multiple(),
            "meets_realtime_floor": (
                self._realtime_multiple() >= REALTIME_FLOOR
                if self._realtime_multiple() is not None
                else None
            ),
        }

    def _realtime_multiple(self):
        processing = sum(self.process_seconds)
        if processing <= 0 or self.audio_seconds <= 0:
            return None
        return self.audio_seconds / processing


def _percentile(values, pct: float):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 4)
    index = min(len(ordered) - 1, int(round((pct / 100) * (len(ordered) - 1))))
    return round(ordered[index], 4)


def _consumer(bootstrap: str, group: str, topics: list[str]):
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest",
            # The monitor observes; it must never move a real consumer's
            # offsets, and it should be able to rerun over the same data.
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(topics)
    return consumer


def measure_lag(bootstrap: str, topic: str, groups: list[str]) -> dict:
    """Committed offset vs high water mark, per group.

    Watermarks are fetched once and shared: they are a property of the topic,
    not of the group, and at 200 partitions the round trips are the expensive
    part of this function.
    """
    from confluent_kafka import Consumer, TopicPartition

    probe = Consumer({"bootstrap.servers": bootstrap, "group.id": "faas-monitor-lag-probe"})
    try:
        metadata = probe.list_topics(topic, timeout=30)
        if topic not in metadata.topics or metadata.topics[topic].error is not None:
            return {}
        partitions = sorted(metadata.topics[topic].partitions)
        watermarks = {
            p: probe.get_watermark_offsets(TopicPartition(topic, p), timeout=10, cached=False)
            for p in partitions
        }
    finally:
        probe.close()

    lag_by_group = {}
    for group in groups:
        consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group})
        try:
            committed = consumer.committed(
                [TopicPartition(topic, p) for p in partitions], timeout=30
            )
        finally:
            consumer.close()

        total = 0
        unassigned = 0
        for tp in committed:
            low, high = watermarks.get(tp.partition, (0, 0))
            if tp.offset < 0:
                # No committed offset yet: everything on the partition is lag.
                unassigned += 1
                total += max(0, high - low)
            else:
                total += max(0, high - tp.offset)
        lag_by_group[group] = {"lag": total, "partitions_uncommitted": unassigned}
    return lag_by_group


def watch(
    *,
    bootstrap: str,
    expected: dict,
    idle_seconds: float,
    max_seconds: float,
    report_path: Path,
    lag_interval: float,
) -> dict:
    from faas_sdk.codec import DecodeError
    from faas_sdk.codec_protobuf import ProtobufCodec

    codec = ProtobufCodec()
    seconds_by_call = {r["call_id"]: r["seconds"] for r in expected.get("records", [])}
    expected_calls = set(seconds_by_call)

    tallies = defaultdict(FunctionTally)
    dlq = defaultdict(lambda: defaultdict(int))
    dlq_calls = defaultdict(set)
    undecodable = 0

    group = f"faas-monitor-{int(time.time())}"
    results = _consumer(bootstrap, group + "-results", [RESULTS_TOPIC])
    dead = _consumer(bootstrap, group + "-dlq", DLQ_TOPICS)

    started = time.monotonic()
    last_message = time.monotonic()
    last_lag = 0.0
    lag_samples = []

    log.info(
        "watching %s and %d DLQ topics (expecting %d calls x %d functions)",
        RESULTS_TOPIC,
        len(DLQ_TOPICS),
        len(expected_calls),
        len(FUNCTIONS),
    )

    try:
        while True:
            now = time.monotonic()
            if now - started > max_seconds:
                log.warning("hit --max-seconds, stopping")
                break
            if now - last_message > idle_seconds:
                log.info("idle for %.0fs, stopping", idle_seconds)
                break

            for consumer, is_result in ((results, True), (dead, False)):
                message = consumer.poll(0.2)
                if message is None or message.error():
                    continue
                last_message = time.monotonic()

                if is_result:
                    try:
                        result = codec.decode_result(message.value())
                    except DecodeError:
                        # Worth counting rather than crashing: a result the
                        # monitor cannot read is itself a finding.
                        undecodable += 1
                        continue
                    tallies[result.function_id].add(
                        result, seconds_by_call.get(result.call_id, 0.0)
                    )
                else:
                    # confluent-kafka hands back header keys as str and values
                    # as bytes. Matching on bytes keys silently found nothing,
                    # which showed up as every DLQ code being "?" and every
                    # hydration failure being counted as a missing call.
                    headers = {k: v for k, v in (message.headers() or ())}
                    code = headers.get("faas.error.code", b"?").decode()
                    dlq[message.topic()][code] += 1
                    call_id = headers.get("faas.call_id")
                    if call_id:
                        dlq_calls[message.topic()].add(call_id.decode())

            if lag_interval and time.monotonic() - last_lag > lag_interval:
                last_lag = time.monotonic()
                groups = [f"{f}:1.0.0" for f in FUNCTIONS]
                lag = measure_lag(bootstrap, INTERNAL_TOPIC, groups)
                if lag:
                    lag_samples.append({"at": time.monotonic() - started, "by_group": lag})
                    worst = max(lag.items(), key=lambda kv: kv[1]["lag"])
                    seen = sum(len(t.calls) for t in tallies.values())
                    log.info(
                        "%d results | worst lag %s=%d",
                        seen,
                        worst[0],
                        worst[1]["lag"],
                    )

            if expected_calls and _complete(tallies, expected_calls, dlq_calls):
                log.info("every expected call is accounted for")
                break
    finally:
        results.close()
        dead.close()

    return _report(
        expected=expected,
        expected_calls=expected_calls,
        tallies=tallies,
        dlq=dlq,
        dlq_calls=dlq_calls,
        undecodable=undecodable,
        lag_samples=lag_samples,
        elapsed=time.monotonic() - started,
        report_path=report_path,
        bootstrap=bootstrap,
    )


def _complete(tallies, expected_calls, dlq_calls) -> bool:
    """Every expected call has either a result or a DLQ entry, per function.

    A call that failed hydration produces neither -- no reference is ever
    published, so no function sees it. Those are counted through the hydrator's
    DLQ instead, which is why it is subtracted here.
    """
    hydrator_failures = dlq_calls.get("faas.dlq.hydrator", set())
    reachable = expected_calls - hydrator_failures
    if not reachable:
        return False
    for function in FUNCTIONS:
        seen = tallies[function].calls | dlq_calls.get(f"faas.dlq.{function}", set())
        if not reachable <= seen:
            return False
    return True


def _report(
    *,
    expected,
    expected_calls,
    tallies,
    dlq,
    dlq_calls,
    undecodable,
    lag_samples,
    elapsed,
    report_path,
    bootstrap,
) -> dict:
    hydrator_failures = dlq_calls.get("faas.dlq.hydrator", set())
    reachable = expected_calls - hydrator_failures

    per_function = {}
    for function in FUNCTIONS:
        tally = tallies[function]
        summary = tally.summary()
        dead_lettered = dlq_calls.get(f"faas.dlq.{function}", set())
        summary["dlq_calls"] = len(dead_lettered)
        missing = reachable - tally.calls - dead_lettered
        summary["missing_calls"] = len(missing)
        summary["missing_examples"] = sorted(missing)[:5]
        per_function[function] = summary

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bootstrap": bootstrap,
        "elapsed_seconds": round(elapsed, 1),
        "seed_run": expected.get("run_id"),
        "expected_calls": len(expected_calls),
        "hydration_failures": len(hydrator_failures),
        "reachable_calls": len(reachable),
        "undecodable_results": undecodable,
        "functions": per_function,
        "dlq": {topic: dict(codes) for topic, codes in dlq.items()},
        "lag_samples": lag_samples,
    }

    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2))
        log.info("report -> %s", report_path)

    _print_summary(report)
    return report


def _print_summary(report: dict) -> None:
    print()
    print(f"run {report['seed_run']} -- {report['elapsed_seconds']}s")
    print(
        f"{report['expected_calls']} calls seeded, "
        f"{report['hydration_failures']} failed hydration, "
        f"{report['reachable_calls']} reachable by functions"
    )
    print()
    header = (
        f"{'function':<20} {'results':>8} {'uniq':>6} {'dup':>5} "
        f"{'dlq':>5} {'miss':>5} {'p95 s':>7} {'xRT':>7}"
    )
    print(header)
    print("-" * len(header))
    for name, summary in report["functions"].items():
        speedup = summary["realtime_multiple"]
        print(
            f"{name:<20} {summary['results']:>8} {summary['unique_calls']:>6} "
            f"{summary['duplicates']:>5} {summary['dlq_calls']:>5} "
            f"{summary['missing_calls']:>5} "
            f"{(summary['p95_latency_seconds'] or 0):>7.1f} "
            f"{(speedup or 0):>7.1f}"
        )
    print()
    if report["dlq"]:
        print("dead letters:")
        for topic, codes in sorted(report["dlq"].items()):
            detail = ", ".join(f"{code}={count}" for code, count in sorted(codes.items()))
            print(f"  {topic:<32} {detail}")
    else:
        print("dead letters: none -- the DLQ path was not exercised")
    print()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--expect-from",
        type=Path,
        default=Path("/runs/latest-seed.json"),
        help="seed run file, so missing calls can be told from unsent ones",
    )
    parser.add_argument("--report", type=Path, default=None)
    parser.add_argument(
        "--idle-seconds",
        type=float,
        default=60.0,
        help="stop after this long with no new message",
    )
    parser.add_argument("--max-seconds", type=float, default=1800.0)
    parser.add_argument("--lag-interval", type=float, default=15.0)
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=os.environ.get("FAAS_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    expected = {}
    if args.expect_from and args.expect_from.exists():
        expected = json.loads(args.expect_from.read_text())
    else:
        log.warning("no seed file at %s -- accounting will be counts only", args.expect_from)

    report_path = args.report
    if report_path is None and expected.get("run_id"):
        report_path = Path("/runs") / f"report-{expected['run_id']}.json"

    report = watch(
        bootstrap=args.bootstrap,
        expected=expected,
        idle_seconds=args.idle_seconds,
        max_seconds=args.max_seconds,
        report_path=report_path,
        lag_interval=args.lag_interval,
    )

    # Non-zero on a real accounting failure, so this can gate a CI job later.
    lost = sum(f["missing_calls"] for f in report["functions"].values())
    duplicated = sum(f["duplicates"] for f in report["functions"].values())
    if lost or duplicated or report["undecodable_results"]:
        log.error(
            "accounting failed: %d missing, %d duplicated, %d undecodable",
            lost,
            duplicated,
            report["undecodable_results"],
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
