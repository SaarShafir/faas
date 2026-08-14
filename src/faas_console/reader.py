"""Where the console gets its data.

One interface, `ConsoleReader`, and one implementation that reads Kafka
directly. The interface is the point: the results sink is build-order step 5,
and when it lands, call tracing and result search should move to indexed
queries without a single view changing. The SDK already does exactly this with
`Codec`/`ProtobufCodec`, and this copies the shape deliberately.

**Why reading Kafka directly is affordable at all.** Results are partitioned on
`call_id` alone, so every result for a call is colocated and the aggregator
needs no shuffle (§6). References are partitioned the same way (§4.2). That
makes a call lookup two targeted partition reads -- one of 200 on the results
topic, one of 200 on the internal topic -- rather than a scan of either. The
DLQ topics are small and scanned whole.

**What it will not survive.** "Every call for this tenant last month" is not a
partition read, and no amount of care here makes it one. That query needs the
sink. This is a lookup tool, not a search engine, and the seam is what keeps
that from becoming a rewrite.
"""

from __future__ import annotations

import logging
import time
from typing import Protocol

from faas_sdk.codec import DecodeError
from faas_sdk.partitioner import partition_for

from .models import (
    CallTrace,
    DeadLetter,
    Finding,
    GroupStatus,
    Reference,
    ResultView,
    TopicInfo,
)

log = logging.getLogger(__name__)

RESULTS_TOPIC = "faas.results"
INTERNAL_TOPIC = "faas.audio.internal"
INPUT_TOPIC = "faas.calls.raw"


class ConsoleReader(Protocol):
    def fleet(self) -> list[GroupStatus]: ...

    def find_call(self, call_id: str) -> CallTrace: ...

    def dead_letters(self, topic: str | None = None, limit: int = 50) -> list[DeadLetter]: ...

    def topics(self) -> list[TopicInfo]: ...

    def lint(self) -> list[Finding]: ...


class KafkaConsoleReader:
    def __init__(
        self,
        bootstrap: str,
        *,
        declarations=None,
        codec=None,
        consumer_factory=None,
        admin_factory=None,
        scan_timeout: float = 20.0,
    ):
        self.bootstrap = bootstrap
        # Declarations come from git, never from the console (§8). They are
        # what "expected set" and the partition-count lint are checked against.
        self.declarations = declarations or {}
        self._codec = codec
        self._consumer_factory = consumer_factory or self._default_consumer
        self._admin_factory = admin_factory or self._default_admin
        self.scan_timeout = scan_timeout

    # -- wiring ------------------------------------------------------------

    @property
    def codec(self):
        if self._codec is None:
            from faas_sdk.codec_protobuf import ProtobufCodec

            self._codec = ProtobufCodec()
        return self._codec

    def _default_consumer(self, group_suffix: str = "console"):
        from confluent_kafka import Consumer

        return Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                # A unique group per read, never committing: the console must
                # never move a real consumer's offsets, and two operators
                # looking at the same call must not steal partitions from each
                # other.
                "group.id": f"faas-console-{group_suffix}-{time.time_ns()}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                # Turns "I have reached the end" into an event instead of a
                # timeout, which is what makes a bounded scan possible.
                "enable.partition.eof": True,
            }
        )

    def _default_admin(self):
        from confluent_kafka.admin import AdminClient

        return AdminClient({"bootstrap.servers": self.bootstrap})

    # -- topology ----------------------------------------------------------

    def topics(self) -> list[TopicInfo]:
        consumer = self._consumer_factory("topics")
        try:
            metadata = consumer.list_topics(timeout=self.scan_timeout)
            return sorted(
                (
                    TopicInfo(name=name, partitions=len(topic.partitions))
                    for name, topic in metadata.topics.items()
                    if name.startswith("faas.")
                ),
                key=lambda t: t.name,
            )
        finally:
            consumer.close()

    def _partition_count(self, topic: str) -> int:
        for info in self.topics():
            if info.name == topic:
                return info.partitions
        return 0

    # -- fleet -------------------------------------------------------------

    def fleet(self) -> list[GroupStatus]:
        """Consumer groups and their backlog.

        Lag is clamped at zero per partition. librdkafka and kafka-exporter
        both use a negative offset to mean "this group has never committed
        here", which is not a measurement -- summed unclamped across a
        200-partition topic it goes negative and makes a busy function look
        idle.
        """
        from confluent_kafka import Consumer, TopicPartition

        admin = self._admin_factory()
        groups = self._list_groups(admin)

        partitions = list(range(self._partition_count(INTERNAL_TOPIC)))
        if not partitions:
            return []

        probe = self._consumer_factory("watermarks")
        try:
            watermarks = {
                p: probe.get_watermark_offsets(
                    TopicPartition(INTERNAL_TOPIC, p), timeout=10, cached=False
                )
                for p in partitions
            }
        finally:
            probe.close()

        statuses = []
        for group_id, state, members in groups:
            consumer = Consumer({"bootstrap.servers": self.bootstrap, "group.id": group_id})
            try:
                committed = consumer.committed(
                    [TopicPartition(INTERNAL_TOPIC, p) for p in partitions],
                    timeout=self.scan_timeout,
                )
            finally:
                consumer.close()

            lag = 0
            uncommitted = 0
            for tp in committed:
                low, high = watermarks.get(tp.partition, (0, 0))
                if tp.offset < 0:
                    uncommitted += 1
                    lag += max(0, high - low)
                else:
                    lag += max(0, high - tp.offset)

            function_id, _, version = group_id.partition(":")
            statuses.append(
                GroupStatus(
                    group_id=group_id,
                    function_id=function_id,
                    function_version=version,
                    state=state,
                    members=members,
                    lag=lag,
                    partitions_uncommitted=uncommitted,
                )
            )
        return sorted(statuses, key=lambda s: (-s.lag, s.group_id))

    def _list_groups(self, admin) -> list[tuple]:
        try:
            future = admin.list_consumer_groups(request_timeout=self.scan_timeout)
            listing = future.result()
        except Exception as exc:  # noqa: BLE001 - admin API shape varies by version
            log.warning("could not list consumer groups: %s", exc)
            return []

        out = []
        for group in getattr(listing, "valid", []):
            group_id = getattr(group, "group_id", "")
            # The hydrator and the functions are the interesting groups; the
            # console's own throwaway groups are noise.
            if group_id.startswith("faas-console") or group_id.startswith("faas-monitor"):
                continue
            state = str(getattr(group, "state", "")).split(".")[-1]
            out.append((group_id, state, 0))
        return out

    # -- call tracing ------------------------------------------------------

    def find_call(self, call_id: str) -> CallTrace:
        started = time.monotonic()
        scanned = []

        reference = self._find_reference(call_id, scanned)
        results = self._find_results(call_id, scanned)
        dead = [d for d in self.dead_letters(limit=2000) if d.call_id == call_id]

        answered = {r.function_id for r in results} | {d.function_id for d in dead if d.function_id}
        expected = {fid for fid in self.declarations if fid != "hydrator"}

        return CallTrace(
            call_id=call_id,
            reference=reference,
            results=sorted(results, key=lambda r: r.function_id),
            dead_letters=dead,
            missing=sorted(expected - answered) if reference else [],
            partitions_scanned=scanned,
            scan_seconds=round(time.monotonic() - started, 3),
        )

    def _find_reference(self, call_id: str, scanned: list) -> Reference | None:
        count = self._partition_count(INTERNAL_TOPIC)
        if not count:
            return None
        partition = partition_for(call_id.encode(), count)
        scanned.append(f"{INTERNAL_TOPIC}[{partition}] of {count}")

        found = None
        for raw, _, offset in self._scan(INTERNAL_TOPIC, partition):
            try:
                ref = self.codec.decode_reference(raw)
            except DecodeError:
                continue
            if ref.call_id != call_id:
                continue
            # Keep the latest: a reprocessed call replaces itself on the same
            # partition rather than landing twice elsewhere (§4.2).
            found = Reference(
                call_id=ref.call_id,
                object_key=ref.object_key,
                sample_rate=ref.sample_rate,
                channels=ref.channels,
                duration_seconds=ref.duration_seconds,
                ingested_at=ref.ingested_at,
                hydrated_at=ref.hydrated_at,
                partition=partition,
                offset=offset,
            )
        return found

    def _find_results(self, call_id: str, scanned: list) -> list[ResultView]:
        count = self._partition_count(RESULTS_TOPIC)
        if not count:
            return []
        # The §6 partitioning decision paying off: one partition, not 200.
        partition = partition_for(call_id.encode(), count)
        scanned.append(f"{RESULTS_TOPIC}[{partition}] of {count}")

        latest: dict[tuple, ResultView] = {}
        for raw, _, offset in self._scan(RESULTS_TOPIC, partition):
            try:
                result = self.codec.decode_result(raw)
            except DecodeError:
                continue
            if result.call_id != call_id:
                continue
            view = ResultView(
                function_id=result.function_id,
                function_version=result.function_version,
                status=result.status.name,
                attempt=result.attempt,
                payload=result.payload,
                payload_ref=result.payload_ref,
                payload_content_type=result.payload_content_type,
                error_code=result.error.code if result.error else "",
                error_message=result.error.message if result.error else "",
                error_retryable=result.error.retryable if result.error else False,
                ingested_at=result.ingested_at,
                started_at=result.started_at,
                completed_at=result.completed_at,
                partition=partition,
                offset=offset,
            )
            # Keyed by (function, version) exactly like the record key, so a
            # redelivered call shows once per function rather than twice.
            latest[(view.function_id, view.function_version)] = view
        return list(latest.values())

    # -- dead letters ------------------------------------------------------

    def dead_letters(self, topic: str | None = None, limit: int = 50) -> list[DeadLetter]:
        topics = [topic] if topic else [t.name for t in self.topics() if ".dlq." in t.name]

        out = []
        for name in topics:
            partitions = self._partition_count(name)
            for partition in range(partitions):
                for raw, headers, _ in self._scan(name, partition):
                    out.append(_dead_letter(name, headers, raw))
        out.sort(key=lambda d: d.failed_at, reverse=True)
        return out[:limit]

    # -- scanning ----------------------------------------------------------

    def _scan(self, topic: str, partition: int, max_records: int = 20000):
        """Read one partition from its low water mark to its high, then stop.

        The watermarks are fetched first and drive everything, which fixes two
        problems the obvious implementation has.

        An empty partition costs one metadata call instead of a poll timeout.
        That matters more than it sounds: a call trace checks eleven DLQ topics,
        nearly all of them empty, and at a second each that was twenty seconds
        of a twenty-second budget.

        More importantly, it is correct. Treating a `None` poll as "end of
        partition" conflates "no more records" with "the assignment is not
        ready yet", and the second is normal for the first few hundred
        milliseconds after `assign`. A trace that quietly returned six of ten
        results because the broker was briefly slow would be worse than one
        that failed outright.
        """
        from confluent_kafka import TopicPartition

        consumer = self._consumer_factory(f"scan-{topic}-{partition}")
        deadline = time.monotonic() + self.scan_timeout
        try:
            tp = TopicPartition(topic, partition)
            try:
                low, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
            except Exception as exc:  # noqa: BLE001 - unknown topic, broker down
                log.warning("no watermarks for %s[%d]: %s", topic, partition, exc)
                return

            if high <= low:
                return

            consumer.assign([TopicPartition(topic, partition, low)])
            count = 0
            while count < max_records and time.monotonic() < deadline:
                message = consumer.poll(0.5)
                if message is None:
                    # Not the end -- just nothing yet. The high water mark is
                    # what says when to stop.
                    continue
                if message.error():
                    from confluent_kafka import KafkaError

                    if message.error().code() == KafkaError._PARTITION_EOF:
                        break
                    log.warning("scan error on %s[%d]: %s", topic, partition, message.error())
                    break

                count += 1
                offset = message.offset()
                yield (
                    message.value(),
                    {k: v for k, v in (message.headers() or ())},
                    offset,
                )
                if offset >= high - 1:
                    break
        finally:
            consumer.close()

    # -- config lint -------------------------------------------------------

    def lint(self) -> list[Finding]:
        """Checks that catch a misconfiguration before it fails at run time."""
        findings = []
        topics = {t.name: t.partitions for t in self.topics()}

        for name in (RESULTS_TOPIC, INTERNAL_TOPIC, INPUT_TOPIC):
            if name not in topics:
                findings.append(
                    Finding("error", name, "topic does not exist; nothing can flow through it")
                )

        for function_id, config in sorted(self.declarations.items()):
            declared = config.results_topic_partitions
            actual = topics.get(config.results_topic)

            if actual is None:
                findings.append(
                    Finding(
                        "error",
                        function_id,
                        f"declares results_topic {config.results_topic}, which does not exist",
                    )
                )
            elif declared > actual:
                # The one that fails at run time rather than at startup: the SDK
                # computes the partition itself with murmur2 and passes it
                # explicitly, so producing to partition 150 of a 12-partition
                # topic is an error on the first result, not at boot.
                findings.append(
                    Finding(
                        "error",
                        function_id,
                        f"declares results_topic_partitions={declared} but "
                        f"{config.results_topic} has {actual}. The SDK passes the "
                        f"partition explicitly, so this fails when the first result "
                        f"is produced, not at startup.",
                    )
                )
            elif declared < actual:
                findings.append(
                    Finding(
                        "warning",
                        function_id,
                        f"declares results_topic_partitions={declared} but "
                        f"{config.results_topic} has {actual}: {actual - declared} "
                        f"partitions will never receive a result from this function.",
                    )
                )

            if config.dlq_topic not in topics:
                findings.append(
                    Finding(
                        "warning",
                        function_id,
                        f"dlq_topic {config.dlq_topic} does not exist; a failure here "
                        f"has nowhere to go",
                    )
                )

        return findings


def _dead_letter(topic: str, headers: dict, body: bytes) -> DeadLetter:
    """Build a view from the DLQ's headers.

    The header names are `faas_sdk.dlq`'s contract, and confluent-kafka hands
    keys back as str with bytes values.
    """

    def text(name: str, default: str = "") -> str:
        value = headers.get(name)
        return value.decode(errors="replace") if isinstance(value, bytes) else default

    def number(name: str) -> int:
        try:
            return int(text(name, "-1"))
        except ValueError:
            return -1

    return DeadLetter(
        topic=topic,
        function_id=text("faas.function_id"),
        function_version=text("faas.function_version"),
        group_id=text("faas.group_id"),
        error_code=text("faas.error.code", "?"),
        error_message=text("faas.error.message"),
        retryable=text("faas.error.retryable") == "true",
        attempt=number("faas.attempt"),
        call_id=text("faas.call_id"),
        source_topic=text("faas.source.topic"),
        source_partition=number("faas.source.partition"),
        source_offset=number("faas.source.offset"),
        failed_at=text("faas.failed_at"),
        body_bytes=len(body or b""),
    )
