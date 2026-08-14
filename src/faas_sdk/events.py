"""Per-call events (spec §5.5, extended).

§5.5 says the SDK emits observability and function authors never do, because
otherwise it is inconsistent and there is no cross-function view. That argument
applies to events exactly as it does to metrics, so this is shaped like
`metrics.py`: a protocol, a null default, an in-memory double, and one real
binding.

**Why events and not just metrics.** They answer different questions and the
difference is cardinality. A metric can tell you a function's p95 latency; it
can never tell you what happened to call `abc-123`, because putting `call_id` on
a metric label would multiply the series count by the number of calls and take
the metric store with it. Events carry exactly that high-cardinality detail --
one record per file per transition -- which is what makes a support question
answerable.

The console reads these. Before them it answered "what happened to this call?"
by scanning Kafka partitions, which works because §6 partitions results on
`call_id`, but only ever as a lookup: "every call for this tenant last week" is
not a partition read. A log query is.

`NullEvents` is the default, so a runner that has not been told where to send
events pays a method call and nothing else.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Protocol

log = logging.getLogger(__name__)

# One name per lifecycle transition. Deliberately few: an event per state
# change is queryable, an event per loop iteration is a bill.
RECEIVED = "call.received"
COMPLETED = "call.completed"
FAILED = "call.failed"
RETRY_SCHEDULED = "call.retry_scheduled"
DEAD_LETTERED = "call.dead_lettered"
HYDRATED = "call.hydrated"


class Events(Protocol):
    def emit(self, name: str, **fields: Any) -> None: ...


class NullEvents:
    def emit(self, name: str, **fields: Any) -> None:
        pass


class InMemoryEvents:
    """Test double, and a usable local sink."""

    def __init__(self):
        self.records: list[tuple[str, dict]] = []

    def emit(self, name: str, **fields: Any) -> None:
        self.records.append((name, fields))

    def named(self, name: str) -> list[dict]:
        return [fields for event, fields in self.records if event == name]

    def for_call(self, call_id: str) -> list[tuple[str, dict]]:
        return [(n, f) for n, f in self.records if f.get("call_id") == call_id]


class OTelEvents:
    """OpenTelemetry logs over OTLP (spec §11).

    Deliberately not aimed at a storage backend. The runner speaks OTLP to a
    collector and never learns whether the other end is OpenSearch, Loki or a
    file -- swapping it is a collector config change, not an image rebuild.

    Every field lands as a log attribute rather than being formatted into the
    message, because a query for "every call this function dead-lettered with
    DECODE_ERROR" should be a field match and not a regex over prose.
    """

    def __init__(self, logger=None, service_name: str = "", service_version: str = ""):
        self._logger = logger
        self.service_name = service_name
        self.service_version = service_version

    def emit(self, name: str, **fields: Any) -> None:
        if self._logger is None:
            return
        try:
            self._logger.emit(self._record(name, fields))
        except Exception as exc:  # noqa: BLE001 - never let telemetry break a file
            # Losing an event is a gap in a dashboard; raising here would fail
            # a file that has already been processed successfully.
            log.debug("event %s dropped: %s", name, exc)

    def _record(self, name: str, fields: dict):
        import time

        from opentelemetry._logs import LogRecord, SeverityNumber

        severity = SeverityNumber.ERROR if name in (FAILED, DEAD_LETTERED) else SeverityNumber.INFO
        # Stamped here, explicitly. Leaving these None sends no timestamp at
        # all, and the backend then indexes the record at the epoch -- so
        # `@timestamp` reads 1970-01-01, every time range excludes everything,
        # and "most recent first" returns an arbitrary order. Found the first
        # time these were queried rather than counted.
        now = time.time_ns()
        return LogRecord(
            timestamp=now,
            observed_timestamp=now,
            severity_text=severity.name,
            severity_number=severity,
            body=name,
            attributes={
                "event.name": name,
                "service.name": self.service_name,
                "service.version": self.service_version,
                **{k: _attribute(v) for k, v in fields.items() if v is not None},
            },
        )


def _attribute(value):
    """OTLP attributes are scalars, bytes are not one, and datetimes are not either.

    Payload bytes are decoded rather than dropped: the whole point of putting
    them in the log is that a trace is readable without a second lookup. §6's
    claim check bounds the size for us -- anything over 256 KB is a reference
    rather than inline bytes, so there is no unbounded field here.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, (str, bool, int, float)):
        return value
    return str(value)


def from_env(service_name: str = "", service_version: str = "") -> Events:
    """Pick an event binding from the environment.

    `FAAS_EVENTS=otlp` sends to `OTEL_EXPORTER_OTLP_ENDPOINT`. Anything else,
    including unset, is `NullEvents` -- the same default-off rule metrics
    follows, for the same reason: this ships in every function image and
    turning telemetry on by default is a change nobody reviewed.
    """
    backend = os.environ.get("FAAS_EVENTS", "").strip().lower()
    if backend in ("", "none", "null", "off"):
        return NullEvents()

    if backend != "otlp":
        log.warning("unknown FAAS_EVENTS=%r; events disabled", backend)
        return NullEvents()

    try:
        return _start_otlp(service_name, service_version)
    except Exception as exc:  # noqa: BLE001 - any import, config or connect failure
        log.error(
            "could not start the OTLP log exporter (%s) -- continuing without events. "
            "This pod's calls will not appear in the console.",
            exc,
        )
        return NullEvents()


def _start_otlp(service_name: str, service_version: str) -> Events:
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.resources import Resource

    provider = LoggerProvider(
        resource=Resource.create({"service.name": service_name, "service.version": service_version})
    )
    # Batched, because one HTTP round trip per file at 17 files/sec across ten
    # functions would make the exporter the slowest thing in the pipeline.
    provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
    set_logger_provider(provider)

    log.info("emitting events over OTLP as %s", service_name)
    return OTelEvents(
        logger=provider.get_logger("faas_sdk"),
        service_name=service_name,
        service_version=service_version,
    )
