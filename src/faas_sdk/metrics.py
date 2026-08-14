"""Observability (spec §5.5) -- emitted by the SDK, never by function authors.

If authors instrument, metrics end up inconsistent and there is no
cross-function view. The runner emits the §5.5 set for free:

  faas.lag                  consumer lag (also via kafka-exporter, which feeds
                            the autoscaler -- this one is a cross-check)
  faas.file.latency         per-file latency histogram
  faas.realtime_multiple    throughput vs realtime (the §8 >=25x floor)
  faas.dlq                  DLQ rate
  faas.in_flight            in-flight depth
  faas.retries              retry count

OTelMetrics is the production binding; InMemoryMetrics backs the tests.

`from_env` is how a process chooses between them. It defaults to `NullMetrics`
-- the §5.5 set costs nothing to compute and everything to export, and an image
that has not been given somewhere to send it should not pay for a meter
provider it has no reader for.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from typing import Protocol

log = logging.getLogger(__name__)

LAG = "faas.lag"
FILE_LATENCY = "faas.file.latency"
REALTIME_MULTIPLE = "faas.realtime_multiple"
DLQ = "faas.dlq"
IN_FLIGHT = "faas.in_flight"
RETRIES = "faas.retries"
PROCESSED = "faas.processed"

# Evictions caused by a blocked poll loop -- the §5.2 failure, caught in the
# act. `ConfluentConsumer` has always counted them; this is what carries the
# count out of the process and onto a dashboard next to consumer lag, where a
# non-zero value means in-flight work is being reprocessed.
MAX_POLL_EXCEEDED = "faas.max_poll_exceeded"


class Metrics(Protocol):
    def counter(self, name: str, value: int = 1, **labels) -> None: ...

    def gauge(self, name: str, value: float, **labels) -> None: ...

    def histogram(self, name: str, value: float, **labels) -> None: ...


class NullMetrics:
    def counter(self, name: str, value: int = 1, **labels) -> None:
        pass

    def gauge(self, name: str, value: float, **labels) -> None:
        pass

    def histogram(self, name: str, value: float, **labels) -> None:
        pass


def _key(name: str, labels: dict) -> tuple:
    return (name, tuple(sorted(labels.items())))


class InMemoryMetrics:
    """Test double, and a usable local-dev sink."""

    def __init__(self):
        self.counters: dict[tuple, float] = defaultdict(float)
        self.gauges: dict[tuple, float] = {}
        self.histograms: dict[tuple, list] = defaultdict(list)

    def counter(self, name: str, value: int = 1, **labels) -> None:
        self.counters[_key(name, labels)] += value

    def gauge(self, name: str, value: float, **labels) -> None:
        self.gauges[_key(name, labels)] = value

    def histogram(self, name: str, value: float, **labels) -> None:
        self.histograms[_key(name, labels)].append(value)

    def counter_value(self, name: str, **labels) -> float:
        if labels:
            return self.counters.get(_key(name, labels), 0.0)
        return sum(v for (n, _), v in self.counters.items() if n == name)

    def gauge_value(self, name: str, **labels) -> float | None:
        if labels:
            return self.gauges.get(_key(name, labels))
        matches = [v for (n, _), v in self.gauges.items() if n == name]
        return matches[-1] if matches else None

    def histogram_values(self, name: str, **labels) -> list:
        if labels:
            return self.histograms.get(_key(name, labels), [])
        out = []
        for (n, _), values in self.histograms.items():
            if n == name:
                out.extend(values)
        return out


class OTelMetrics:
    """OpenTelemetry -> Prometheus (spec §11).

    Instruments are created lazily and cached: the OTel API disallows
    re-registering the same instrument name on a meter.
    """

    def __init__(self, meter=None, namespace: str = ""):
        if meter is None:
            from opentelemetry import metrics as otel_metrics

            meter = otel_metrics.get_meter("faas_sdk")
        self._meter = meter
        self._namespace = namespace
        self._counters: dict[str, object] = {}
        self._gauges: dict[str, object] = {}
        self._histograms: dict[str, object] = {}
        self._gauge_values: dict[tuple, float] = {}

    def counter(self, name: str, value: int = 1, **labels) -> None:
        instrument = self._counters.get(name)
        if instrument is None:
            instrument = self._counters[name] = self._meter.create_counter(name)
        instrument.add(value, labels)

    def gauge(self, name: str, value: float, **labels) -> None:
        # Observable gauges need a callback; an up-down counter with a remembered
        # last value gives set-semantics without one.
        instrument = self._gauges.get(name)
        if instrument is None:
            instrument = self._gauges[name] = self._meter.create_up_down_counter(name)
        key = _key(name, labels)
        previous = self._gauge_values.get(key, 0.0)
        instrument.add(value - previous, labels)
        self._gauge_values[key] = value

    def histogram(self, name: str, value: float, **labels) -> None:
        instrument = self._histograms.get(name)
        if instrument is None:
            instrument = self._histograms[name] = self._meter.create_histogram(name)
        instrument.record(value, labels)


def from_env(**labels) -> Metrics:
    """Pick a metrics binding from the environment.

    `FAAS_METRICS=prometheus` starts a scrape endpoint on `FAAS_METRICS_PORT`
    (9108 by default) and returns `OTelMetrics`. Anything else -- including
    unset -- returns `NullMetrics`, so nothing changes for a caller that has not
    asked for metrics.

    Failing to start the exporter must not stop a pod from processing audio:
    a function that runs blind is degraded, one that will not boot is an
    outage. The failure is logged loudly and the process continues on
    `NullMetrics`.
    """
    backend = os.environ.get("FAAS_METRICS", "").strip().lower()
    if backend in ("", "none", "null", "off"):
        return NullMetrics()

    if backend != "prometheus":
        log.warning("unknown FAAS_METRICS=%r; metrics disabled", backend)
        return NullMetrics()

    port = int(os.environ.get("FAAS_METRICS_PORT", "9108"))
    try:
        return _start_prometheus(port, labels)
    except Exception as exc:  # noqa: BLE001 - any import or bind failure
        log.error(
            "could not start the metrics exporter on :%d (%s) -- continuing without "
            "metrics. This pod is invisible to the §5.5 dashboards.",
            port,
            exc,
        )
        return NullMetrics()


def _start_prometheus(port: int, labels: dict) -> Metrics:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.resources import Resource
    from prometheus_client import start_http_server

    # Resource attributes rather than per-metric labels for the identity of the
    # pod: they end up on every series without every call site repeating them,
    # and `function_id` is already a label on the metrics the runner emits.
    provider = MeterProvider(
        metric_readers=[PrometheusMetricReader()],
        resource=Resource.create({k: v for k, v in labels.items() if v}),
    )
    otel_metrics.set_meter_provider(provider)
    start_http_server(port)
    log.info("metrics on :%d/metrics", port)
    return OTelMetrics(meter=provider.get_meter("faas_sdk"))
