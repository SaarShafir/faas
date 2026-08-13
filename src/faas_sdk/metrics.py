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
"""

from __future__ import annotations

from collections import defaultdict
from typing import Protocol

LAG = "faas.lag"
FILE_LATENCY = "faas.file.latency"
REALTIME_MULTIPLE = "faas.realtime_multiple"
DLQ = "faas.dlq"
IN_FLIGHT = "faas.in_flight"
RETRIES = "faas.retries"
PROCESSED = "faas.processed"


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
