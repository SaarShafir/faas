"""Choosing a metrics binding from the environment.

The §5.5 set was implemented and wired to nothing: `OTelMetrics` existed and was
never instantiated, so every runner computed lag, latency, realtime multiple,
DLQ rate, in-flight depth and retry count and threw all of it away. `from_env`
is what gives it somewhere to go.

Two properties matter more than the happy path. The default must stay
`NullMetrics`, because this code ships to every function image and a metrics
backend nobody asked for is a behaviour change nobody reviewed. And a broken
exporter must not stop a pod from processing audio -- running blind is degraded,
refusing to boot is an outage.
"""

from __future__ import annotations

import pytest

from faas_sdk.metrics import NullMetrics, from_env


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("FAAS_METRICS", raising=False)
    monkeypatch.delenv("FAAS_METRICS_PORT", raising=False)


def test_the_default_is_still_null_metrics():
    """Unset means unchanged: this is the behaviour every existing deployment
    has, and turning metrics on by default would change it silently."""
    assert isinstance(from_env(), NullMetrics)


@pytest.mark.parametrize("value", ["", "none", "null", "off", "NONE"])
def test_the_off_switches_are_all_honoured(monkeypatch, value):
    monkeypatch.setenv("FAAS_METRICS", value)
    assert isinstance(from_env(), NullMetrics)


def test_an_unknown_backend_disables_metrics_and_says_so(monkeypatch, caplog):
    """A typo in a deployment manifest should not take a pod down, but it must
    not pass silently either -- that is how a fleet ends up half-instrumented."""
    monkeypatch.setenv("FAAS_METRICS", "prometheus-ish")

    with caplog.at_level("WARNING"):
        metrics = from_env()

    assert isinstance(metrics, NullMetrics)
    assert "prometheus-ish" in caplog.text


def test_a_failing_exporter_does_not_stop_the_pod(monkeypatch, caplog):
    """The one that matters operationally. A port already bound, a missing
    extra, an OTel version mismatch -- none of them are reasons to stop
    processing audio."""
    import faas_sdk.metrics as module

    def explode(port, labels):
        raise OSError("address already in use")

    monkeypatch.setattr(module, "_start_prometheus", explode)
    monkeypatch.setenv("FAAS_METRICS", "prometheus")

    with caplog.at_level("ERROR"):
        metrics = from_env()

    assert isinstance(metrics, NullMetrics)
    assert "invisible" in caplog.text

    # And it is still a usable Metrics: the runner calls these on every file.
    metrics.counter("faas.processed", 1, function_id="x")
    metrics.gauge("faas.in_flight", 2, function_id="x")
    metrics.histogram("faas.file.latency", 0.5, function_id="x")


def test_the_port_is_configurable(monkeypatch):
    """Two containers on one host in the local stack cannot share a port."""
    import faas_sdk.metrics as module

    seen = {}

    def capture(port, labels):
        seen["port"] = port
        seen["labels"] = labels
        return NullMetrics()

    monkeypatch.setattr(module, "_start_prometheus", capture)
    monkeypatch.setenv("FAAS_METRICS", "prometheus")
    monkeypatch.setenv("FAAS_METRICS_PORT", "9999")

    from_env(**{"service.name": "duration_rms"})

    assert seen["port"] == 9999
    assert seen["labels"]["service.name"] == "duration_rms"


def test_prometheus_selection_serves_a_scrape_endpoint(monkeypatch):
    """The end of the chain, against the real exporter: a counter recorded
    through OTelMetrics comes back out of the scrape endpoint."""
    pytest.importorskip("opentelemetry.sdk", reason="metrics extra not installed")
    pytest.importorskip("opentelemetry.exporter.prometheus")
    prometheus_client = pytest.importorskip("prometheus_client")

    import faas_sdk.metrics as module

    # Bind nothing: start_http_server is the only part that needs a socket, and
    # the registry is what the scrape actually reads.
    monkeypatch.setattr(module, "_start_prometheus", module._start_prometheus)
    monkeypatch.setattr("prometheus_client.start_http_server", lambda port: None)
    monkeypatch.setenv("FAAS_METRICS", "prometheus")

    metrics = from_env(**{"service.name": "duration_rms"})
    metrics.counter("faas.processed", 1, function_id="duration_rms", status="SUCCESS")

    exported = prometheus_client.generate_latest(prometheus_client.REGISTRY).decode()
    assert "faas_processed" in exported
    assert "duration_rms" in exported
