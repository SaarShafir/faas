"""The console's pages, rendered.

These exist because a template error is invisible to unit tests of the reader
and perfectly visible to whoever opens the page during an incident. Every route
is rendered against a stub reader, so a typo'd attribute or a renamed field
fails here.

The stub is deliberately not a Kafka fake: what is under test is the views, and
the reader has its own tests.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi", reason="console is an optional extra")

from fastapi.testclient import TestClient  # noqa: E402

from faas_console import app as app_module  # noqa: E402
from faas_console.models import (  # noqa: E402
    CallTrace,
    DeadLetter,
    Finding,
    GroupStatus,
    Reference,
    ResultView,
    TopicInfo,
)
from faas_sdk.config import FunctionConfig  # noqa: E402


def _declaration(function_id="duration_rms", **overrides):
    fields = dict(
        function_id=function_id,
        function_version="1.0.0",
        image="registry/x:1.0.0",
        dlq_topic=f"faas.dlq.{function_id}",
    )
    fields.update(overrides)
    return FunctionConfig(**fields)


class StubReader:
    def __init__(self):
        self.declarations = {"duration_rms": _declaration()}
        self._trace = CallTrace(call_id="call-1", reference=None)
        self._dead: list[DeadLetter] = []
        self._findings: list[Finding] = []
        self._groups = [
            GroupStatus(
                group_id="duration_rms:1.0.0",
                function_id="duration_rms",
                function_version="1.0.0",
                state="STABLE",
                members=1,
                lag=0,
                partitions_uncommitted=140,
            )
        ]

    def fleet(self):
        return self._groups

    def find_call(self, call_id):
        return self._trace

    def dead_letters(self, topic=None, limit=50):
        return self._dead

    def topics(self):
        return [TopicInfo("faas.results", 200), TopicInfo("faas.dlq.duration_rms", 3)]

    def lint(self):
        return self._findings


@pytest.fixture
def reader(monkeypatch):
    stub = StubReader()
    monkeypatch.setattr(app_module, "get_reader", lambda: stub)
    return stub


@pytest.fixture
def client(reader):
    return TestClient(app_module.app)


def test_every_page_renders(client):
    for path in ("/", "/call", "/dlq", "/registry", "/healthz"):
        response = client.get(path)
        assert response.status_code == 200, f"{path} returned {response.status_code}"


def test_the_overview_shows_lint_findings(client, reader):
    reader._findings = [
        Finding("error", "duration_rms", "declares results_topic_partitions=500 but it has 200")
    ]
    body = client.get("/").text
    assert "declares results_topic_partitions=500" in body
    assert "error" in body


def test_the_overview_says_so_when_lint_is_clean(client):
    assert "lint is clean" in client.get("/").text.lower()


def test_a_group_with_no_declaration_is_flagged(client, reader):
    """Drift in the direction §10 cares about: something ran that nobody can
    point at a declaration for."""
    reader._groups = reader._groups + [
        GroupStatus(
            group_id="ghost:9.9.9",
            function_id="ghost",
            function_version="9.9.9",
            state="STABLE",
            members=1,
            lag=3,
            partitions_uncommitted=0,
        )
    ]
    body = client.get("/").text
    assert "ghost" in body
    assert "no declaration" in body


def test_a_trace_renders_results_and_the_partitions_it_read(client, reader):
    reader._trace = CallTrace(
        call_id="call-1",
        reference=Reference(
            call_id="call-1",
            object_key="call-1.flac",
            sample_rate=16000,
            channels=1,
            duration_seconds=42.0,
            ingested_at=None,
            hydrated_at=None,
            partition=7,
            offset=12,
        ),
        results=[
            ResultView(
                function_id="duration_rms",
                function_version="1.0.0",
                status="SUCCESS",
                attempt=1,
                payload=b'{"rms": 0.1}',
                payload_ref=None,
                payload_content_type="application/json",
            )
        ],
        partitions_scanned=["faas.results[7] of 200"],
        scan_seconds=0.2,
    )

    body = client.get("/call?call_id=call-1").text

    assert "duration_rms" in body
    assert "SUCCESS" in body
    assert "faas.results[7] of 200" in body
    assert "call-1.flac" in body


def test_an_unhydrated_call_points_at_the_hydrator_dlq(client, reader):
    """The single most common support question, and the answer is never in the
    function's DLQ -- it is upstream."""
    reader._trace = CallTrace(call_id="call-x", reference=None)
    body = client.get("/call?call_id=call-x").text
    assert "faas.dlq.hydrator" in body


def test_a_duration_disagreement_is_called_out(client, reader):
    reader._trace = CallTrace(
        call_id="call-t",
        reference=Reference(
            call_id="call-t",
            object_key="t.flac",
            sample_rate=16000,
            channels=1,
            duration_seconds=300.0,
            ingested_at=None,
            hydrated_at=None,
        ),
        results=[
            ResultView(
                function_id="duration_rms",
                function_version="1.0.0",
                status="SUCCESS",
                attempt=1,
                payload=b'{"duration_seconds": 150.0, "reference_duration_seconds": 300.0}',
                payload_ref=None,
                payload_content_type="application/json",
            )
        ],
    )
    body = client.get("/call?call_id=call-t").text
    assert "disagrees" in body


def test_missing_functions_are_listed(client, reader):
    reader._trace = CallTrace(
        call_id="call-1",
        reference=Reference(
            call_id="call-1",
            object_key="x.flac",
            sample_rate=16000,
            channels=1,
            duration_seconds=1.0,
            ingested_at=None,
            hydrated_at=None,
        ),
        missing=["energy_vad", "snr_estimate"],
    )
    body = client.get("/call?call_id=call-1").text
    assert "energy_vad" in body and "snr_estimate" in body


def test_the_dlq_page_shows_what_a_replay_would_need(client, reader):
    reader._dead = [
        DeadLetter(
            topic="faas.dlq.duration_rms",
            function_id="duration_rms",
            function_version="1.0.0",
            group_id="duration_rms:1.0.0",
            error_code="DECODE_ERROR",
            error_message="not FLAC",
            retryable=False,
            attempt=1,
            call_id="call-9",
            source_topic="faas.audio.internal",
            source_partition=7,
            source_offset=42,
            failed_at="2026-08-14T00:00:00+00:00",
            body_bytes=128,
        )
    ]
    body = client.get("/dlq").text
    assert "DECODE_ERROR" in body
    assert "faas.audio.internal[7]@42" in body
    assert "/call?call_id=call-9" in body


def test_an_empty_dlq_is_not_reported_as_good_news(client):
    """A DLQ nobody has written to is a DLQ nobody has tested."""
    assert "never been tested" in client.get("/dlq").text


def test_the_registry_says_it_cannot_satisfy_the_deletion_requirement(client):
    """§10 needs every version that ever ran; consumer groups expire. The page
    must not imply otherwise."""
    body = client.get("/registry").text
    assert "stopgap" in body.lower()


def test_the_console_never_offers_to_edit_a_declaration(client):
    """§8's config-as-code claim dies the moment there are two sources of
    truth. No page should contain a form that writes one."""
    for path in ("/", "/registry", "/call", "/dlq"):
        body = client.get(path).text
        assert 'method="post"' not in body.lower()
        assert "<textarea" not in body.lower()


def test_the_trace_is_available_as_json(client, reader):
    reader._trace = CallTrace(call_id="call-1", reference=None, missing=["energy_vad"])
    payload = client.get("/api/call/call-1").json()
    assert payload["call_id"] == "call-1"
    assert payload["hydrated"] is False
    assert payload["missing"] == ["energy_vad"]


def test_the_lint_endpoint_is_a_smoke_test(client, reader):
    assert client.get("/api/lint").json()["ok"] is True

    reader._findings = [Finding("error", "duration_rms", "partition count mismatch")]
    assert client.get("/api/lint").json()["ok"] is False


def test_a_broker_that_is_down_still_renders_the_overview(client, reader):
    """The console is most needed when something is broken. A page that 500s
    because the broker is unreachable is a page that is never there when it
    matters."""

    def explode():
        raise RuntimeError("no brokers available")

    reader.fleet = explode

    response = client.get("/")
    assert response.status_code == 200
    assert "broker is unreachable" in response.text
