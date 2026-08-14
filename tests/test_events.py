"""Per-call events, emitted by the runner.

These are what the console reads to answer "what happened to call abc-123",
which metrics structurally cannot: a `call_id` label would multiply the series
count by the number of calls.

The tests worth having are about coverage and about not lying. Every transition
a call can make must produce exactly one event, because a missing event is an
invisible call and a duplicated one is a support engineer chasing a redelivery
that never happened.
"""

from __future__ import annotations

import json

import pytest

from faas_sdk.codec import JsonCodec
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.events import (
    COMPLETED,
    DEAD_LETTERED,
    FAILED,
    RECEIVED,
    RETRY_SCHEDULED,
    InMemoryEvents,
    NullEvents,
    from_env,
)
from faas_sdk.results import ResultEmitter
from faas_sdk.runner import FunctionRunner
from faas_sdk.testing import reference_message


@pytest.fixture
def events():
    return InMemoryEvents()


@pytest.fixture
def watched_runner(config, consumer, producer, object_store, pool, clock, metrics, events):
    runner = FunctionRunner(
        config=config,
        consumer=consumer,
        pool=pool,
        codec=JsonCodec(),
        results=ResultEmitter(
            config=config,
            producer=producer,
            object_store=object_store,
            codec=JsonCodec(),
            clock=clock,
        ),
        dlq=DeadLetterQueue(config=config, producer=producer, clock=clock),
        metrics=metrics,
        events=events,
        clock=clock,
    )
    runner.start()
    return runner


# -- selection -------------------------------------------------------------


def test_events_are_off_unless_asked_for(monkeypatch):
    """Same default-off rule as metrics: this ships in every function image."""
    monkeypatch.delenv("FAAS_EVENTS", raising=False)
    assert isinstance(from_env(), NullEvents)


def test_an_unknown_backend_disables_events(monkeypatch, caplog):
    monkeypatch.setenv("FAAS_EVENTS", "syslog")
    with caplog.at_level("WARNING"):
        assert isinstance(from_env(), NullEvents)
    assert "syslog" in caplog.text


def test_a_broken_exporter_does_not_stop_the_pod(monkeypatch, caplog):
    """Telemetry that will not start is a gap in a dashboard. Refusing to boot
    over it is an outage."""
    import faas_sdk.events as module

    monkeypatch.setattr(module, "_start_otlp", lambda *a: (_ for _ in ()).throw(OSError("no")))
    monkeypatch.setenv("FAAS_EVENTS", "otlp")

    with caplog.at_level("ERROR"):
        events = from_env("duration_rms", "1.0.0")

    assert isinstance(events, NullEvents)
    assert "will not appear in the console" in caplog.text
    events.emit("call.received", call_id="x")  # still usable


# -- lifecycle coverage ----------------------------------------------------


def test_a_successful_call_emits_received_then_completed(watched_runner, consumer, pool, events):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    watched_runner.run_once()

    assert [name for name, _ in events.records] == [RECEIVED]

    pool.succeed("c10", payload=b'{"rms": 0.12}', schema_version="1")
    watched_runner.run_once()

    assert [name for name, _ in events.records] == [RECEIVED, COMPLETED]


def test_the_completed_event_carries_the_payload(watched_runner, consumer, pool, events):
    """The point of putting payloads in the log: a trace is readable without a
    second lookup. §6's claim check bounds the size -- over 256 KB it is a
    reference, not inline bytes."""
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    watched_runner.run_once()
    pool.succeed("c10", payload=b'{"rms": 0.12}', schema_version="1")
    watched_runner.run_once()

    (completed,) = events.named(COMPLETED)

    assert json.loads(completed["payload"]) == {"rms": 0.12}
    assert completed["status"] == "SUCCESS"
    assert completed["call_id"] == "c10"
    assert completed["payload_schema_version"] == "1"


def test_every_event_identifies_the_function_and_version(watched_runner, consumer, pool, events):
    """Without these a query cannot separate two versions of a function during
    a shadow deploy, which is exactly when someone is looking."""
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    watched_runner.run_once()
    pool.succeed("c10", payload=b"{}", schema_version="1")
    watched_runner.run_once()

    for _, fields in events.records:
        assert fields["function_id"] == "duration_rms"
        assert fields["function_version"]


def test_a_retry_emits_failed_then_scheduled_then_completed(
    watched_runner, consumer, pool, events, clock, config
):
    """The whole ladder, in order. A trace that showed only the final success
    would hide that the call took three attempts to get there."""
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    watched_runner.run_once()
    pool.fail("c10", code="S3_TIMEOUT", retryable=True, message="slow")
    watched_runner.run_once()

    assert [n for n, _ in events.records] == [RECEIVED, FAILED, RETRY_SCHEDULED]

    clock.advance(config.retry_backoff_seconds + 1)
    watched_runner.run_once()
    pool.succeed("c10", payload=b"{}", schema_version="1")
    watched_runner.run_once()

    assert [n for n, _ in events.records] == [
        RECEIVED,
        FAILED,
        RETRY_SCHEDULED,
        COMPLETED,
    ]
    assert events.named(RETRY_SCHEDULED)[0]["attempt"] == 2
    assert events.named(COMPLETED)[0]["attempt"] == 2


def test_a_dead_letter_event_says_where_a_replay_would_read_from(
    watched_runner, consumer, pool, events
):
    consumer.feed(reference_message(partition=3, offset=99, call_id="c99"))
    watched_runner.run_once()
    pool.fail("c99", code="DECODE_ERROR", retryable=False, message="not FLAC")
    watched_runner.run_once()

    (dead,) = events.named(DEAD_LETTERED)

    assert dead["error_code"] == "DECODE_ERROR"
    assert dead["retryable"] is False
    assert (dead["source_partition"], dead["source_offset"]) == (3, 99)
    assert dead["dlq_topic"].endswith("duration_rms")


def test_poison_on_arrival_is_still_an_event(watched_runner, consumer, events):
    """A message that never reaches the pool is the easiest kind of call to
    lose track of, and the one most likely to be asked about."""
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10", raw_value=b"\xff!"))
    watched_runner.run_once()

    (dead,) = events.named(DEAD_LETTERED)
    assert dead["error_code"] == "DECODE_ERROR"


def test_a_skipped_call_is_recorded_as_such(watched_runner, consumer, pool, events):
    """SKIPPED is an answer, not an absence. If it produced no event the call
    would look lost."""
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    watched_runner.run_once()
    pool.skip("c10")
    watched_runner.run_once()

    (completed,) = events.named(COMPLETED)
    assert completed["status"] == "SKIPPED"


def test_events_are_off_by_default_on_a_runner(
    config, consumer, producer, object_store, pool, clock
):
    """A runner built without events must not fail when it emits them."""
    runner = FunctionRunner(
        config=config,
        consumer=consumer,
        pool=pool,
        codec=JsonCodec(),
        results=ResultEmitter(
            config=config,
            producer=producer,
            object_store=object_store,
            codec=JsonCodec(),
            clock=clock,
        ),
        dlq=DeadLetterQueue(config=config, producer=producer, clock=clock),
        clock=clock,
    )
    runner.start()
    consumer.feed(reference_message(partition=0, offset=1, call_id="c1"))
    runner.run_once()  # must not raise


def test_attributes_survive_the_otlp_encoding():
    """OTLP attributes are scalars. Bytes are not one, and a payload that was
    silently dropped at this boundary would be a payload nobody could see."""
    from faas_sdk.events import _attribute

    assert _attribute(b'{"rms": 0.1}') == '{"rms": 0.1}'
    assert _attribute(b"\xff\xfe") == "��"
    assert _attribute(True) is True
    assert _attribute(1.5) == 1.5


def test_the_record_carries_a_real_timestamp():
    """Leaving the timestamp unset sends none at all, and the backend then
    indexes the record at the epoch: `@timestamp` reads 1970-01-01, every time
    range excludes everything, and "most recent first" is arbitrary. Caught by
    querying the events rather than counting them."""
    pytest.importorskip("opentelemetry.sdk", reason="events extra not installed")

    import time

    from faas_sdk.events import OTelEvents

    record = OTelEvents(logger=object(), service_name="x", service_version="1")._record(
        COMPLETED, {"call_id": "c1"}
    )

    assert record.timestamp is not None
    assert record.observed_timestamp is not None
    # Nanoseconds since the epoch, within a second of now.
    assert abs(record.timestamp - time.time_ns()) < 1e9
