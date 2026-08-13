"""The SQLite store behind the ResultsStore port."""

from __future__ import annotations

from datetime import datetime, timezone

from faas_sdk.models import ErrorInfo, Result, Status
from faas_sdk.testing import FakeClock
from faas_sink.store import SqliteResultsStore


def _result(call_id="call-1", function_id="duration_rms", version="1.0.0", **overrides):
    fields = dict(
        call_id=call_id,
        function_id=function_id,
        function_version=version,
        status=Status.SUCCESS,
        input_object_key=f"{call_id}.flac",
        input_offset=7,
        attempt=1,
        payload=b'{"duration": 300.0}',
        payload_schema_version="1",
        payload_content_type="application/json",
        ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        started_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        completed_at=datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
    )
    fields.update(overrides)
    return Result(**fields)


def test_success_result_round_trips():
    store = SqliteResultsStore()
    result = _result()
    store.upsert(result)
    assert store.get(result.key.decode()) == result
    assert store.count() == 1


def test_failed_result_with_error_round_trips():
    store = SqliteResultsStore()
    result = _result(
        status=Status.FAILED,
        payload=None,
        error=ErrorInfo(code="LIBRARY_CRASH", message="boom", retryable=True),
    )
    store.upsert(result)
    assert store.get(result.key.decode()) == result


def test_skipped_result_round_trips():
    store = SqliteResultsStore()
    result = _result(status=Status.SKIPPED, payload=None)
    store.upsert(result)
    assert store.get(result.key.decode()) == result


def test_claim_checked_payload_stays_a_ref():
    """The sink stores the ref and never fetches the payload: the object store
    has its own TTL, consumers fetch by ref when they want it."""
    store = SqliteResultsStore()
    result = _result(payload=None, payload_ref="results/duration_rms/1.0.0/call-1")
    store.upsert(result)
    assert store.get(result.key.decode()) == result


def test_missing_key_is_none():
    store = SqliteResultsStore()
    assert store.get("call-9:duration_rms:1.0.0") is None


def test_upsert_is_idempotent_on_the_composite_key():
    """At-least-once means redelivery; the §6 composite key makes it a no-op.
    The latest record wins (a retry carries a higher attempt)."""
    store = SqliteResultsStore()
    store.upsert(_result(attempt=1, payload=b"first"))
    store.upsert(_result(attempt=2, payload=b"second"))
    assert store.count() == 1
    landed = store.get("call-1:duration_rms:1.0.0")
    assert landed.attempt == 2
    assert landed.payload == b"second"


def test_same_call_different_functions_coexist():
    """The composite key exists so one function's result cannot destroy
    another's for the same call (§6)."""
    store = SqliteResultsStore()
    store.upsert(_result(function_id="duration_rms", payload=b"a"))
    store.upsert(_result(function_id="rms", payload=b"b"))
    assert store.count() == 2
    assert store.get("call-1:duration_rms:1.0.0").payload == b"a"
    assert store.get("call-1:rms:1.0.0").payload == b"b"


def test_wal_mode_enabled():
    """The aggregator reads while the sink writes; WAL is what makes that
    not a lock fight. (In-memory databases cannot use WAL, hence a file.)"""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        store = SqliteResultsStore(f"{tmp}/results.db")
        (journal,) = store._conn.execute("PRAGMA journal_mode").fetchone()
        assert journal == "wal"
        store.close()


def test_file_store_persists_across_reopen():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/results.db"
        first = SqliteResultsStore(path)
        first.upsert(_result())
        first.close()
        second = SqliteResultsStore(path)
        assert second.count() == 1
        assert second.get("call-1:duration_rms:1.0.0").payload == b'{"duration": 300.0}'
        second.close()


def test_stored_at_is_written_by_the_clock():
    clock = FakeClock(start=100.0)
    store = SqliteResultsStore(clock=clock)
    store.upsert(_result())
    (stored_at,) = store._conn.execute(
        "SELECT stored_at FROM results WHERE key = 'call-1:duration_rms:1.0.0'"
    ).fetchone()
    assert stored_at == clock.now().isoformat()