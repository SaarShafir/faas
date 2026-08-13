"""Results landing (spec §4.4): the sink service persists results.

The port is the seam: in production this is "some topic or a db", on a dev box
it is SQLite. The sink never cares which. What the sink must guarantee is the
same either way:

  - idempotent on the §6 composite key, because at-least-once delivery means
    duplicates are not just possible but expected (spec §5.3);
  - FAILED and SKIPPED results land exactly like SUCCESS ones, so a consumer
    can tell "no result yet" from "no result ever" (§5.4);
  - claim-checked payloads are stored as the ref, never fetched: the object
    store has its own 24h TTL, and consumers fetch by ref when they actually
    want the payload.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Protocol

from faas_sdk.models import ErrorInfo, Result, Status

_SCHEMA = """
CREATE TABLE IF NOT EXISTS results (
    key TEXT PRIMARY KEY,
    envelope_version INTEGER NOT NULL,
    call_id TEXT NOT NULL,
    function_id TEXT NOT NULL,
    function_version TEXT NOT NULL,
    status INTEGER NOT NULL,
    error_code TEXT,
    error_message TEXT,
    error_retryable INTEGER,
    payload_schema_version TEXT NOT NULL,
    payload_content_type TEXT NOT NULL,
    payload BLOB,
    payload_ref TEXT,
    input_object_key TEXT NOT NULL,
    input_offset INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    ingested_at TEXT,
    started_at TEXT,
    completed_at TEXT,
    stored_at TEXT NOT NULL
)
"""


class ResultsStore(Protocol):
    def upsert(self, result: Result) -> None: ...

    def get(self, key: str) -> Result | None: ...

    def count(self) -> int: ...

    def close(self) -> None: ...


class SqliteResultsStore:
    """SQLite behind the store port.

    The production sink is explicitly "some topic or a db in real prod"; this
    implementation exists so the sink's logic is real, running code today. One
    connection, WAL journal: the aggregator (step 7) will read while the sink
    writes.
    """

    def __init__(self, path: str | Path = ":memory:", *, clock=None):
        self.path = str(path)
        self._clock = clock or _system_clock()
        self._conn = sqlite3.connect(self.path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def upsert(self, result: Result) -> None:
        now = self._clock.now().isoformat()
        self._conn.execute(
            """
            INSERT INTO results (
                key, envelope_version, call_id, function_id, function_version,
                status, error_code, error_message, error_retryable,
                payload_schema_version, payload_content_type, payload, payload_ref,
                input_object_key, input_offset, attempt,
                ingested_at, started_at, completed_at, stored_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                envelope_version = excluded.envelope_version,
                call_id = excluded.call_id,
                function_id = excluded.function_id,
                function_version = excluded.function_version,
                status = excluded.status,
                error_code = excluded.error_code,
                error_message = excluded.error_message,
                error_retryable = excluded.error_retryable,
                payload_schema_version = excluded.payload_schema_version,
                payload_content_type = excluded.payload_content_type,
                payload = excluded.payload,
                payload_ref = excluded.payload_ref,
                input_object_key = excluded.input_object_key,
                input_offset = excluded.input_offset,
                attempt = excluded.attempt,
                ingested_at = excluded.ingested_at,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at,
                stored_at = excluded.stored_at
            """,
            (
                result.key.decode(),
                result.envelope_version,
                result.call_id,
                result.function_id,
                result.function_version,
                int(result.status),
                result.error.code if result.error else None,
                result.error.message if result.error else None,
                _int(result.error.retryable) if result.error else None,
                result.payload_schema_version,
                result.payload_content_type,
                result.payload,
                result.payload_ref,
                result.input_object_key,
                result.input_offset,
                result.attempt,
                _iso(result.ingested_at),
                _iso(result.started_at),
                _iso(result.completed_at),
                now,
            ),
        )
        self._conn.commit()

    def get(self, key: str) -> Result | None:
        row = self._conn.execute(
            "SELECT * FROM results WHERE key = ?", (key,)
        ).fetchone()
        return _row_to_result(row) if row else None

    def count(self) -> int:
        (total,) = self._conn.execute("SELECT COUNT(*) FROM results").fetchone()
        return total

    def close(self) -> None:
        self._conn.close()


_COLUMNS = [
    "key", "envelope_version", "call_id", "function_id", "function_version",
    "status", "error_code", "error_message", "error_retryable",
    "payload_schema_version", "payload_content_type", "payload", "payload_ref",
    "input_object_key", "input_offset", "attempt",
    "ingested_at", "started_at", "completed_at", "stored_at",
]


def _row_to_result(row) -> Result:
    values = dict(zip(_COLUMNS, row, strict=True))
    error = None
    if values["error_code"] is not None:
        error = ErrorInfo(
            code=values["error_code"],
            message=values["error_message"] or "",
            retryable=bool(values["error_retryable"]),
        )
    return Result(
        envelope_version=values["envelope_version"],
        call_id=values["call_id"],
        function_id=values["function_id"],
        function_version=values["function_version"],
        status=Status(values["status"]),
        error=error,
        payload_schema_version=values["payload_schema_version"],
        payload_content_type=values["payload_content_type"],
        payload=values["payload"],
        payload_ref=values["payload_ref"],
        input_object_key=values["input_object_key"],
        input_offset=values["input_offset"],
        attempt=values["attempt"],
        ingested_at=_parse(values["ingested_at"]),
        started_at=_parse(values["started_at"]),
        completed_at=_parse(values["completed_at"]),
    )


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _int(value: bool) -> int:
    return 1 if value else 0


def _system_clock():
    from faas_sdk.clock import SystemClock

    return SystemClock()


__all__ = ["ResultsStore", "SqliteResultsStore"]