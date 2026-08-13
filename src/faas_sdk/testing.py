"""Test doubles, shipped as part of the SDK.

Function authors get the same fakes the SDK tests use, so a function can be
tested end-to-end -- dispatch, retry, DLQ, envelope -- without a broker, an
object store or a subprocess.
"""

from __future__ import annotations

import functools
import itertools
import time
import uuid
from datetime import datetime, timedelta, timezone

from .codec import JsonCodec
from .models import (
    AudioReference,
    ErrorInfo,
    FunctionResult,
    InboundMessage,
    Job,
    JobOutcome,
    OutboundRecord,
    Status,
    TopicPartition,
)

DEFAULT_TOPIC = "faas.audio.internal"
_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


# -- builders --------------------------------------------------------------


def reference(call_id: str = "call-1", duration_seconds: float = 300.0, **overrides):
    fields = dict(
        call_id=call_id,
        object_key=f"{call_id}.flac",
        sample_rate=16000,
        channels=1,
        duration_seconds=duration_seconds,
        ingested_at=_EPOCH,
        hydrated_at=_EPOCH + timedelta(seconds=30),
    )
    fields.update(overrides)
    return AudioReference(**fields)


def reference_message(
    partition: int = 0,
    offset: int = 0,
    call_id: str = "call-1",
    topic: str = DEFAULT_TOPIC,
    raw_value: bytes | None = None,
    codec=None,
    **ref_overrides,
) -> InboundMessage:
    if raw_value is not None:
        value = raw_value
        key = call_id.encode()
    else:
        ref = reference(call_id=call_id, **ref_overrides)
        value = (codec or JsonCodec()).encode_reference(ref)
        key = ref.call_id.encode()
    return InboundMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        value=value,
        key=key,
        timestamp_ms=0,
    )


def job(call_id: str = "call-1", offset: int = 0, attempt: int = 1, **ref_overrides) -> Job:
    return Job(
        job_id=uuid.uuid4().hex,
        ref=reference(call_id=call_id, **ref_overrides),
        message=reference_message(offset=offset, call_id=call_id, **ref_overrides),
        attempt=attempt,
    )


class SlowFunction:
    """A function that takes longer than any sane max.poll.interval.ms.

    Exists so the §5.2 eviction scenario can be reproduced against a real
    broker. Defined at module level, and built through the module-level factory
    below, because a ProcessWorkerPool worker has to import it -- on Windows
    that means spawn, and spawn re-imports rather than forking.
    """

    function_id = "slow"
    function_version = "1.0.0"

    def __init__(self, seconds: float):
        self.seconds = seconds

    def process(self, ref, audio):
        time.sleep(self.seconds)
        return FunctionResult(
            payload=b'{"slept": true}',
            schema_version="1",
            content_type="application/json",
        )


def _build_slow_function(seconds: float) -> SlowFunction:
    return SlowFunction(seconds)


def slow_function_factory(seconds: float):
    """A picklable zero-arg factory, as ProcessWorkerPool requires."""
    return functools.partial(_build_slow_function, seconds)


def stub_audio_handle_factory():
    """Zero-arg, picklable, and never touches S3."""
    return audio_handle_factory()


class StubAudioHandle:
    """Stands in for a real AudioHandle without touching S3 or libsndfile."""

    def __init__(self, ref: AudioReference, samples=None):
        self.ref = ref
        self.object_key = ref.object_key
        self.sample_rate = ref.sample_rate
        self._samples = samples if samples is not None else []
        self.reads = 0

    def bytes(self) -> bytes:
        self.reads += 1
        return b""

    def samples(self):
        self.reads += 1
        return self._samples


def audio_handle_factory(samples=None):
    def build(ref: AudioReference) -> StubAudioHandle:
        return StubAudioHandle(ref, samples=samples)

    return build


# -- fakes -----------------------------------------------------------------


class FakeClock:
    def __init__(self, start: float = 0.0, tick: float = 0.01):
        self.now_monotonic = start
        self.tick = tick
        self.auto_advance = False
        self._wall = _EPOCH

    def monotonic(self) -> float:
        if self.auto_advance:
            self.now_monotonic += self.tick
        return self.now_monotonic

    def now(self) -> datetime:
        return self._wall + timedelta(seconds=self.now_monotonic)

    def sleep(self, seconds: float) -> None:
        self.now_monotonic += seconds

    def advance(self, seconds: float) -> None:
        self.now_monotonic += seconds


class FakeConsumer:
    def __init__(self, assignment=(("faas.audio.internal", 0),)):
        self._assignment = [_tp(p) for p in assignment]
        self._queue: list[InboundMessage] = []
        self.paused = set()
        self.commits: list[list] = []
        self.poll_count = 0
        self.closed = False
        self.topics: list[str] = []
        self._on_assign = None
        self._on_revoke = None

    @property
    def topic(self) -> str:
        return self._assignment[0][0] if self._assignment else DEFAULT_TOPIC

    def feed(self, *messages: InboundMessage) -> None:
        self._queue.extend(messages)

    def subscribe(self, topics, on_assign=None, on_revoke=None) -> None:
        self.topics = list(topics)
        self._on_assign = on_assign
        self._on_revoke = on_revoke

    def poll(self, timeout: float) -> InboundMessage | None:
        self.poll_count += 1
        for index, message in enumerate(self._queue):
            if _tp(message.tp) in self.paused:
                continue
            return self._queue.pop(index)
        return None

    def commit(self, offsets) -> None:
        self.commits.append([(_tp(tp), offset) for tp, offset in offsets])

    def pause(self, partitions) -> None:
        self.paused |= {_tp(p) for p in partitions}

    def resume(self, partitions) -> None:
        self.paused -= {_tp(p) for p in partitions}

    def assignment(self):
        return [TopicPartition(*p) for p in self._assignment]

    def close(self) -> None:
        self.closed = True

    def trigger_assign(self, partitions) -> None:
        self._assignment = [_tp(p) for p in partitions]
        if self._on_assign:
            self._on_assign(self.assignment())

    def trigger_revoke(self, partitions) -> None:
        if self._on_revoke is None:
            raise AssertionError("runner.start() was never called; no rebalance callback")
        revoked = {_tp(p) for p in partitions}
        self._on_revoke([TopicPartition(*p) for p in revoked])
        self._assignment = [p for p in self._assignment if p not in revoked]
        self.paused -= revoked


class FakeProducer:
    def __init__(self):
        self.records: list[OutboundRecord] = []
        self.flushes = 0

    def produce(self, record: OutboundRecord) -> None:
        self.records.append(record)

    def flush(self, timeout: float = 10.0) -> int:
        self.flushes += 1
        return 0

    def records_for(self, topic: str) -> list[OutboundRecord]:
        return [r for r in self.records if r.topic == topic]


class FakeObjectStore:
    def __init__(self, objects: dict[str, bytes] | None = None):
        self.objects: dict[str, bytes] = dict(objects or {})
        self.gets: list[str] = []

    def get(self, key: str) -> bytes:
        self.gets.append(key)
        try:
            return self.objects[key]
        except KeyError:
            from .errors import ObjectMissingError

            raise ObjectMissingError(f"no object at {key}") from None

    def put(self, key: str, body: bytes, content_type: str = "") -> None:
        self.objects[key] = body


class ManualPool:
    """A pool that holds work until a test releases it.

    This is what makes poll/work decoupling testable: jobs stay in flight for as
    many loop iterations as the test wants, exactly like a slow function.
    """

    def __init__(self, max_in_flight: int = 2):
        self.max_in_flight = max_in_flight
        self._jobs: dict[str, Job] = {}  # keyed by call_id, for ergonomic tests
        self._by_job_id: dict[str, Job] = {}
        self._completed: list[JobOutcome] = []
        self.cancelled: list[str] = []
        self.submitted: list[Job] = []
        self.closed = False
        self._counter = itertools.count()

    # pool protocol
    def submit(self, job: Job) -> None:
        if self.in_flight() >= self.max_in_flight:
            raise RuntimeError(f"pool at capacity ({self.max_in_flight})")
        self._jobs[job.call_id] = job
        self._by_job_id[job.job_id] = job
        self.submitted.append(job)

    def poll_completed(self) -> list[JobOutcome]:
        done, self._completed = self._completed, []
        return done

    def in_flight(self) -> int:
        return len(self._jobs)

    def cancel(self, job_id: str) -> bool:
        job = self._by_job_id.pop(job_id, None)
        if job is None:
            return False
        self._jobs.pop(job.call_id, None)
        self.cancelled.append(job.call_id)
        return True

    def close(self, timeout: float = 30.0) -> None:
        self.closed = True

    # test controls
    def job_for(self, call_id: str) -> Job:
        return self._jobs[call_id]

    def succeed(
        self,
        call_id: str,
        payload: bytes = b"{}",
        schema_version: str = "",
        content_type: str = "application/json",
    ) -> None:
        self._finish(
            call_id,
            JobOutcome(
                job=self._jobs[call_id],
                status=Status.SUCCESS,
                payload=payload,
                schema_version=schema_version,
                content_type=content_type,
                started_at=_EPOCH,
                completed_at=_EPOCH + timedelta(seconds=1),
            ),
        )

    def succeed_all(self, payload: bytes = b"{}") -> None:
        for call_id in list(self._jobs):
            self.succeed(call_id, payload=payload)

    def skip(self, call_id: str) -> None:
        self._finish(
            call_id,
            JobOutcome(job=self._jobs[call_id], status=Status.SKIPPED, started_at=_EPOCH),
        )

    def fail(
        self,
        call_id: str,
        code: str = "UNHANDLED",
        retryable: bool = True,
        message: str = "",
    ) -> None:
        self._finish(
            call_id,
            JobOutcome(
                job=self._jobs[call_id],
                status=Status.FAILED,
                error=ErrorInfo(code=code, message=message or code, retryable=retryable),
                started_at=_EPOCH,
                completed_at=_EPOCH + timedelta(seconds=1),
            ),
        )

    def _finish(self, call_id: str, outcome: JobOutcome) -> None:
        job = self._jobs.pop(call_id)
        self._by_job_id.pop(job.job_id, None)
        self._completed.append(outcome)


def _tp(value):
    if hasattr(value, "topic"):
        return (value.topic, value.partition)
    return tuple(value)
