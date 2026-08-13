"""The in-flight pool (spec §5.2).

The pool is what makes in-flight concurrency independent of partition count.
It is also the thing that must never be a plain thread pool for real work:
decode plus inference is CPU-bound and GIL-blocked. InlineWorkerPool exists for
tests and for the single-slot pod default; ProcessWorkerPool is the real one.
"""

import pytest

from faas_sdk.errors import PoisonMessageError, TransientError
from faas_sdk.models import FunctionResult, Status
from faas_sdk.pool import InlineWorkerPool
from faas_sdk.testing import audio_handle_factory, job, reference


class DurationRms:
    function_id = "duration_rms"
    function_version = "1.0.0"

    def process(self, ref, audio):
        return FunctionResult(payload=str(ref.duration_seconds).encode(), schema_version="1")


class Boom:
    function_id = "boom"
    function_version = "1.0.0"

    def __init__(self, exc):
        self.exc = exc

    def process(self, ref, audio):
        raise self.exc


def _pool(function, **kwargs):
    return InlineWorkerPool(
        function=function, audio_handle_factory=audio_handle_factory(), **kwargs
    )


def test_successful_work_yields_a_success_outcome():
    pool = _pool(DurationRms())
    pool.submit(job(call_id="c1", duration_seconds=300.0))

    (outcome,) = pool.poll_completed()
    assert outcome.status is Status.SUCCESS
    assert outcome.payload == b"300.0"
    assert outcome.schema_version == "1"
    assert outcome.job.call_id == "c1"
    assert outcome.started_at is not None and outcome.completed_at is not None


def test_returning_none_is_a_skip():
    class Skipper(DurationRms):
        def process(self, ref, audio):
            return None

    pool = _pool(Skipper())
    pool.submit(job(call_id="c1"))
    (outcome,) = pool.poll_completed()
    assert outcome.status is Status.SKIPPED


def test_transient_errors_are_marked_retryable():
    pool = _pool(Boom(TransientError("s3 timed out", code="S3_TIMEOUT")))
    pool.submit(job(call_id="c1"))

    (outcome,) = pool.poll_completed()
    assert outcome.status is Status.FAILED
    assert outcome.error.code == "S3_TIMEOUT"
    assert outcome.error.retryable is True


def test_poison_messages_are_marked_non_retryable():
    pool = _pool(Boom(PoisonMessageError("not FLAC", code="DECODE_ERROR")))
    pool.submit(job(call_id="c1"))

    (outcome,) = pool.poll_completed()
    assert outcome.error.code == "DECODE_ERROR"
    assert outcome.error.retryable is False


def test_unhandled_exceptions_are_retryable_but_bounded():
    """The retry budget bounds them; failing closed would DLQ every blip."""
    pool = _pool(Boom(ZeroDivisionError("oops")))
    pool.submit(job(call_id="c1"))

    (outcome,) = pool.poll_completed()
    assert outcome.error.code == "UNHANDLED"
    assert outcome.error.retryable is True
    assert "ZeroDivisionError" in outcome.error.message


def test_pool_reports_in_flight_depth_and_capacity():
    pool = _pool(DurationRms(), max_in_flight=4)
    assert pool.max_in_flight == 4
    assert pool.in_flight() == 0

    pool.submit(job(call_id="c1"))
    # Inline pool completes on submit; the outcome is queued, not lost.
    assert pool.in_flight() == 0
    assert len(pool.poll_completed()) == 1


def test_deferred_mode_holds_work_so_depth_is_observable():
    pool = _pool(DurationRms(), max_in_flight=2, defer=True)
    pool.submit(job(call_id="c1"))
    pool.submit(job(call_id="c2"))

    assert pool.in_flight() == 2
    assert pool.poll_completed() == []

    pool.run_pending()
    assert pool.in_flight() == 0
    assert len(pool.poll_completed()) == 2


def test_pool_refuses_to_exceed_capacity():
    """A backpressure bug in the runner should fail loudly, not overcommit."""
    pool = _pool(DurationRms(), max_in_flight=1, defer=True)
    pool.submit(job(call_id="c1"))

    with pytest.raises(RuntimeError, match="capacity"):
        pool.submit(job(call_id="c2"))


def test_cancel_removes_pending_work():
    pool = _pool(DurationRms(), max_in_flight=2, defer=True)
    j = job(call_id="c1")
    pool.submit(j)

    assert pool.cancel(j.job_id) is True
    assert pool.in_flight() == 0
    assert pool.cancel(j.job_id) is False


def test_poll_completed_drains():
    pool = _pool(DurationRms())
    pool.submit(job(call_id="c1"))
    pool.submit(job(call_id="c2"))

    assert len(pool.poll_completed()) == 2
    assert pool.poll_completed() == []


def test_audio_handle_is_built_per_job_and_passed_to_process():
    seen = {}

    class Peek(DurationRms):
        def process(self, ref, audio):
            seen["object_key"] = audio.object_key
            return FunctionResult(payload=b"ok")

    pool = _pool(Peek())
    pool.submit(job(call_id="c1"))
    pool.poll_completed()

    assert seen["object_key"] == "c1.flac"


def test_reference_reaches_the_function_intact():
    seen = {}

    class Peek(DurationRms):
        def process(self, ref, audio):
            seen["ref"] = ref
            return FunctionResult(payload=b"ok")

    pool = _pool(Peek())
    pool.submit(job(call_id="c1", duration_seconds=42.5))
    pool.poll_completed()

    assert seen["ref"] == reference(call_id="c1", duration_seconds=42.5)
