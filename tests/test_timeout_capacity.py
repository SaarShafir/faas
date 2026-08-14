"""Capacity accounting when a timed-out worker will not let go.

Found by the local stress run, not by this suite, and the reason is worth
stating: `ManualPool.cancel` frees the slot it is asked to cancel. The real
`ProcessWorkerPool` cannot -- a worker already inside `process()` is not
interruptible without killing the process, so `cancel` abandons the *outcome*
and the slot stays occupied until the work returns on its own.

That divergence is the bug. The runner dropped the job from `_in_flight`,
concluded a slot was free, resumed its partitions and submitted the next file
into a pool that was still full, which raised out of `run_once` and killed the
process -- taking every other in-flight file with it and leaving queued results
undelivered. A per-file timeout, the one thing guaranteed to happen to a slow
function, crash-looped the pod.

`StubbornPool` below models the real semantics.
"""

from __future__ import annotations

import pytest

from faas_sdk.models import Job
from faas_sdk.testing import ManualPool


class StubbornPool(ManualPool):
    """A pool whose running work cannot be cancelled, like the real one."""

    def cancel(self, job_id: str) -> bool:
        job = self._by_job_id.get(job_id)
        if job is None:
            return False
        # Abandoned, not freed: the outcome will be discarded when it arrives,
        # but the worker is still holding the slot.
        self.cancelled.append(job.call_id)
        return True

    def finish_abandoned(self) -> None:
        """The worker finally returns, long after the deadline passed."""
        self._jobs.clear()
        self._by_job_id.clear()


@pytest.fixture
def stubborn_runner(config, consumer, producer, object_store, clock, metrics):
    from faas_sdk.codec import JsonCodec
    from faas_sdk.dlq import DeadLetterQueue
    from faas_sdk.results import ResultEmitter
    from faas_sdk.runner import FunctionRunner

    pool = StubbornPool(max_in_flight=1)
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
        clock=clock,
    )
    runner.start()
    return runner, pool


def test_a_timeout_does_not_crash_the_runner(stubborn_runner, consumer, clock):
    """The headline: one timed-out file must not take the process down."""
    from faas_sdk.testing import reference_message

    runner, pool = stubborn_runner

    consumer.feed(reference_message(partition=0, offset=1, call_id="c1"))
    runner.run_once()
    assert pool.in_flight() == 1

    clock.advance(runner.config.per_file_timeout_seconds + 1)
    consumer.feed(reference_message(partition=0, offset=2, call_id="c2"))

    # Before the fix this raised RuntimeError("pool at capacity") out of
    # run_once and the process exited.
    runner.run_once()
    runner.run_once()


def test_the_second_file_waits_rather_than_overcommitting(stubborn_runner, consumer, clock):
    from faas_sdk.testing import reference_message

    runner, pool = stubborn_runner

    consumer.feed(reference_message(partition=0, offset=1, call_id="c1"))
    runner.run_once()

    clock.advance(runner.config.per_file_timeout_seconds + 1)
    consumer.feed(reference_message(partition=0, offset=2, call_id="c2"))
    runner.run_once()

    # The abandoned worker still holds the only slot, so nothing new went in.
    assert pool.in_flight() == 1
    assert [j.call_id for j in pool.submitted] == ["c1"]


def test_the_waiting_file_runs_once_the_worker_finally_returns(
    stubborn_runner, consumer, clock
):
    from faas_sdk.testing import reference_message

    runner, pool = stubborn_runner

    consumer.feed(reference_message(partition=0, offset=1, call_id="c1"))
    runner.run_once()
    clock.advance(runner.config.per_file_timeout_seconds + 1)
    consumer.feed(reference_message(partition=0, offset=2, call_id="c2"))
    runner.run_once()

    pool.finish_abandoned()
    runner.run_once()

    assert [j.call_id for j in pool.submitted] == ["c1", "c2"]


def test_partitions_stay_paused_while_an_abandoned_worker_holds_the_slot(
    stubborn_runner, consumer, clock
):
    """Backpressure has to count the slot the timed-out worker still owns.

    Resuming here is what let the next file be polled and submitted into a full
    pool in the first place.
    """
    from faas_sdk.testing import reference_message

    runner, pool = stubborn_runner

    consumer.feed(reference_message(partition=0, offset=1, call_id="c1"))
    runner.run_once()
    # Backpressure is applied at the top of the loop, so the pause lands on the
    # iteration after the one that filled the pool.
    runner.run_once()
    assert consumer.paused, "a saturated pool must pause its partitions"

    clock.advance(runner.config.per_file_timeout_seconds + 1)
    runner.run_once()

    assert consumer.paused, "the abandoned worker still holds the slot"

    pool.finish_abandoned()
    runner.run_once()
    assert not consumer.paused


def test_the_timed_out_file_is_still_retried(stubborn_runner, consumer, clock, config):
    """Deferring must not swallow the retry: the file that timed out still owes
    an attempt, and its offset stays uncommitted until it is settled."""
    from faas_sdk.testing import reference_message

    runner, pool = stubborn_runner

    consumer.feed(reference_message(partition=0, offset=1, call_id="c1"))
    runner.run_once()
    clock.advance(runner.config.per_file_timeout_seconds + 1)
    runner.run_once()

    pool.finish_abandoned()
    clock.advance(config.retry_backoff_seconds + 1)
    runner.run_once()

    assert [j.call_id for j in pool.submitted] == ["c1", "c1"]
    assert pool.submitted[-1].attempt == 2
    assert consumer.commits == [], "an unfinished file must not be committed"


def test_a_job_that_times_out_repeatedly_still_reaches_the_dlq(
    stubborn_runner, consumer, clock, config, producer
):
    """The end of the ladder. Retries are bounded even when every attempt times
    out, or a permanently slow file would be redelivered for ever."""
    from faas_sdk.testing import reference_message

    runner, pool = stubborn_runner
    consumer.feed(reference_message(partition=0, offset=1, call_id="c1"))

    for _ in range(config.retry_budget + 2):
        runner.run_once()
        clock.advance(runner.config.per_file_timeout_seconds + 1)
        runner.run_once()
        pool.finish_abandoned()
        clock.advance(config.retry_backoff_max_seconds)
        runner.run_once()

    dlq = [r for r in producer.records if r.topic == config.dlq_topic]
    assert len(dlq) == 1, "the file should reach the DLQ exactly once"


def test_the_pool_still_refuses_an_overcommitted_submit():
    """The pool's own guard stays: it is the invariant that caught this, and
    removing it would turn the next accounting bug into silent overcommit."""
    from faas_sdk.models import AudioReference

    def job(job_id, call_id):
        return Job(
            job_id=job_id,
            ref=AudioReference(call_id=call_id, object_key=f"{call_id}.flac"),
            message=None,
            attempt=1,
        )

    pool = ManualPool(max_in_flight=1)
    pool.submit(job("a", "c1"))
    with pytest.raises(RuntimeError, match="capacity"):
        pool.submit(job("b", "c2"))


