"""The poll loop (spec §5.2, §5.3, §5.4).

The failure this exists to prevent, quoting the spec: per-file processing takes
seconds to minutes, a naive poll loop exceeds max.poll.interval.ms, the broker
evicts the consumer mid-file, a rebalance fires, and another consumer
reprocesses the same file -- forever, if the function is slow enough.

So the loop below has one rule: it never blocks on work. Every iteration is
bounded by the poll timeout regardless of what the pool is doing.

  1. drain finished work           (cheap, non-blocking)
  2. resubmit retries whose backoff has elapsed
  3. fail files past their deadline
  4. pause/resume for backpressure  <- never "stop polling"
  5. poll once, dispatch at most one record
  6. commit the low-water mark

Backpressure is pause/resume rather than skipping the poll, because skipping the
poll is exactly the thing that gets the consumer evicted.
"""

from __future__ import annotations

import logging
import uuid

from .clock import SystemClock
from .codec import DecodeError
from .errors import TIMEOUT_CODE
from .metrics import (
    DLQ,
    FILE_LATENCY,
    IN_FLIGHT,
    PROCESSED,
    REALTIME_MULTIPLE,
    RETRIES,
    NullMetrics,
)
from .models import AudioReference, ErrorInfo, InboundMessage, Job, JobOutcome, Status
from .offsets import OffsetLedger

log = logging.getLogger(__name__)


class _InFlight:
    __slots__ = ("job", "deadline", "submitted_at")

    def __init__(self, job: Job, deadline: float, submitted_at: float):
        self.job = job
        self.deadline = deadline
        self.submitted_at = submitted_at


class FunctionRunner:
    def __init__(
        self,
        *,
        config,
        consumer,
        pool,
        codec,
        results,
        dlq,
        decoder=None,
        metrics=None,
        clock=None,
    ):
        self.config = config
        self.consumer = consumer
        self.pool = pool
        self.codec = codec
        self.results = results
        self.dlq = dlq
        # A function reads AudioReference off the internal topic. The hydrator
        # reads source metadata off the input topic. Everything between the two
        # -- poll/work decoupling, the ledger, retry and DLQ routing -- is the
        # same, so only the decode step is swapped.
        self.decode = decoder or codec.decode_reference
        self.metrics = metrics or NullMetrics()
        self.clock = clock or SystemClock()

        self.ledger = OffsetLedger()
        self._in_flight: dict[str, _InFlight] = {}
        self._retry_queue: list[tuple] = []  # (due_monotonic, Job)
        # Work the runner owns but the pool had no room for. Distinct from the
        # retry queue: nothing here has failed, so no attempt is consumed and no
        # backoff applies. See `_submit`.
        self._deferred: list[Job] = []
        self._paused = False
        self._running = False
        self._last_commit = self.clock.monotonic()
        self._labels = {
            "function_id": config.function_id,
            "function_version": config.function_version,
        }

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        self.consumer.subscribe(
            [self.config.input_topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

    def run(self) -> None:
        self._running = True
        self.start()
        try:
            while self._running:
                self.run_once()
        finally:
            self.close()

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        """Drain what has finished, commit it, and let the rest be reprocessed."""
        self._running = False
        try:
            self._drain_completions()
            self._commit(force=True)
        finally:
            self.pool.close()
            self.consumer.close()

    # -- one iteration -----------------------------------------------------

    def run_once(self) -> None:
        self._drain_completions()
        self._enforce_deadlines()
        self._submit_deferred()
        self._resubmit_due_retries()
        self._apply_backpressure()

        message = self.consumer.poll(self.config.poll_timeout_seconds)
        if message is not None:
            self._dispatch(message)

        self._commit()
        self.metrics.gauge(IN_FLIGHT, self.in_flight, **self._labels)

    @property
    def in_flight(self) -> int:
        return len(self._in_flight)

    @property
    def _occupancy(self) -> int:
        """What backpressure has to reason about.

        Three things can hold capacity, and only the first is in `_in_flight`:
        work the runner is tracking, work the pool is still running after a
        timeout abandoned it, and work deferred because the pool was full. Using
        only `_in_flight` resumes partitions while the workers are still busy,
        which is how a timeout turned into a crashed pod.
        """
        return max(len(self._in_flight), self.pool.in_flight()) + len(self._deferred)

    # -- dispatch ----------------------------------------------------------

    def _dispatch(self, message: InboundMessage) -> None:
        try:
            ref = self.decode(message.value)
        except DecodeError as exc:
            # Poison on arrival: it never reaches the pool, and the offset is
            # committed so it cannot accrue lag (spec §5.4).
            self._poison(message, ErrorInfo("DECODE_ERROR", str(exc), retryable=False))
            return

        job = Job(job_id=uuid.uuid4().hex, ref=ref, message=message, attempt=1)
        self.ledger.start(message.tp, message.offset)
        self._submit(job)

    def _submit(self, job: Job) -> None:
        if self.pool.in_flight() >= self.pool.max_in_flight:
            # The pool is genuinely full even though the runner may think a slot
            # is free. That happens after a timeout: `_enforce_deadlines` drops
            # the job from `_in_flight`, but `ProcessWorkerPool.cancel` cannot
            # interrupt a worker that is already running, so it abandons the
            # outcome and the slot stays occupied until the work returns.
            #
            # Submitting anyway used to raise out of `run_once` and kill the
            # process, taking every other in-flight file with it. Deferring
            # keeps the offset uncommitted -- the ledger already has it -- so
            # the worst case is redelivery, not loss.
            self._deferred.append(job)
            return

        now = self.clock.monotonic()
        self._in_flight[job.job_id] = _InFlight(
            job=job,
            deadline=now + self.config.per_file_timeout_seconds,
            submitted_at=now,
        )
        self.pool.submit(job)

    def _poison(self, message: InboundMessage, error: ErrorInfo) -> None:
        """DLQ the input and commit the offset.

        A FAILED result goes out too whenever the call_id is recoverable. The
        body did not parse, but the record key is the call_id (§4.2), so
        downstream can still be told this call was attempted and produced
        nothing -- which is the whole point of §5.4's "emit both".
        """
        call_id = _call_id_from_key(message.key)
        self.dlq.send(message, error, attempt=1, call_id=call_id)
        self.metrics.counter(DLQ, 1, reason=error.code, **self._labels)

        if call_id:
            self.results.emit_failure(
                Job(
                    job_id=uuid.uuid4().hex,
                    # object_key is derivable from call_id (§4.1) even though
                    # the reference itself was unreadable.
                    ref=AudioReference(call_id=call_id, object_key=f"{call_id}.flac"),
                    message=message,
                    attempt=1,
                ),
                error,
            )

        self.ledger.start(message.tp, message.offset)
        self.ledger.complete(message.tp, message.offset)

    # -- completion --------------------------------------------------------

    def _drain_completions(self) -> None:
        for outcome in self.pool.poll_completed():
            tracked = self._in_flight.pop(outcome.job.job_id, None)
            if tracked is None:
                # Cancelled by a deadline, or the partition was revoked while
                # the work was outstanding. Either way it is no longer ours.
                continue
            self._settle(outcome, tracked)

    def _settle(self, outcome: JobOutcome, tracked: _InFlight) -> None:
        job = outcome.job
        if outcome.status is Status.FAILED:
            self._handle_failure(job, outcome.error, outcome.started_at)
            return

        self.results.emit(outcome)
        self._complete(job)
        self._observe_latency(job, tracked)
        self.metrics.counter(PROCESSED, 1, status=outcome.status.name, **self._labels)

    def _handle_failure(self, job: Job, error: ErrorInfo | None, started_at=None) -> None:
        error = error or ErrorInfo("UNKNOWN", "no error reported", retryable=True)

        if error.retryable and job.attempt < self.config.retry_budget:
            self._schedule_retry(job, error)
            return

        # Budget spent, or unrecoverable. Both records go out (spec §5.4): the
        # input to the DLQ for replay, and a FAILED result so downstream can
        # tell "no result yet" from "no result ever".
        self.dlq.send(job.message, error, attempt=job.attempt, call_id=job.ref.call_id)
        self.results.emit_failure(job, error, started_at=started_at)
        self.metrics.counter(DLQ, 1, reason=error.code, **self._labels)
        self.metrics.counter(PROCESSED, 1, status=Status.FAILED.name, **self._labels)
        self._complete(job)

    def _schedule_retry(self, job: Job, error: ErrorInfo) -> None:
        delay = min(
            self.config.retry_backoff_seconds * (2 ** (job.attempt - 1)),
            self.config.retry_backoff_max_seconds,
        )
        retry = Job(
            job_id=uuid.uuid4().hex,
            ref=job.ref,
            message=job.message,
            attempt=job.attempt + 1,
        )
        self._retry_queue.append((self.clock.monotonic() + delay, retry))
        self.metrics.counter(RETRIES, 1, reason=error.code, **self._labels)
        log.info(
            "retrying %s attempt=%d in %.1fs (%s)",
            job.ref.call_id,
            retry.attempt,
            delay,
            error.code,
        )

    def _submit_deferred(self) -> None:
        """Work that arrived while the pool was full, oldest first."""
        while self._deferred and self.pool.in_flight() < self.pool.max_in_flight:
            self._submit(self._deferred.pop(0))

    def _resubmit_due_retries(self) -> None:
        if not self._retry_queue:
            return
        now = self.clock.monotonic()
        still_waiting = []
        for due, job in self._retry_queue:
            # Capacity is shared with fresh records; a retry that does not fit
            # waits rather than overcommitting the pool. The pool's own count is
            # what matters here -- see `_submit`.
            if due <= now and self.pool.in_flight() < self.pool.max_in_flight:
                self._submit(job)
            else:
                still_waiting.append((due, job))
        self._retry_queue = still_waiting

    def _enforce_deadlines(self) -> None:
        now = self.clock.monotonic()
        for job_id, tracked in list(self._in_flight.items()):
            if tracked.deadline > now:
                continue
            self.pool.cancel(job_id)
            del self._in_flight[job_id]
            self._handle_failure(
                tracked.job,
                ErrorInfo(
                    TIMEOUT_CODE,
                    f"exceeded per_file_timeout_seconds="
                    f"{self.config.per_file_timeout_seconds}",
                    retryable=True,
                ),
            )

    def _complete(self, job: Job) -> None:
        self.ledger.complete(job.message.tp, job.message.offset)

    def _observe_latency(self, job: Job, tracked: _InFlight) -> None:
        elapsed = self.clock.monotonic() - tracked.submitted_at
        self.metrics.histogram(FILE_LATENCY, elapsed, **self._labels)
        if elapsed > 0 and job.ref.duration_seconds:
            # The §8 onboarding floor is >=25x realtime per core; measuring it
            # here is what makes that contract checkable rather than aspirational.
            self.metrics.histogram(
                REALTIME_MULTIPLE, job.ref.duration_seconds / elapsed, **self._labels
            )

    # -- backpressure ------------------------------------------------------

    def _apply_backpressure(self) -> None:
        saturated = self._occupancy >= self.pool.max_in_flight
        if saturated and not self._paused:
            partitions = self.consumer.assignment()
            if partitions:
                self.consumer.pause(partitions)
                self._paused = True
        elif not saturated and self._paused:
            partitions = self.consumer.assignment()
            if partitions:
                self.consumer.resume(partitions)
            self._paused = False

    # -- commits -----------------------------------------------------------

    def _commit(self, force: bool = False) -> None:
        now = self.clock.monotonic()
        if not force and now - self._last_commit < self.config.commit_interval_seconds:
            return
        offsets = self.ledger.drain_committable()
        self._last_commit = now
        if offsets:
            self.consumer.commit(offsets)

    # -- rebalance ---------------------------------------------------------

    def _on_assign(self, partitions) -> None:
        # Cooperative-sticky: an incremental assign may arrive while other
        # partitions keep working, so nothing here may disturb existing state.
        self._paused = False
        log.info("assigned %s", partitions)

    def _on_revoke(self, partitions) -> None:
        """Give outstanding work a bounded chance to finish, commit what did,
        then drop the state. Whatever did not finish is reprocessed by the new
        owner -- at-least-once, as designed (spec §5.3)."""
        deadline = self.clock.monotonic() + self.config.rebalance_drain_seconds
        revoked = {_tp(p) for p in partitions}

        while self.clock.monotonic() < deadline:
            self._drain_completions()
            if not any(_tp(f.job.message.tp) in revoked for f in self._in_flight.values()):
                break
            self.clock.sleep(0.05)

        self._commit(force=True)

        for job_id, tracked in list(self._in_flight.items()):
            if _tp(tracked.job.message.tp) in revoked:
                self.pool.cancel(job_id)
                del self._in_flight[job_id]
        self._retry_queue = [
            entry for entry in self._retry_queue if _tp(entry[1].message.tp) not in revoked
        ]
        self.ledger.revoke(revoked)
        self._paused = False


def _call_id_from_key(key):
    if not key:
        return ""
    try:
        return key.decode()
    except UnicodeDecodeError:
        return ""


def _tp(value):
    if hasattr(value, "topic"):
        return (value.topic, value.partition)
    return tuple(value)
