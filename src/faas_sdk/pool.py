"""The bounded in-flight pool (spec §5.2).

In-flight concurrency is independent of partition count: partitions bound
*process* count, this bounds concurrency. 200 partitions x 20 in-flight is
4,000 concurrent files with no repartitioning.

The pool must not be threads. Decode plus inference is CPU-bound and
GIL-blocked, so a thread pool buys nothing and hides the cost. Two
implementations here:

  ProcessWorkerPool -- real concurrency inside one pod.
  InlineWorkerPool  -- no concurrency at all, for tests and for the spec's
                       preferred default: one consumer per pod with shallow
                       in-flight depth, letting the autoscaler scale pod count.
                       The failure unit is then a pod and commit reasoning is
                       far simpler.
"""

from __future__ import annotations

import traceback
from collections.abc import Callable

from .clock import SystemClock
from .errors import FaaSError
from .models import ErrorInfo, FunctionResult, Job, JobOutcome, Status

__all__ = ["InlineWorkerPool", "ProcessWorkerPool", "classify", "execute"]


def classify(exc: BaseException) -> ErrorInfo:
    """Map an exception to the only question the runner asks: retry, or DLQ?"""
    if isinstance(exc, FaaSError):
        return ErrorInfo(code=exc.code, message=str(exc), retryable=exc.retryable)
    # Unknown failures are retryable: the retry budget bounds them, whereas
    # failing closed would DLQ every transient blip in a dependency.
    return ErrorInfo(
        code="UNHANDLED",
        message=f"{type(exc).__name__}: {exc}",
        retryable=True,
    )


def execute(function, job: Job, audio_handle_factory: Callable, clock=None) -> JobOutcome:
    """Run one file. Never raises: the outcome is the error channel."""
    clock = clock or SystemClock()
    started_at = clock.now()
    try:
        audio = audio_handle_factory(job.ref)
        returned = function.process(job.ref, audio)
    except BaseException as exc:  # noqa: BLE001 - the outcome is the error channel
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        error = classify(exc)
        if error.code == "UNHANDLED":
            error = ErrorInfo(
                code=error.code,
                message=error.message + "\n" + traceback.format_exc(limit=8),
                retryable=error.retryable,
            )
        return JobOutcome(
            job=job,
            status=Status.FAILED,
            error=error,
            started_at=started_at,
            completed_at=clock.now(),
        )

    if returned is None:
        return JobOutcome(
            job=job,
            status=Status.SKIPPED,
            started_at=started_at,
            completed_at=clock.now(),
        )
    if not isinstance(returned, FunctionResult):
        return JobOutcome(
            job=job,
            status=Status.FAILED,
            error=ErrorInfo(
                code="BAD_RETURN",
                message=f"process() returned {type(returned).__name__}, expected FunctionResult",
                retryable=False,
            ),
            started_at=started_at,
            completed_at=clock.now(),
        )
    return JobOutcome(
        job=job,
        status=returned.status,
        payload=returned.payload,
        schema_version=returned.schema_version,
        content_type=returned.content_type,
        started_at=started_at,
        completed_at=clock.now(),
    )


class InlineWorkerPool:
    """Runs work on the calling thread.

    With `defer=True` submission is queued until `run_pending()`, which makes
    in-flight depth observable in tests without real concurrency.
    """

    def __init__(
        self,
        *,
        function,
        audio_handle_factory: Callable,
        max_in_flight: int = 1,
        defer: bool = False,
        clock=None,
    ):
        self.function = function
        self.audio_handle_factory = audio_handle_factory
        self.max_in_flight = max_in_flight
        self.defer = defer
        self._clock = clock or SystemClock()
        self._pending: dict[str, Job] = {}
        self._completed: list[JobOutcome] = []

    def submit(self, job: Job) -> None:
        if self.in_flight() >= self.max_in_flight:
            raise RuntimeError(
                f"pool at capacity ({self.max_in_flight}): the runner should have "
                "paused its partitions before submitting"
            )
        if self.defer:
            self._pending[job.job_id] = job
            return
        self._completed.append(execute(self.function, job, self.audio_handle_factory, self._clock))

    def run_pending(self) -> None:
        jobs, self._pending = self._pending, {}
        for job in jobs.values():
            self._completed.append(
                execute(self.function, job, self.audio_handle_factory, self._clock)
            )

    def poll_completed(self) -> list[JobOutcome]:
        done, self._completed = self._completed, []
        return done

    def in_flight(self) -> int:
        return len(self._pending)

    def cancel(self, job_id: str) -> bool:
        return self._pending.pop(job_id, None) is not None

    def close(self, timeout: float = 30.0) -> None:
        self._pending.clear()


_WORKER_FUNCTION = None
_WORKER_AUDIO_FACTORY = None


def _init_worker(function_factory, audio_handle_factory):
    global _WORKER_FUNCTION, _WORKER_AUDIO_FACTORY
    _WORKER_FUNCTION = function_factory()
    _WORKER_AUDIO_FACTORY = audio_handle_factory()


def _run_in_worker(job: Job) -> JobOutcome:
    return execute(_WORKER_FUNCTION, job, _WORKER_AUDIO_FACTORY)


class ProcessWorkerPool:
    """Process-backed pool.

    `function_factory` and `audio_handle_factory` are zero-arg module-level
    callables, invoked once per worker process. Model weights, S3 clients and
    CUDA contexts are built there and never crossed over a pickle boundary --
    only `Job` and `JobOutcome` travel.
    """

    def __init__(
        self,
        *,
        function_factory: Callable,
        audio_handle_factory: Callable,
        max_in_flight: int = 1,
        mp_context: str | None = None,
    ):
        import multiprocessing
        from concurrent.futures import ProcessPoolExecutor

        self.max_in_flight = max_in_flight
        context = multiprocessing.get_context(mp_context) if mp_context else None
        self._executor = ProcessPoolExecutor(
            max_workers=max_in_flight,
            initializer=_init_worker,
            initargs=(function_factory, audio_handle_factory),
            **({"mp_context": context} if context else {}),
        )
        self._futures: dict[str, tuple] = {}
        self._abandoned: set = set()

    def submit(self, job: Job) -> None:
        if self.in_flight() >= self.max_in_flight:
            raise RuntimeError(
                f"pool at capacity ({self.max_in_flight}): the runner should have "
                "paused its partitions before submitting"
            )
        self._futures[job.job_id] = (job, self._executor.submit(_run_in_worker, job))

    def poll_completed(self) -> list[JobOutcome]:
        done = []
        for job_id, (job, future) in list(self._futures.items()):
            if not future.done():
                continue
            del self._futures[job_id]
            if job_id in self._abandoned:
                self._abandoned.discard(job_id)
                continue
            try:
                done.append(future.result())
            except BaseException as exc:  # noqa: BLE001 - worker died mid-file
                done.append(_worker_died(job, exc))
        return done

    def in_flight(self) -> int:
        return len(self._futures)

    def cancel(self, job_id: str) -> bool:
        """Best effort.

        A queued job is cancelled outright. A job already running in a worker
        cannot be interrupted without killing the process, so it is abandoned:
        its outcome is discarded when it eventually arrives, and the runner is
        free to retry or DLQ. The worker keeps burning CPU until it finishes --
        which is one more reason the spec prefers shallow in-flight depth and a
        pod as the failure unit (§5.2).
        """
        entry = self._futures.get(job_id)
        if entry is None:
            return False
        if entry[1].cancel():
            del self._futures[job_id]
            return True
        self._abandoned.add(job_id)
        return True

    def close(self, timeout: float = 30.0) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)


def _worker_died(job: Job, exc: BaseException) -> JobOutcome:
    """A dead worker is retryable: the pool respawns and the file is re-run."""
    return JobOutcome(
        job=job,
        status=Status.FAILED,
        error=ErrorInfo(
            code="WORKER_DIED",
            message=f"{type(exc).__name__}: {exc}",
            retryable=True,
        ),
    )
