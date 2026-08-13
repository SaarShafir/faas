"""Error taxonomy.

The only thing the runner needs from an exception is: retry it, or DLQ it.
Function authors raise these; anything else is treated as retryable-but-bounded
(see `pool.classify`), because failing closed would DLQ every transient blip.
"""

from __future__ import annotations


class FaaSError(Exception):
    code = "UNHANDLED"
    retryable = True

    def __init__(self, message: str = "", *, code: str = "", retryable: bool = None):
        super().__init__(message)
        if code:
            self.code = code
        if retryable is not None:
            self.retryable = retryable


class TransientError(FaaSError):
    """Recoverable: S3 hiccup, Audio API 5xx, model server not ready yet."""

    code = "TRANSIENT"
    retryable = True


class PoisonMessageError(FaaSError):
    """Unrecoverable for this input. Straight to the DLQ; commit the offset.

    Spec §5.4: a poison message must never accrue unbounded lag while every
    other function looks healthy.
    """

    code = "POISON"
    retryable = False


class ObjectMissingError(TransientError):
    """Dead object key -- the object outlived by our lag (spec §5.4).

    Retryable because the recovery path is a re-fetch from the Audio API,
    rate-limited separately from live hydration.
    """

    code = "OBJECT_MISSING"


class TimeoutError_(TransientError):  # noqa: N801 - avoid shadowing builtins
    code = "TIMEOUT"


TIMEOUT_CODE = "TIMEOUT"
