"""faas-sdk -- the layer every audio function inherits (spec §5).

Function authors touch exactly two things:

    from faas_sdk import FunctionResult, run

    class DurationRms:
        function_id = "duration_rms"
        function_version = "1.0.0"

        def process(self, ref, audio):
            samples = audio.samples()
            rms = float((samples ** 2).mean() ** 0.5)
            return FunctionResult(
                payload=json.dumps({"rms": rms}).encode(),
                schema_version="1",
                content_type="application/json",
            )

    if __name__ == "__main__":
        run(DurationRms)   # the class, not an instance: each worker builds its own

Kafka, offsets, retries, the DLQ, the results envelope, the claim check and the
metrics are all on the other side of that line, which is the point: the
poll/work semantics and commit correctness are miserable to change later.
"""

from .audio import AudioHandle, audio_handle_factory
from .config import FunctionConfig, Resources
from .errors import (
    FaaSError,
    ObjectMissingError,
    PoisonMessageError,
    TransientError,
)
from .function import Function
from .models import (
    AudioReference,
    ErrorInfo,
    FunctionResult,
    Result,
    Status,
    TopicPartition,
)
from .pool import InlineWorkerPool, ProcessWorkerPool
from .runner import FunctionRunner

__all__ = [
    "AudioHandle",
    "AudioReference",
    "ErrorInfo",
    "FaaSError",
    "Function",
    "FunctionConfig",
    "FunctionResult",
    "FunctionRunner",
    "InlineWorkerPool",
    "ObjectMissingError",
    "PoisonMessageError",
    "ProcessWorkerPool",
    "Resources",
    "Result",
    "Status",
    "TopicPartition",
    "TransientError",
    "audio_handle_factory",
    "run",
]

__version__ = "0.1.0"


def run(function_factory, config=None, **kwargs):
    """Wire a function to the platform and block. See `faas_sdk.bootstrap`.

    Takes the function *class* (or any zero-arg factory), not an instance: the
    worker process constructs its own, so weights and clients are never pickled.
    """
    from .bootstrap import run as _run

    return _run(function_factory, config=config, **kwargs)
