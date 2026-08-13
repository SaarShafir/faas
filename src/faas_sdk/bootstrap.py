"""Wiring: declaration + function object -> a running consumer.

This is the whole "adding a function requires no infrastructure work" claim
(spec §1, §8) made concrete. A function image's entrypoint is one call.
"""

from __future__ import annotations

import logging
import os
import signal

from .clock import SystemClock
from .config import FunctionConfig
from .dlq import DeadLetterQueue
from .function import validate
from .metrics import NullMetrics
from .pool import ProcessWorkerPool
from .results import ResultEmitter
from .runner import FunctionRunner

log = logging.getLogger(__name__)

DECLARATION_PATH = os.environ.get("FAAS_DECLARATION", "function.yaml")


def build_runner(
    function_factory,
    config=None,
    *,
    consumer=None,
    producer=None,
    object_store=None,
    audio_handle_factory=None,
    pool=None,
    codec=None,
    metrics=None,
    clock=None,
    bootstrap_servers=None,
) -> FunctionRunner:
    """Wire a function to the platform.

    `function_factory` is a zero-arg callable -- usually just the class -- not
    an instance. It is invoked once per worker process, which is what lets model
    weights, GPU contexts and clients be built there instead of pickled across.
    """
    if not callable(function_factory):
        raise TypeError(
            f"expected a factory (usually the class itself), got an instance of "
            f"{type(function_factory).__name__}. Pass `run(MyFunction)`, not "
            f"`run(MyFunction())` -- the worker process constructs its own."
        )
    # Build one here purely to fail at boot rather than on the first message.
    validate(function_factory())
    config = config or FunctionConfig.from_yaml(DECLARATION_PATH)

    if codec is None:
        # Protobuf is the wire format on the real topics (§11). Imported lazily
        # so a source checkout without generated code can still import the SDK;
        # pass JsonCodec() explicitly for local dev against a plain-text topic.
        from .codec_protobuf import ProtobufCodec

        codec = ProtobufCodec()
    clock = clock or SystemClock()
    metrics = metrics or NullMetrics()
    bootstrap_servers = bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")

    if object_store is None:
        from .objectstore import S3ObjectStore

        object_store = S3ObjectStore(bucket=os.environ["FAAS_AUDIO_BUCKET"])

    if consumer is None:
        from .kafka import ConfluentConsumer

        consumer = ConfluentConsumer(config.consumer_config(bootstrap_servers))

    if producer is None:
        from .kafka import ConfluentProducer

        producer = ConfluentProducer(
            config.producer_config(bootstrap_servers),
            num_partitions_by_topic={config.results_topic: config.results_topic_partitions},
        )

    if pool is None:
        # A process pool, always -- never InlineWorkerPool here.
        #
        # Inline runs process() synchronously inside submit(), so the work
        # happens on the poll thread and the loop stops polling for its
        # duration. Against a real broker that is an eviction and a reprocessed
        # file (tests/kafka/test_poll_interval.py proves it), which is precisely
        # the §5.2 failure this SDK exists to prevent. Decode plus inference is
        # CPU-bound and GIL-blocked, so threads would not help either.
        #
        # §5.2's "one consumer per pod with small in-flight depth" describes the
        # deployment shape, not where the work runs: keep in_flight small and
        # let the autoscaler scale pods, but keep the work off the poll thread.
        if audio_handle_factory is None:
            from .objectstore import s3_audio_handle_factory

            audio_handle_factory = s3_audio_handle_factory

        pool = ProcessWorkerPool(
            function_factory=function_factory,
            audio_handle_factory=audio_handle_factory,
            max_in_flight=config.in_flight,
        )

    return FunctionRunner(
        config=config,
        consumer=consumer,
        pool=pool,
        codec=codec,
        results=ResultEmitter(
            config=config,
            producer=producer,
            object_store=object_store,
            codec=codec,
            clock=clock,
        ),
        dlq=DeadLetterQueue(config=config, producer=producer, clock=clock),
        metrics=metrics,
        clock=clock,
    )


def run(function_factory, config=None, **kwargs) -> None:
    logging.basicConfig(level=os.environ.get("FAAS_LOG_LEVEL", "INFO"))
    runner = build_runner(function_factory, config=config, **kwargs)

    def _shutdown(signum, frame):
        # SIGTERM is the normal case on OpenShift: stop polling, let in-flight
        # work finish and commit, then exit. Anything unfinished is reprocessed.
        log.info("signal %s: draining", signum)
        runner.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("starting %s in group %s", runner.config.function_id, runner.config.group_id)
    runner.run()
