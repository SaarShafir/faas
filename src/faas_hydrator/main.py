"""Hydrator entrypoint.

Stateless, few replicas -- at 17 files/sec it will never be the bottleneck
(§4.1). Scale it on lag like everything else, but expect the number to stay
small; if it does not, the Audio API or the object store is the constraint, not
this process.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal

from faas_sdk.clock import SystemClock
from faas_sdk.config import FunctionConfig
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.metrics import from_env as metrics_from_env
from faas_sdk.pool import InlineWorkerPool
from faas_sdk.runner import FunctionRunner

from .audioapi import AudioApiClient, source_audio_handle_factory
from .emitter import ReferenceEmitter
from .hydrator import Hydrator
from .metadata import JsonSourceDecoder
from .transcode import Transcoder

log = logging.getLogger(__name__)

DECLARATION_PATH = os.environ.get("FAAS_DECLARATION", "hydrator.yaml")


def build_runner(
    config=None,
    *,
    consumer=None,
    producer=None,
    object_store=None,
    audio_client=None,
    transcoder=None,
    codec=None,
    metrics=None,
    clock=None,
    bootstrap_servers=None,
) -> FunctionRunner:
    config = config or FunctionConfig.from_yaml(DECLARATION_PATH)
    clock = clock or SystemClock()
    # The hydrator is a consumer group like any other (§8) and belongs on the
    # same dashboards -- its lag is the first thing to move when the Audio API
    # or the object store is the constraint.
    metrics = metrics or metrics_from_env(
        **{
            "service.name": config.function_id,
            "service.version": config.function_version,
        }
    )
    bootstrap_servers = bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")

    if codec is None:
        from faas_sdk.codec_protobuf import ProtobufCodec

        codec = ProtobufCodec()

    if transcoder is None:
        # Fail at boot, not per message: a missing binary would otherwise DLQ
        # every record identically until someone noticed.
        ffmpeg = os.environ.get("FAAS_FFMPEG", "ffmpeg")
        if shutil.which(ffmpeg) is None:
            raise RuntimeError(f"ffmpeg not found on PATH as {ffmpeg!r}; the image is broken")
        transcoder = Transcoder(
            ffmpeg=ffmpeg,
            compression_level=int(os.environ.get("FAAS_FLAC_COMPRESSION_LEVEL", "0")),
            timeout_seconds=config.per_file_timeout_seconds,
        )

    if object_store is None:
        from faas_sdk.objectstore import S3ObjectStore

        object_store = S3ObjectStore(bucket=os.environ["FAAS_AUDIO_BUCKET"])

    if audio_client is None:
        audio_client = AudioApiClient(base_url=os.environ["FAAS_AUDIO_API_URL"])

    if consumer is None:
        from faas_sdk.kafka import ConfluentConsumer

        consumer = ConfluentConsumer(config.consumer_config(bootstrap_servers))

    if producer is None:
        from faas_sdk.kafka import ConfluentProducer

        producer = ConfluentProducer(
            config.producer_config(bootstrap_servers),
            num_partitions_by_topic={config.results_topic: config.results_topic_partitions},
        )

    hydrator = Hydrator(
        transcoder=transcoder, object_store=object_store, codec=codec, clock=clock
    )

    # An inline pool is safe *here*, unlike for a function (see
    # faas_sdk.bootstrap), and the difference is worth being precise about.
    #
    # Inline work blocks the poll loop, so it is only safe when the block is
    # bounded below max.poll.interval.ms. `consumer_config` sets that interval
    # to per_file_timeout x in_flight + 60s, and the hydrator's work really is
    # bounded by per_file_timeout: ffmpeg runs under a subprocess timeout the
    # transcoder enforces itself. So the worst case is 60s inside the limit.
    #
    # A function gets no such guarantee. Its per-file timeout is enforced by the
    # runner between poll iterations, which cannot run while inline work is
    # blocking -- so for a function the timeout is exactly the thing that stops
    # working, and the pool must be a process pool.
    return FunctionRunner(
        config=config,
        consumer=consumer,
        pool=InlineWorkerPool(
            function=hydrator,
            audio_handle_factory=source_audio_handle_factory(audio_client),
            max_in_flight=config.in_flight,
            clock=clock,
        ),
        codec=codec,
        decoder=JsonSourceDecoder(
            call_id_field=os.environ.get("FAAS_CALL_ID_FIELD", "call_id"),
            audio_id_field=os.environ.get("FAAS_AUDIO_ID_FIELD", "audio_id"),
        ).decode,
        results=ReferenceEmitter(
            topic=config.results_topic,
            producer=producer,
            hydrator_version=hydrator.function_version,
            metrics=metrics,
        ),
        dlq=DeadLetterQueue(config=config, producer=producer, clock=clock),
        metrics=metrics,
        clock=clock,
    )


def main() -> None:
    logging.basicConfig(level=os.environ.get("FAAS_LOG_LEVEL", "INFO"))
    runner = build_runner()

    def _shutdown(signum, frame):
        log.info("signal %s: draining", signum)
        runner.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("hydrating %s -> %s", runner.config.input_topic, runner.config.results_topic)
    runner.run()


if __name__ == "__main__":
    main()
