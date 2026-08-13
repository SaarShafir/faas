from __future__ import annotations

import pytest

from faas_hydrator.emitter import ReferenceEmitter
from faas_hydrator.hydrator import Hydrator
from faas_hydrator.metadata import JsonSourceDecoder
from faas_hydrator.testing import FakeAudioApi, FakeFfmpeg, flac_bytes
from faas_hydrator.transcode import Transcoder
from faas_sdk.codec import JsonCodec
from faas_sdk.config import FunctionConfig
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.pool import InlineWorkerPool
from faas_sdk.runner import FunctionRunner

INPUT_TOPIC = "faas.calls.raw"
INTERNAL_TOPIC = "faas.audio.internal"


@pytest.fixture
def ffmpeg():
    return FakeFfmpeg(output=flac_bytes(total_samples=16000 * 300))


@pytest.fixture
def transcoder(ffmpeg):
    return Transcoder(run=ffmpeg)


@pytest.fixture
def hydrator(transcoder, object_store, clock):
    return Hydrator(
        transcoder=transcoder,
        object_store=object_store,
        codec=JsonCodec(),
        clock=clock,
    )


@pytest.fixture
def hydrator_config() -> FunctionConfig:
    # The hydrator is a single consumer group on the input topic (§4.1), and it
    # reuses the function declaration wholesale: `results_topic` is simply the
    # internal topic. Fewer concepts, and the DLQ and group-id conventions come
    # along for free.
    return FunctionConfig(
        function_id="hydrator",
        function_version="1.0.0",
        image="registry/faas-hydrator:1.0.0",
        input_topic=INPUT_TOPIC,
        results_topic=INTERNAL_TOPIC,
        in_flight=2,
        per_file_timeout_seconds=120,
        retry_budget=3,
        retry_backoff_seconds=1.0,
        commit_interval_seconds=0.0,
    )


@pytest.fixture
def audio_api():
    return FakeAudioApi()


@pytest.fixture
def hydrator_consumer():
    from faas_sdk.testing import FakeConsumer

    return FakeConsumer(assignment=[(INPUT_TOPIC, 0)])


@pytest.fixture
def hydrator_runner(
    hydrator, hydrator_config, hydrator_consumer, producer, audio_api, clock, metrics
):
    from faas_hydrator.audioapi import source_audio_handle_factory

    pool = InlineWorkerPool(
        function=hydrator,
        audio_handle_factory=source_audio_handle_factory(audio_api),
        max_in_flight=hydrator_config.in_flight,
        defer=True,  # hold work so the poll loop stays observable
        clock=clock,
    )
    runner = FunctionRunner(
        config=hydrator_config,
        consumer=hydrator_consumer,
        pool=pool,
        codec=JsonCodec(),
        decoder=JsonSourceDecoder().decode,
        results=ReferenceEmitter(
            topic=hydrator_config.results_topic,
            producer=producer,
            hydrator_version=hydrator.function_version,
            metrics=metrics,
        ),
        dlq=DeadLetterQueue(config=hydrator_config, producer=producer, clock=clock),
        metrics=metrics,
        clock=clock,
    )
    runner.start()
    return runner


@pytest.fixture
def consumer(hydrator_consumer):
    """Shadow the root fixture: hydrator tests read the input topic."""
    return hydrator_consumer
