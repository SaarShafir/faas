from __future__ import annotations

import pytest

from faas_sdk.codec import JsonCodec
from faas_sdk.config import FunctionConfig
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.metrics import InMemoryMetrics
from faas_sdk.models import AudioReference
from faas_sdk.results import ResultEmitter
from faas_sdk.runner import FunctionRunner
from faas_sdk.testing import (
    FakeClock,
    FakeConsumer,
    FakeObjectStore,
    FakeProducer,
    ManualPool,
    reference,
    reference_message,
)

INPUT_TOPIC = "faas.audio.internal"


@pytest.fixture
def config() -> FunctionConfig:
    return FunctionConfig(
        function_id="duration_rms",
        function_version="1.0.0",
        image="registry/faas-duration-rms:1.0.0",
        input_topic=INPUT_TOPIC,
        results_topic="faas.results",
        dlq_topic="faas.dlq.duration_rms",
        in_flight=2,
        per_file_timeout_seconds=120,
        retry_budget=3,
        retry_backoff_seconds=1.0,
        commit_interval_seconds=0.0,
    )


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def consumer() -> FakeConsumer:
    return FakeConsumer(assignment=[(INPUT_TOPIC, 0)])


@pytest.fixture
def producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture
def object_store() -> FakeObjectStore:
    return FakeObjectStore()


@pytest.fixture
def pool(config: FunctionConfig) -> ManualPool:
    return ManualPool(max_in_flight=config.in_flight)


@pytest.fixture
def metrics() -> InMemoryMetrics:
    return InMemoryMetrics()


@pytest.fixture
def runner(config, consumer, producer, object_store, pool, clock, metrics) -> FunctionRunner:
    codec = JsonCodec()
    runner = FunctionRunner(
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
    runner.start()  # registers the rebalance callbacks on the fake consumer
    return runner


__all__ = [
    "AudioReference",
    "INPUT_TOPIC",
    "reference",
    "reference_message",
]
