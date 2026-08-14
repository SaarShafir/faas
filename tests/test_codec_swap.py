"""The codec seam, exercised through the whole runner.

This is the test that justifies building the seam at all: the same runner, the
same ledger, the same DLQ routing, driven over JSON and over protobuf, with
nothing but the codec swapped. If putting the real wire format on the topics
required touching the runner, the seam did not work.
"""

import pytest

from faas_sdk.codec import JsonCodec
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.models import Status
from faas_sdk.results import ResultEmitter
from faas_sdk.runner import FunctionRunner
from faas_sdk.testing import reference_message

pytest.importorskip("google.protobuf", reason="protobuf runtime not installed")

from faas_sdk.codec_protobuf import ProtobufCodec  # noqa: E402


@pytest.fixture(params=[JsonCodec, ProtobufCodec], ids=["json", "protobuf"])
def codec(request):
    return request.param()


@pytest.fixture
def swapped_runner(codec, config, consumer, producer, object_store, pool, clock, metrics):
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
    runner.start()
    return runner


def test_a_file_goes_through_end_to_end(swapped_runner, consumer, pool, producer, codec, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10", codec=codec))

    swapped_runner.run_once()
    pool.succeed("c10", payload=b'{"rms": 0.12}', schema_version="1")
    swapped_runner.run_once()

    (record,) = [r for r in producer.records if r.topic == config.results_topic]
    result = codec.decode_result(record.value)

    assert result.status is Status.SUCCESS
    assert result.call_id == "c10"
    assert result.payload == b'{"rms": 0.12}'
    assert result.input_offset == 10
    assert record.key == b"c10:duration_rms:1.0.0"
    assert record.partition_key == b"c10"
    assert consumer.commits == [[((consumer.topic, 0), 11)]]


def test_a_malformed_body_is_poison_on_either_codec(
    swapped_runner, consumer, pool, producer, config
):
    """Whatever the wire format, unparseable bytes must reach the DLQ on the
    first attempt rather than burning the retry budget."""
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10", raw_value=b"\xff\xff!"))

    swapped_runner.run_once()

    assert pool.in_flight() == 0
    assert len([r for r in producer.records if r.topic == config.dlq_topic]) == 1
    assert consumer.commits == [[((consumer.topic, 0), 11)]]


def test_neither_codec_will_emit_an_unspecified_status(codec):
    """Only protobuf needs a reserved zero value, but a Result that is invalid
    on the wire must be invalid on both sides of the seam -- otherwise local
    dev on JSON produces envelopes that production would reject."""
    from faas_sdk.models import Result

    result = Result(
        call_id="c10",
        function_id="duration_rms",
        function_version="1.0.0",
        status=Status.UNSPECIFIED,
        input_object_key="c10.flac",
        input_offset=10,
        attempt=1,
    )
    with pytest.raises(ValueError, match="UNSPECIFIED"):
        codec.encode_result(result)


def test_a_failure_emits_both_records_on_either_codec(
    swapped_runner, consumer, pool, producer, codec, config
):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10", codec=codec))
    swapped_runner.run_once()
    pool.fail("c10", code="DECODE_ERROR", retryable=False, message="not FLAC")
    swapped_runner.run_once()

    assert len([r for r in producer.records if r.topic == config.dlq_topic]) == 1
    (record,) = [r for r in producer.records if r.topic == config.results_topic]
    result = codec.decode_result(record.value)
    assert result.status is Status.FAILED
    assert result.error.code == "DECODE_ERROR"
    assert result.error.retryable is False
