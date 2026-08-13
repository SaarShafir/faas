"""Results envelope (spec §6).

Three rules that are cheap now and expensive later:
  - key is {call_id}:{function_id}:{function_version}, never call_id alone;
  - partitioning is on call_id alone, so the aggregator needs no shuffle;
  - payloads >= 256 KB are claim-checked to S3, transparently to the author.
"""

from faas_sdk.models import Status
from faas_sdk.results import INLINE_PAYLOAD_LIMIT_BYTES
from faas_sdk.testing import reference_message


def _results(producer, config):
    return [r for r in producer.records if r.topic == config.results_topic]


def test_success_emits_a_success_envelope(runner, consumer, pool, producer, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    pool.succeed(
        "c10", payload=b'{"rms": 0.12}', schema_version="1", content_type="application/json"
    )
    runner.run_once()

    records = _results(producer, config)
    assert len(records) == 1
    result = runner.codec.decode_result(records[0].value)

    assert result.status is Status.SUCCESS
    assert result.call_id == "c10"
    assert result.function_id == config.function_id
    assert result.function_version == config.function_version
    assert result.payload == b'{"rms": 0.12}'
    assert result.payload_ref is None
    assert result.payload_schema_version == "1"
    assert result.payload_content_type == "application/json"
    assert result.error is None


def test_envelope_carries_provenance(runner, consumer, pool, producer, config):
    consumer.feed(reference_message(partition=0, offset=42, call_id="c42"))
    runner.run_once()
    pool.succeed("c42", payload=b"{}")
    runner.run_once()

    result = runner.codec.decode_result(_results(producer, config)[0].value)
    assert result.input_object_key == "c42.flac"
    assert result.input_offset == 42
    assert result.attempt == 1
    assert result.envelope_version == 1
    assert result.ingested_at is not None
    assert result.started_at is not None
    assert result.completed_at is not None


def test_key_is_composite(runner, consumer, pool, producer, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    pool.succeed("c10", payload=b"{}")
    runner.run_once()

    record = _results(producer, config)[0]
    assert record.key == b"c10:duration_rms:1.0.0"


def test_partitioning_is_on_call_id_alone(runner, consumer, pool, producer, config):
    """Two versions of the same function must land on the same partition."""
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    pool.succeed("c10", payload=b"{}")
    runner.run_once()

    record = _results(producer, config)[0]
    assert record.partition_key == b"c10"
    assert record.partition_key != record.key


def test_small_payloads_are_inlined(runner, consumer, pool, producer, object_store, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    pool.succeed("c10", payload=b"x" * (INLINE_PAYLOAD_LIMIT_BYTES - 1))
    runner.run_once()

    result = runner.codec.decode_result(_results(producer, config)[0].value)
    assert result.payload is not None
    assert result.payload_ref is None
    assert object_store.objects == {}


def test_large_payloads_are_claim_checked_to_the_object_store(
    runner, consumer, pool, producer, object_store, config
):
    big = b"x" * INLINE_PAYLOAD_LIMIT_BYTES
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    pool.succeed("c10", payload=big)
    runner.run_once()

    result = runner.codec.decode_result(_results(producer, config)[0].value)
    assert result.payload is None
    assert result.payload_ref is not None
    # Namespaced by function and version -- shadow deploys must not collide.
    assert result.payload_ref == "results/duration_rms/1.0.0/c10"
    assert object_store.objects[result.payload_ref] == big


def test_failed_results_carry_no_payload(runner, consumer, pool, producer, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    pool.fail("c10", code="DECODE_ERROR", retryable=False, message="not FLAC")
    runner.run_once()

    result = runner.codec.decode_result(_results(producer, config)[0].value)
    assert result.status is Status.FAILED
    assert result.payload is None
    assert result.payload_ref is None
    assert result.error.message == "not FLAC"
