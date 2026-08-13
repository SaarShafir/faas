"""Failure handling (spec §5.4).

| Transient error | bounded retries with backoff, then DLQ            |
| Poison message  | straight to DLQ, commit the offset                 |
| Timeout         | per-file timeout from config, treated as a failure |

And the §5.4 note: the DLQ holds the input message for replay, while a FAILED
record on the results topic says the call was attempted and produced nothing.
Both are emitted, or downstream cannot tell "no result yet" from "no result ever".
"""

from dataclasses import replace

import pytest

from faas_sdk.models import Status
from faas_sdk.testing import reference_message


def _dlq_records(producer, config):
    return [r for r in producer.records if r.topic == config.dlq_topic]


def _results(producer, config, codec):
    return [
        codec.decode_result(r.value) for r in producer.records if r.topic == config.results_topic
    ]


def test_transient_error_is_retried_after_backoff(runner, consumer, pool, clock, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()

    pool.fail("c10", code="S3_TIMEOUT", retryable=True)
    runner.run_once()

    # Backoff has not elapsed: nothing resubmitted, nothing committed.
    assert pool.in_flight() == 0
    assert consumer.commits == []

    clock.advance(config.retry_backoff_seconds)
    runner.run_once()

    assert pool.in_flight() == 1
    assert pool.job_for("c10").attempt == 2


def test_retries_are_bounded_then_the_message_goes_to_the_dlq(
    runner, consumer, pool, clock, config, producer
):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()

    for _ in range(config.retry_budget):
        pool.fail("c10", code="S3_TIMEOUT", retryable=True)
        runner.run_once()
        clock.advance(3600)
        runner.run_once()

    dlq = _dlq_records(producer, config)
    assert len(dlq) == 1
    assert dlq[0].headers["faas.error.code"] == b"S3_TIMEOUT"
    assert dlq[0].headers["faas.attempt"] == str(config.retry_budget).encode()


def test_backoff_is_exponential(runner, consumer, pool, clock, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()

    pool.fail("c10", code="S3_TIMEOUT", retryable=True)
    runner.run_once()
    clock.advance(config.retry_backoff_seconds)
    runner.run_once()
    assert pool.in_flight() == 1

    pool.fail("c10", code="S3_TIMEOUT", retryable=True)
    runner.run_once()
    clock.advance(config.retry_backoff_seconds)
    runner.run_once()
    # Second retry waits 2x the base backoff.
    assert pool.in_flight() == 0
    clock.advance(config.retry_backoff_seconds)
    runner.run_once()
    assert pool.in_flight() == 1


def test_non_retryable_error_goes_straight_to_the_dlq(runner, consumer, pool, producer, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()

    pool.fail("c10", code="DECODE_ERROR", retryable=False, message="not FLAC")
    runner.run_once()

    assert len(_dlq_records(producer, config)) == 1
    assert pool.in_flight() == 0


def test_dlq_carries_the_original_message_for_replay(runner, consumer, pool, producer, config):
    message = reference_message(partition=0, offset=10, call_id="c10")
    consumer.feed(message)
    runner.run_once()
    pool.fail("c10", code="DECODE_ERROR", retryable=False)
    runner.run_once()

    record = _dlq_records(producer, config)[0]
    assert record.value == message.value
    assert record.key == message.key
    assert record.headers["faas.source.topic"] == config.input_topic.encode()
    assert record.headers["faas.source.partition"] == b"0"
    assert record.headers["faas.source.offset"] == b"10"
    assert record.headers["faas.function_id"] == config.function_id.encode()


def test_dlq_commits_the_offset_so_poison_cannot_accrue_lag(runner, consumer, pool, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    pool.fail("c10", code="DECODE_ERROR", retryable=False)
    runner.run_once()

    assert consumer.commits == [[((consumer.topic, 0), 11)]]


def test_failure_also_emits_a_failed_result(runner, consumer, pool, producer, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    pool.fail("c10", code="DECODE_ERROR", retryable=False, message="not FLAC")
    runner.run_once()

    results = _results(producer, config, runner.codec)
    assert len(results) == 1
    result = results[0]
    assert result.status is Status.FAILED
    assert result.error is not None
    assert result.error.code == "DECODE_ERROR"
    assert result.error.retryable is False
    assert result.call_id == "c10"
    assert result.input_offset == 10


def test_an_undecodable_message_never_reaches_the_pool(runner, consumer, pool, producer, config):
    consumer.feed(reference_message(partition=0, offset=10, raw_value=b"{not json"))
    runner.run_once()

    assert pool.in_flight() == 0
    assert len(_dlq_records(producer, config)) == 1
    assert consumer.commits == [[((consumer.topic, 0), 11)]]


def test_an_undecodable_message_still_emits_a_failed_result(runner, consumer, producer, config):
    """The body did not parse, but the record key is the call_id (§4.2)."""
    consumer.feed(
        reference_message(partition=0, offset=10, call_id="c10", raw_value=b"{not json")
    )
    runner.run_once()

    results = _results(producer, config, runner.codec)
    assert len(results) == 1
    assert results[0].status is Status.FAILED
    assert results[0].call_id == "c10"
    assert results[0].error.code == "DECODE_ERROR"


def test_an_undecodable_message_with_no_key_emits_dlq_only(runner, consumer, producer, config):
    """Nothing to key a Result on -- the DLQ record is all we can honestly emit."""
    message = reference_message(partition=0, offset=10, raw_value=b"{not json")
    consumer.feed(replace(message, key=None))
    runner.run_once()

    assert len(_dlq_records(producer, config)) == 1
    assert _results(producer, config, runner.codec) == []


def test_per_file_timeout_is_treated_as_a_failure(runner, consumer, pool, clock, config, producer):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()

    clock.advance(config.per_file_timeout_seconds + 1)
    runner.run_once()

    assert pool.cancelled == ["c10"]
    clock.advance(config.retry_backoff_seconds)
    runner.run_once()
    assert pool.job_for("c10").attempt == 2


def test_timeout_exhausting_the_budget_lands_in_the_dlq(
    runner, consumer, pool, clock, config, producer
):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()

    for _ in range(config.retry_budget):
        clock.advance(config.per_file_timeout_seconds + 1)
        runner.run_once()
        clock.advance(3600)
        runner.run_once()

    dlq = _dlq_records(producer, config)
    assert len(dlq) == 1
    assert dlq[0].headers["faas.error.code"] == b"TIMEOUT"


def test_a_failing_file_does_not_block_its_neighbours(runner, consumer, pool, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    consumer.feed(reference_message(partition=0, offset=11, call_id="c11"))
    runner.run_once()
    runner.run_once()

    pool.succeed("c11")
    runner.run_once()
    assert consumer.commits == []  # 10 still in flight

    pool.fail("c10", code="DECODE_ERROR", retryable=False)
    runner.run_once()
    assert consumer.commits == [[((consumer.topic, 0), 12)]]


@pytest.mark.parametrize("code", ["S3_TIMEOUT", "AUDIO_API_5XX"])
def test_retry_count_is_reported_as_a_metric(runner, consumer, pool, clock, metrics, code, config):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    pool.fail("c10", code=code, retryable=True)
    runner.run_once()
    clock.advance(config.retry_backoff_seconds)
    runner.run_once()

    assert metrics.counter_value("faas.retries") == 1
