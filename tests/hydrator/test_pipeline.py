"""The hydrator running on the SDK's runner.

The spec says to build the SDK before the hydrator (§5) because poll/work
decoupling and commit correctness are miserable to change later. The hydrator
has exactly the same problem -- a file fetch and a store round-trip are seconds
of work between polls -- so it runs on the same runner rather than growing a
second copy of the hardest code in the system.

What differs is only the two ends: the input is source metadata instead of an
AudioReference, and the output is a reference on the internal topic instead of
a Result on the results topic.
"""

import json

from faas_sdk.codec import JsonCodec
from faas_sdk.models import InboundMessage

INPUT_TOPIC = "faas.calls.raw"
INTERNAL_TOPIC = "faas.audio.internal"


def _message(offset=0, call_id="c1", partition=0, raw=None):
    value = (
        raw
        if raw is not None
        else json.dumps({"call_id": call_id, "audio_id": f"audio-{call_id}"}).encode()
    )
    return InboundMessage(
        topic=INPUT_TOPIC,
        partition=partition,
        offset=offset,
        value=value,
        key=call_id.encode(),
    )


def _references(producer):
    return [r for r in producer.records if r.topic == INTERNAL_TOPIC]


def test_a_call_is_hydrated_end_to_end(hydrator_runner, consumer, producer, object_store):
    consumer.feed(_message(offset=10, call_id="c1"))

    hydrator_runner.run_once()  # dispatch
    hydrator_runner.pool.run_pending()  # fetch + store
    hydrator_runner.run_once()  # publish + commit

    (record,) = _references(producer)
    ref = JsonCodec().decode_reference(record.value)

    assert ref.call_id == "c1"
    assert ref.object_key == "c1.flac"
    assert object_store.objects["c1.flac"].startswith(b"fLaC")
    assert consumer.commits == [[((INPUT_TOPIC, 0), 11)]]


def test_the_reference_is_keyed_and_partitioned_on_call_id(hydrator_runner, consumer, producer):
    """Spec §4.2: key is call_id, so a call's audio always lands on one
    partition and reprocessing overwrites rather than duplicates."""
    consumer.feed(_message(offset=10, call_id="c1"))
    hydrator_runner.run_once()
    hydrator_runner.pool.run_pending()
    hydrator_runner.run_once()

    (record,) = _references(producer)
    assert record.key == b"c1"
    assert record.partition_key == b"c1"


def test_the_offset_is_not_committed_before_the_reference_is_published(
    hydrator_runner, consumer, producer
):
    """Committing first would drop the call entirely on a crash: the input is
    gone from our offset's point of view and no reference ever reaches the
    internal topic."""
    consumer.feed(_message(offset=10, call_id="c1"))
    hydrator_runner.run_once()

    assert _references(producer) == []
    assert consumer.commits == []


def test_unparseable_metadata_goes_straight_to_the_dlq(
    hydrator_runner, consumer, producer, hydrator_config
):
    consumer.feed(_message(offset=10, raw=b"{not json"))
    hydrator_runner.run_once()

    dlq = [r for r in producer.records if r.topic == hydrator_config.dlq_topic]
    assert len(dlq) == 1
    assert _references(producer) == []
    # Committed, so one bad record cannot accrue lag for the whole pipeline.
    assert consumer.commits == [[((INPUT_TOPIC, 0), 11)]]


def test_audio_that_is_not_flac_reaches_the_dlq_without_retrying(
    hydrator_runner, consumer, producer, audio_api, hydrator_config
):
    """An upstream error page served as audio is the likeliest real incident,
    and it will not become FLAC on the third attempt."""
    audio_api.default = b"<!doctype html>\n<h1>502 Bad Gateway</h1>"

    consumer.feed(_message(offset=10, call_id="c1"))
    hydrator_runner.run_once()
    hydrator_runner.pool.run_pending()
    hydrator_runner.run_once()

    dlq = [r for r in producer.records if r.topic == hydrator_config.dlq_topic]
    assert len(dlq) == 1
    assert dlq[0].headers["faas.attempt"] == b"1"
    assert _references(producer) == []
    assert consumer.commits == [[((INPUT_TOPIC, 0), 11)]]


def test_no_reference_is_published_for_a_failed_call(
    hydrator_runner, consumer, producer, audio_api, object_store
):
    """Which is the gap worth knowing about: a call that fails hydration is
    invisible downstream. The DLQ holds it for replay, but nothing on any topic
    says it was ever meant to exist."""
    audio_api.default = b"not audio at all"

    consumer.feed(_message(offset=10, call_id="c1"))
    hydrator_runner.run_once()
    hydrator_runner.pool.run_pending()
    hydrator_runner.run_once()

    assert _references(producer) == []
    assert object_store.objects == {}


def test_a_failed_store_publishes_no_reference(
    hydrator_runner, consumer, producer, object_store, hydrator_config
):
    """The other half of the ordering invariant: if the object never lands,
    nothing may claim it did. A reference pointing at a key that was never
    written is a dead key for every function downstream."""
    from faas_sdk.errors import TransientError

    def explode(key, body, content_type=""):
        raise TransientError("s3 is having a day", code="S3_UNAVAILABLE")

    object_store.put = explode

    consumer.feed(_message(offset=10, call_id="c1"))
    hydrator_runner.run_once()
    hydrator_runner.pool.run_pending()
    hydrator_runner.run_once()

    assert _references(producer) == []
    # Retryable, so it is held for another attempt rather than dropped.
    assert consumer.commits == []
    assert [r for r in producer.records if r.topic == hydrator_config.dlq_topic] == []


def test_a_store_that_keeps_failing_eventually_reaches_the_dlq(
    hydrator_runner, consumer, producer, object_store, clock, hydrator_config
):
    from faas_sdk.errors import TransientError

    def explode(key, body, content_type=""):
        raise TransientError("s3 is having a day", code="S3_UNAVAILABLE")

    object_store.put = explode
    consumer.feed(_message(offset=10, call_id="c1"))
    hydrator_runner.run_once()

    for _ in range(hydrator_config.retry_budget):
        hydrator_runner.pool.run_pending()
        hydrator_runner.run_once()
        clock.advance(3600)
        hydrator_runner.run_once()

    dlq = [r for r in producer.records if r.topic == hydrator_config.dlq_topic]
    assert len(dlq) == 1
    assert dlq[0].headers["faas.error.code"] == b"S3_UNAVAILABLE"
    assert _references(producer) == []


def test_the_poll_loop_keeps_running_while_a_call_is_in_flight(hydrator_runner, consumer):
    """Same failure mode as §5.2: fetching a 5 MB file and storing it again is
    long enough that a blocking loop would blow past max.poll.interval.ms under
    load."""
    consumer.feed(_message(offset=10, call_id="c1"))
    hydrator_runner.run_once()

    for _ in range(4):
        hydrator_runner.run_once()

    assert consumer.poll_count == 5
    assert hydrator_runner.in_flight == 1


def test_several_calls_hydrate_independently(hydrator_runner, consumer, producer, object_store):
    for offset, call_id in enumerate(["c1", "c2"], start=10):
        consumer.feed(_message(offset=offset, call_id=call_id))

    for _ in range(2):
        hydrator_runner.run_once()
    hydrator_runner.pool.run_pending()
    hydrator_runner.run_once()

    assert {r.key for r in _references(producer)} == {b"c1", b"c2"}
    assert set(object_store.objects) == {"c1.flac", "c2.flac"}
    assert consumer.commits[-1] == [((INPUT_TOPIC, 0), 12)]
