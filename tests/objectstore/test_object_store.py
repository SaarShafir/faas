"""Real S3 behaviour, against a real MinIO server.

FakeObjectStore covers the SDK's *interface*; it cannot show what the wire
protocol actually does. These tests close that gap for the paths that matter:

  - the round trip the whole claim-check pattern rests on: hydrator PUTs
    canonical FLAC, the function GETs and decodes it, and the measurements
    survive (the contract test's S3 is a fake, so this is the real one);
  - a real `NoSuchKey` is an `ObjectMissingError`, not a generic transient --
    that distinction is what routes a dead key to the §5.4 re-fetch path;
  - the 256 KB claim check really moves the payload to the store;
  - a key that outlived its 24h TTL (deleted here to simulate it) is re-fetched
    from the Audio API exactly once.

The audio pipeline parts are the same ones the contract test uses: real
ffmpeg, real libsndfile. Only the store is different.
"""

from __future__ import annotations

import json
import math
import shutil
import subprocess

import pytest

from faas_hydrator.audioapi import source_audio_handle_factory
from faas_hydrator.emitter import ReferenceEmitter
from faas_hydrator.hydrator import Hydrator
from faas_hydrator.metadata import JsonSourceDecoder
from faas_hydrator.testing import FakeAudioApi, flac_bytes
from faas_hydrator.transcode import Transcoder
from faas_sdk.audio import AudioHandle, audio_handle_factory
from faas_sdk.codec import JsonCodec
from faas_sdk.config import FunctionConfig
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.errors import ObjectMissingError, TransientError
from faas_sdk.models import FunctionResult, InboundMessage, JobOutcome, Status
from faas_sdk.objectstore import AudioApiFallback
from faas_sdk.pool import InlineWorkerPool
from faas_sdk.results import INLINE_PAYLOAD_LIMIT_BYTES, ResultEmitter
from faas_sdk.runner import FunctionRunner
from faas_sdk.testing import FakeConsumer, FakeProducer, job, reference

pytestmark = pytest.mark.minio

INPUT_TOPIC = "faas.calls.raw"
INTERNAL_TOPIC = "faas.audio.internal"
RESULTS_TOPIC = "faas.results"

HAS_FFMPEG = shutil.which("ffmpeg") is not None


def test_a_put_round_trips_bytes_and_content_type(store, s3_client, bucket):
    key = "roundtrip.bin"
    store.put(key, b"audio bytes", content_type="audio/flac")

    assert store.get(key) == b"audio bytes"
    head = s3_client.head_object(Bucket=bucket, Key=key)
    assert head["ContentType"] == "audio/flac"


def test_a_missing_key_is_object_missing_not_transient(store):
    """The taxonomy matters: ObjectMissingError is retryable but routed to the
    §5.4 re-fetch path, while a generic transient would burn retries against a
    store that cannot conjure the object. A real NoSuchKey must map cleanly."""
    with pytest.raises(ObjectMissingError):
        store.get("never-written.flac")

    # And a genuinely broken request is still a transient, not a poison.
    with pytest.raises(TransientError):
        store.get("")

    with pytest.raises(TransientError):
        store.put("", b"body")


def test_a_large_payload_is_claimed_checked_to_the_store(store):
    """§6: over 256 KB goes to S3 and the result carries the ref. The fake
    store could not show the wire round trip; this one can."""
    codec = JsonCodec()
    config = FunctionConfig(
        function_id="duration_rms",
        function_version="1.0.0",
        image="img",
        results_topic=RESULTS_TOPIC,
    )
    producer = FakeProducer()
    emitter = ResultEmitter(config=config, producer=producer, codec=codec, object_store=store)

    outcome = JobOutcome(
        job=job(call_id="big"),
        status=Status.SUCCESS,
        payload=b"x" * (INLINE_PAYLOAD_LIMIT_BYTES + 1),
        schema_version="1",
        content_type="application/json",
    )
    result = emitter.emit(outcome)

    assert result.payload is None
    assert result.payload_ref == "results/duration_rms/1.0.0/big"
    assert store.get(result.payload_ref) == outcome.payload

    # The inline side still fits in the envelope.
    small = JobOutcome(
        job=job(call_id="small"),
        status=Status.SUCCESS,
        payload=b"tiny",
        schema_version="1",
        content_type="application/json",
    )
    assert emitter.emit(small).payload == b"tiny"


def test_a_dead_key_is_refetched_from_the_audio_api_exactly_once(store, s3_client):
    """§5.4: a lagging consumer meets a live offset with a dead object. Deleting
    the object simulates the 24h TTL expiry. The handle must fall back to the
    Audio API, rate-limited separately from live hydration -- and only once,
    because the result is cached for the lifetime of the call."""
    key = "dead.flac"
    store.put(key, flac_bytes())

    ref = reference(call_id="dead", object_key=key)
    fallback = AudioApiFallback(
        FakeAudioApi(audio={"dead": flac_bytes()})
    )
    handle = AudioHandle(ref, object_store=store, fallback=fallback)

    s3_client.delete_object(Bucket=store.bucket, Key=key)

    first = handle.bytes()
    second = handle.bytes()

    assert first == second == flac_bytes()
    assert fallback.client.requested == ["dead"]

    # The re-fetch is what a function would decode; it must be audio-shaped.
    assert first.startswith(b"fLaC")


def test_a_missing_key_without_a_fallback_stays_missing(store):
    handle = AudioHandle(reference(call_id="gone"), object_store=store)
    with pytest.raises(ObjectMissingError):
        handle.bytes()


@pytest.mark.skipif(not HAS_FFMPEG, reason="the store contract needs real ffmpeg")
def test_a_call_survives_hydrator_real_store_and_function(store, s3_client):
    """The contract test's S3 is a fake; this is the same call with a real one.
    Real ffmpeg transcodes, the hydrator PUTs to MinIO, the function GETs from
    MinIO, libsndfile decodes, and the measurements match what went in."""

    def wav(seconds=2, rate=44100, channels=2):
        return subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate={rate}",
                "-ac", str(channels),
                "-t", str(seconds),
                "-f", "wav", "pipe:1",
            ],
            capture_output=True,
            check=True,
        ).stdout

    source = wav()
    import io

    import soundfile

    samples, rate = soundfile.read(io.BytesIO(source), dtype="float32")
    expected_rms = float(math.sqrt((samples.astype("float64") ** 2).mean()))
    expected_duration = len(samples) / rate

    audio_api = FakeAudioApi(default=source)
    producer = FakeProducer()
    codec = JsonCodec()
    hydrator_config = FunctionConfig(
        function_id="hydrator",
        function_version="1.0.0",
        image="img",
        input_topic=INPUT_TOPIC,
        results_topic=INTERNAL_TOPIC,
        in_flight=2,
        commit_interval_seconds=0.0,
    )
    function_config = FunctionConfig(
        function_id="duration_rms",
        function_version="1.0.0",
        image="img",
        input_topic=INTERNAL_TOPIC,
        results_topic=RESULTS_TOPIC,
        in_flight=2,
        commit_interval_seconds=0.0,
    )

    hydrator_consumer = FakeConsumer(assignment=[(INPUT_TOPIC, 0)])
    function_consumer = FakeConsumer(assignment=[(INTERNAL_TOPIC, 0)])

    hydrator = Hydrator(
        transcoder=Transcoder(),
        object_store=store,
        codec=codec,
    )
    hydrator_runner = FunctionRunner(
        config=hydrator_config,
        consumer=hydrator_consumer,
        pool=InlineWorkerPool(
            function=hydrator,
            audio_handle_factory=source_audio_handle_factory(audio_api),
            max_in_flight=2,
        ),
        codec=codec,
        decoder=JsonSourceDecoder().decode,
        results=ReferenceEmitter(topic=INTERNAL_TOPIC, producer=producer),
        dlq=DeadLetterQueue(config=hydrator_config, producer=producer),
    )
    function_runner = FunctionRunner(
        config=function_config,
        consumer=function_consumer,
        pool=InlineWorkerPool(
            function=_DurationRms(),
            audio_handle_factory=audio_handle_factory(store),
            max_in_flight=2,
        ),
        codec=codec,
        results=ResultEmitter(
            config=function_config,
            producer=producer,
            object_store=store,
            codec=codec,
        ),
        dlq=DeadLetterQueue(config=function_config, producer=producer),
    )
    hydrator_runner.start()
    function_runner.start()

    hydrator_consumer.feed(
        InboundMessage(
            topic=INPUT_TOPIC,
            partition=0,
            offset=10,
            key=b"c1",
            value=json.dumps({"call_id": "c1", "audio_id": "audio-c1"}).encode(),
        )
    )
    hydrator_runner.run_once()
    hydrator_runner.run_once()

    (reference_record,) = [r for r in producer.records if r.topic == INTERNAL_TOPIC]
    function_consumer.feed(
        InboundMessage(
            topic=INTERNAL_TOPIC,
            partition=0,
            offset=100,
            key=reference_record.key,
            value=reference_record.value,
        )
    )
    function_runner.run_once()
    function_runner.run_once()

    (result_record,) = [r for r in producer.records if r.topic == RESULTS_TOPIC]
    result = codec.decode_result(result_record.value)
    payload = json.loads(result.payload)

    assert result.status is Status.SUCCESS
    assert result.input_object_key == "c1.flac"
    # The object really is on the real store, under the key the reference
    # advertised, as canonical FLAC.
    assert s3_client.head_object(Bucket=store.bucket, Key="c1.flac")["ContentType"] == "audio/flac"
    assert store.get("c1.flac").startswith(b"fLaC")
    # And the numbers match what went in, across the real round trip.
    assert payload["duration_seconds"] == pytest.approx(expected_duration, abs=0.01)
    assert payload["rms"] == pytest.approx(expected_rms, rel=0.02)


class _DurationRms:
    """The reference function's algorithm, without the repo dependency: a
    decode, an RMS, and a JSON payload -- the whole point of the §5.1 split."""

    function_id = "duration_rms"
    function_version = "1.0.0"

    def process(self, ref, audio):
        samples = audio.samples()
        squares = samples.astype("float64") ** 2
        return FunctionResult(
            payload=json.dumps(
                {
                    "duration_seconds": samples.size / audio.sample_rate,
                    "rms": float(math.sqrt(squares.mean())),
                    "samples": int(samples.size),
                }
            ).encode(),
            schema_version="1",
            content_type="application/json",
        )