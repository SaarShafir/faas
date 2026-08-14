"""The contract test (spec §13 step 4).

"Do not build functions 2..N until step 4 is stable. The reference function is
the contract test."

So this runs the real thing: real ffmpeg transcoding real audio, a real object
store round-trip, real libsndfile decoding, the real protobuf wire format, and
both runners with their real ledgers, retry logic and DLQ routing. Only Kafka
and S3 are fakes, and both are fakes of interfaces the SDK owns.

A call goes in as an Audio API response and comes out as a Result on the
results topic, with measurements that match what went in. If this passes, the
platform works and every subsequent function is the same shape.
"""

import json
import math
import shutil
import subprocess

import pytest

from faas_hydrator.audioapi import source_audio_handle_factory
from faas_hydrator.emitter import ReferenceEmitter
from faas_hydrator.hydrator import Hydrator
from faas_hydrator.metadata import JsonSourceDecoder
from faas_hydrator.testing import FakeAudioApi
from faas_hydrator.transcode import Transcoder
from faas_sdk.audio import audio_handle_factory
from faas_sdk.codec_protobuf import ProtobufCodec
from faas_sdk.config import FunctionConfig
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.models import InboundMessage, Status
from faas_sdk.pool import InlineWorkerPool
from faas_sdk.results import ResultEmitter
from faas_sdk.runner import FunctionRunner
from faas_sdk.testing import FakeConsumer, FakeObjectStore, FakeProducer
from functions.duration_rms.function import DurationRms

pytestmark = pytest.mark.skipif(
    shutil.which("ffmpeg") is None, reason="the contract test needs real ffmpeg"
)

INPUT_TOPIC = "faas.calls.raw"
INTERNAL_TOPIC = "faas.audio.internal"
RESULTS_TOPIC = "faas.results"


def _wav(seconds=2, rate=44100, channels=2, source="sine=frequency=440"):
    """Real audio, produced by ffmpeg, in a format that is not the canonical one."""
    # lavfi separates the first filter option with '=' and the rest with ':'.
    separator = ":" if "=" in source else "="
    return subprocess.run(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            f"{source}{separator}sample_rate={rate}",
            "-ac",
            str(channels),
            "-t",
            str(seconds),
            "-f",
            "wav",
            "pipe:1",
        ],
        capture_output=True,
        check=True,
    ).stdout


def _measure(wav_bytes):
    """What went in, measured the same way the function measures what comes out.

    Ground truth for the fidelity assertions, so they do not depend on the
    amplitude ffmpeg's synthetic sources happen to default to.
    """
    import io

    import soundfile

    samples, rate = soundfile.read(io.BytesIO(wav_bytes), dtype="float32")
    if samples.ndim > 1:  # stereo in, mono out -- compare against the downmix
        samples = samples.mean(axis=1)
    squares = samples.astype("float64") ** 2
    return {
        "rms": float(math.sqrt(squares.mean())),
        "peak": float(abs(samples).max()),
        "duration_seconds": len(samples) / rate,
    }


class Pipeline:
    """Both stages wired together over shared fakes."""

    def __init__(self, audio: bytes, clock, codec=None):
        self.codec = codec or ProtobufCodec()
        self.object_store = FakeObjectStore()
        self.audio_api = FakeAudioApi(default=audio)
        self.producer = FakeProducer()

        self.hydrator_config = FunctionConfig(
            function_id="hydrator",
            function_version="1.0.0",
            image="registry/faas-hydrator:1.0.0",
            input_topic=INPUT_TOPIC,
            results_topic=INTERNAL_TOPIC,
            in_flight=2,
            commit_interval_seconds=0.0,
        )
        self.function_config = FunctionConfig.from_yaml("functions/duration_rms/function.yaml")
        object.__setattr__(self.function_config, "input_topic", INTERNAL_TOPIC)
        object.__setattr__(self.function_config, "results_topic", RESULTS_TOPIC)
        object.__setattr__(self.function_config, "commit_interval_seconds", 0.0)

        self.hydrator_consumer = FakeConsumer(assignment=[(INPUT_TOPIC, 0)])
        self.function_consumer = FakeConsumer(assignment=[(INTERNAL_TOPIC, 0)])

        hydrator = Hydrator(
            transcoder=Transcoder(),  # real ffmpeg
            object_store=self.object_store,
            codec=self.codec,
            clock=clock,
        )
        self.hydrator_runner = FunctionRunner(
            config=self.hydrator_config,
            consumer=self.hydrator_consumer,
            pool=InlineWorkerPool(
                function=hydrator,
                audio_handle_factory=source_audio_handle_factory(self.audio_api),
                max_in_flight=2,
                clock=clock,
            ),
            codec=self.codec,
            decoder=JsonSourceDecoder().decode,
            results=ReferenceEmitter(topic=INTERNAL_TOPIC, producer=self.producer),
            dlq=DeadLetterQueue(config=self.hydrator_config, producer=self.producer, clock=clock),
            clock=clock,
        )
        self.function_runner = FunctionRunner(
            config=self.function_config,
            consumer=self.function_consumer,
            pool=InlineWorkerPool(
                function=DurationRms(),
                # The real handle: fetches from the object store the hydrator
                # wrote to, and decodes with libsndfile.
                audio_handle_factory=audio_handle_factory(self.object_store),
                max_in_flight=2,
                clock=clock,
            ),
            codec=self.codec,
            results=ResultEmitter(
                config=self.function_config,
                producer=self.producer,
                object_store=self.object_store,
                codec=self.codec,
                clock=clock,
            ),
            dlq=DeadLetterQueue(config=self.function_config, producer=self.producer, clock=clock),
            clock=clock,
        )
        self.hydrator_runner.start()
        self.function_runner.start()

    def run(self, call_id="c1", offset=10):
        self.hydrator_consumer.feed(
            InboundMessage(
                topic=INPUT_TOPIC,
                partition=0,
                offset=offset,
                key=call_id.encode(),
                value=json.dumps({"call_id": call_id, "audio_id": f"audio-{call_id}"}).encode(),
            )
        )
        self.hydrator_runner.run_once()
        self.hydrator_runner.run_once()

        # The hydrator's output is fed to the function verbatim: same bytes,
        # same key. If the two disagreed about the wire format, this is where
        # it would show.
        for index, record in enumerate(self.records(INTERNAL_TOPIC)):
            self.function_consumer.feed(
                InboundMessage(
                    topic=INTERNAL_TOPIC,
                    partition=0,
                    offset=100 + index,
                    key=record.key,
                    value=record.value,
                )
            )
        self.function_runner.run_once()
        self.function_runner.run_once()

    def records(self, topic):
        return [r for r in self.producer.records if r.topic == topic]

    def result(self):
        (record,) = self.records(RESULTS_TOPIC)
        return self.codec.decode_result(record.value)

    def payload(self):
        return json.loads(self.result().payload)


def test_a_call_goes_in_as_audio_and_comes_out_as_a_result(clock):
    pipeline = Pipeline(_wav(seconds=2), clock)
    pipeline.run(call_id="c1")

    result = pipeline.result()
    assert result.status is Status.SUCCESS
    assert result.call_id == "c1"
    assert result.function_id == "duration_rms"
    assert result.function_version == "1.0.0"
    assert result.error is None


def test_a_known_amplitude_survives_the_whole_path_exactly(clock):
    """A constant 0.5 signal has RMS and peak of exactly 0.5.

    Chosen over a synthetic sine because the amplitude is stated rather than
    inherited from an ffmpeg source default -- so this pins the numbers, not
    ffmpeg's taste. 44.1 kHz in, 16 kHz canonical FLAC out, and the level is
    unchanged: no stage applies a stray gain.
    """
    pipeline = Pipeline(_wav(seconds=1, channels=1, source="aevalsrc=exprs=0.5"), clock)
    pipeline.run()

    payload = pipeline.payload()
    assert payload["rms"] == pytest.approx(0.5, abs=1e-4)
    assert payload["peak"] == pytest.approx(0.5, abs=1e-4)
    assert payload["duration_seconds"] == pytest.approx(1.0, abs=0.01)


def test_the_measurements_match_the_audio_that_went_in(clock):
    """Fidelity across stereo 44.1 kHz WAV -> mono 16 kHz FLAC -> S3 ->
    libsndfile, measured against the input rather than a constant."""
    source = _wav(seconds=2)
    expected = _measure(source)

    pipeline = Pipeline(source, clock)
    pipeline.run()

    payload = pipeline.payload()
    assert payload["duration_seconds"] == pytest.approx(expected["duration_seconds"], abs=0.01)
    assert payload["rms"] == pytest.approx(expected["rms"], rel=0.02)
    assert payload["peak"] == pytest.approx(expected["peak"], rel=0.02)
    assert payload["rms"] > 0  # guard: a silent pipeline would pass the above


def test_silence_survives_the_round_trip_as_silence(clock):
    pipeline = Pipeline(_wav(seconds=1, source="anullsrc"), clock)
    pipeline.run()

    payload = pipeline.payload()
    assert payload["rms"] == 0.0
    assert payload["dbfs"] == pytest.approx(-120.0)
    assert payload["duration_seconds"] == pytest.approx(1.0, abs=0.01)


def test_the_audio_is_canonical_by_the_time_the_function_sees_it(clock):
    """§4.1: the input zoo stops at the hydrator. The SDK only ever decodes
    16 kHz mono FLAC, and that is what makes libsndfile a sufficient dependency."""
    pipeline = Pipeline(_wav(seconds=1, rate=48000, channels=2), clock)
    pipeline.run()

    (reference_record,) = pipeline.records(INTERNAL_TOPIC)
    ref = pipeline.codec.decode_reference(reference_record.value)

    assert ref.sample_rate == 16000
    assert ref.channels == 1
    assert pipeline.object_store.objects["c1.flac"].startswith(b"fLaC")


def test_the_object_is_read_from_the_key_the_reference_advertises(clock):
    pipeline = Pipeline(_wav(seconds=1), clock)
    pipeline.run(call_id="call-abc")

    ref = pipeline.codec.decode_reference(pipeline.records(INTERNAL_TOPIC)[0].value)
    assert ref.object_key == "call-abc.flac"
    assert pipeline.object_store.gets == ["call-abc.flac"]
    assert pipeline.result().input_object_key == "call-abc.flac"


def test_the_result_is_keyed_for_idempotent_dedup(clock):
    """§6: composite key, partitioned on call_id alone."""
    pipeline = Pipeline(_wav(seconds=1), clock)
    pipeline.run(call_id="c1")

    (record,) = pipeline.records(RESULTS_TOPIC)
    assert record.key == b"c1:duration_rms:1.0.0"
    assert record.partition_key == b"c1"


def test_both_stages_commit_their_offsets(clock):
    pipeline = Pipeline(_wav(seconds=1), clock)
    pipeline.run(call_id="c1", offset=10)

    assert pipeline.hydrator_consumer.commits[-1] == [((INPUT_TOPIC, 0), 11)]
    assert pipeline.function_consumer.commits[-1] == [((INTERNAL_TOPIC, 0), 101)]


def test_provenance_survives_the_whole_path(clock):
    """ingested_at is set by whoever wrote the input topic and must reach the
    result unchanged -- it is the only end-to-end latency measure there is."""
    pipeline = Pipeline(_wav(seconds=1), clock)
    pipeline.run()

    ref = pipeline.codec.decode_reference(pipeline.records(INTERNAL_TOPIC)[0].value)
    result = pipeline.result()

    assert result.ingested_at == ref.ingested_at
    assert result.attempt == 1


def test_reprocessing_a_call_is_harmless(clock):
    """At-least-once means duplicates on restart are expected (§5.3). The
    deterministic object key and the composite result key are what make that a
    non-event rather than a corruption."""
    pipeline = Pipeline(_wav(seconds=1), clock)
    pipeline.run(call_id="c1", offset=10)
    first = pipeline.result()

    pipeline.producer.records.clear()
    pipeline.run(call_id="c1", offset=11)
    second = pipeline.result()

    assert list(pipeline.object_store.objects) == ["c1.flac"]
    assert second.key == first.key
    assert json.loads(second.payload) == json.loads(first.payload)


def test_a_call_whose_audio_is_not_audio_never_reaches_the_function(clock):
    """The zoo includes things that are not in the zoo. It stops at the
    hydrator's DLQ rather than becoming every function's problem."""
    pipeline = Pipeline(b"this is a text file, not audio", clock)
    pipeline.run(call_id="c1")

    assert pipeline.records(INTERNAL_TOPIC) == []
    assert pipeline.records(RESULTS_TOPIC) == []
    assert len(pipeline.records("faas.dlq.hydrator")) == 1
    assert pipeline.object_store.objects == {}
