"""Function declaration (spec §8): one PR, zero infra tickets."""

import pytest

from faas_sdk.config import FunctionConfig

DECLARATION = """
function_id: speaker_diarization
function_version: "2.1.0"
image: registry/faas-speaker-diarization:2.1.0
resources:
  cpu: 4
  memory: 8Gi
  gpu: 1
in_flight: 4
per_file_timeout_seconds: 120
retry_budget: 3
dlq_topic: faas.dlq.speaker_diarization
payload_schema: schemas/speaker_diarization/v2.proto
"""


def test_loads_the_spec_declaration(tmp_path):
    path = tmp_path / "function.yaml"
    path.write_text(DECLARATION)

    config = FunctionConfig.from_yaml(path)

    assert config.function_id == "speaker_diarization"
    assert config.function_version == "2.1.0"
    assert config.in_flight == 4
    assert config.per_file_timeout_seconds == 120
    assert config.retry_budget == 3
    assert config.dlq_topic == "faas.dlq.speaker_diarization"
    assert config.resources.cpu == 4
    assert config.resources.gpu == 1


def test_group_id_is_function_id_and_version(tmp_path):
    """Spec §4.3 -- this is what buys independent lag and free shadow deploys."""
    path = tmp_path / "function.yaml"
    path.write_text(DECLARATION)
    config = FunctionConfig.from_yaml(path)

    assert config.group_id == "speaker_diarization:2.1.0"


def test_dlq_topic_defaults_from_the_function_id(tmp_path):
    path = tmp_path / "function.yaml"
    path.write_text('function_id: foo\nfunction_version: "1"\nimage: img\n')
    config = FunctionConfig.from_yaml(path)

    assert config.dlq_topic == "faas.dlq.foo"


def test_missing_required_fields_fail_loudly(tmp_path):
    path = tmp_path / "function.yaml"
    path.write_text("function_version: '1'\nimage: img\n")

    with pytest.raises(ValueError, match="function_id"):
        FunctionConfig.from_yaml(path)


@pytest.mark.parametrize(
    "field,value",
    [("in_flight", 0), ("retry_budget", 0), ("per_file_timeout_seconds", 0)],
)
def test_nonsensical_values_are_rejected(field, value):
    kwargs = dict(function_id="f", function_version="1", image="img")
    kwargs[field] = value
    with pytest.raises(ValueError, match=field):
        FunctionConfig(**kwargs)


def test_consumer_config_pins_the_settings_that_make_poll_work_decoupling_safe():
    config = FunctionConfig(
        function_id="f",
        function_version="1",
        image="img",
        per_file_timeout_seconds=120,
        in_flight=4,
    )
    kafka = config.consumer_config(bootstrap_servers="kafka:9092")

    assert kafka["group.id"] == "f:1"
    assert kafka["enable.auto.commit"] is False
    assert kafka["enable.auto.offset.store"] is False
    # The poll loop must survive a full pool of files each taking the timeout.
    assert kafka["max.poll.interval.ms"] >= 120 * 1000
