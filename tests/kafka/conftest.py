"""A real Apache Kafka broker, for the things fakes structurally cannot show.

Apache Kafka in KRaft mode rather than Redpanda: the behaviour under test *is*
the rebalance protocol and the group coordinator, which is exactly where a
reimplementation could differ subtly. The deployment target is AMQ Streams
(Strimzi), which is Apache Kafka, so that is what these run against.

The container is managed directly rather than through testcontainers, which
raced its own start script on Docker Desktop here. Doing it by hand also removes
the chicken-and-egg on advertised listeners: the host port is chosen up front,
so the broker can advertise it from the start.

These tests are marked `kafka` and excluded from the default run -- the unit
suite finishes in seconds and that is worth protecting. Run them with:

    pytest -m kafka
"""

from __future__ import annotations

import socket
import subprocess
import time
import uuid

import pytest

IMAGE = "apache/kafka:latest"
CONTAINER_NAME = "faas-test-kafka"
READY_TIMEOUT_SECONDS = 90

pytestmark = pytest.mark.kafka


def _docker_available() -> bool:
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0 and bool(result.stdout.strip())


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="session")
def bootstrap_servers() -> str:
    if not _docker_available():
        pytest.skip("docker is not running")

    port = _free_port()
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            CONTAINER_NAME,
            "-p",
            f"{port}:9092",
            "-e",
            "KAFKA_NODE_ID=1",
            "-e",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "-e",
            "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
            "-e",
            f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:{port}",
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            # Rebalances are what these tests are about; waiting three seconds
            # for each one would dominate the runtime.
            "-e",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            IMAGE,
        ],
        capture_output=True,
        check=True,
    )

    servers = f"localhost:{port}"
    try:
        _await_ready(servers)
        yield servers
    finally:
        subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)


def _await_ready(servers: str) -> None:
    """Poll for real metadata rather than sleeping a guessed interval."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": servers})
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    last_error = None
    while time.monotonic() < deadline:
        try:
            admin.list_topics(timeout=2)
            return
        except Exception as exc:  # noqa: BLE001 - broker not up yet
            last_error = exc
            time.sleep(0.5)

    logs = subprocess.run(
        ["docker", "logs", "--tail", "40", CONTAINER_NAME], capture_output=True
    ).stdout.decode("utf-8", "replace")
    raise RuntimeError(f"kafka never became ready: {last_error}\n{logs}")


@pytest.fixture
def topic_factory(bootstrap_servers):
    """Fresh, uniquely named topics so tests need no cleanup or ordering."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    created = []

    def make(partitions: int = 1, prefix: str = "t") -> str:
        name = f"{prefix}-{uuid.uuid4().hex[:12]}"
        for _, future in admin.create_topics(
            [NewTopic(name, num_partitions=partitions, replication_factor=1)]
        ).items():
            future.result(timeout=30)
        _await_topic(admin, name, partitions)
        created.append(name)
        return name

    yield make

    if created:
        admin.delete_topics(created)


def _await_topic(admin, name: str, partitions: int) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        metadata = admin.list_topics(timeout=5)
        topic = metadata.topics.get(name)
        if topic is not None and not topic.error and len(topic.partitions) == partitions:
            return
        time.sleep(0.2)
    raise RuntimeError(f"topic {name} never became visible")


@pytest.fixture
def group_id() -> str:
    return f"g-{uuid.uuid4().hex[:12]}"
