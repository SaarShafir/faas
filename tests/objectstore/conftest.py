"""A real MinIO object store, for the things a dict fake structurally cannot show.

The SDK's `S3ObjectStore` maps boto3 errors onto the retry/DLQ taxonomy, and
the claim-check path moves bytes through a real S3 wire protocol. The contract
test's `FakeObjectStore` cannot show a real `NoSuchKey` response, a real
content-type round trip, or a real multi-MB object; that is what this suite is
for.

MinIO rather than LocalStack: MinIO is one container with no external
services, and the deployment target is S3-compatible anyway. Managed directly
rather than through testcontainers, for the same reason the Kafka suite does it
by hand: the host port is chosen up front so the server advertises a reachable
endpoint, and there is no race against a start script.

These tests are marked `minio` and excluded from the default run:

    pytest -m minio
"""

from __future__ import annotations

import socket
import subprocess
import time
import uuid

import pytest

IMAGE = "minio/minio:latest"
CONTAINER_NAME = "faas-test-minio"
READY_TIMEOUT_SECONDS = 60

pytestmark = pytest.mark.minio


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
def minio_endpoint() -> str:
    if not _docker_available():
        pytest.skip("docker is not running")

    port = _free_port()
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)
    subprocess.run(
        [
            "docker", "run", "-d", "--name", CONTAINER_NAME,
            "-p", f"{port}:9000",
            "-e", "MINIO_ROOT_USER=faastest",
            "-e", "MINIO_ROOT_PASSWORD=faastestpassword",
            IMAGE,
            "server", "/data",
        ],
        capture_output=True,
        check=True,
    )

    endpoint = f"http://localhost:{port}"
    try:
        _await_ready(endpoint)
        yield endpoint
    finally:
        subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)


def _await_ready(endpoint: str) -> None:
    """Poll with a real S3 call rather than sleeping a guessed interval."""
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="faastest",
        aws_secret_access_key="faastestpassword",
        config=Config(signature_version="s3v4", retries={"max_attempts": 1}),
    )
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    last_error = None
    while time.monotonic() < deadline:
        try:
            client.list_buckets()
            return
        except Exception as exc:  # noqa: BLE001 - server not up yet
            last_error = exc
            time.sleep(0.5)

    logs = subprocess.run(
        ["docker", "logs", "--tail", "40", CONTAINER_NAME], capture_output=True
    ).stdout.decode("utf-8", "replace")
    raise RuntimeError(f"minio never became ready: {last_error}\n{logs}")


@pytest.fixture
def s3_client(minio_endpoint):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id="faastest",
        aws_secret_access_key="faastestpassword",
        config=Config(signature_version="s3v4"),
    )


@pytest.fixture
def bucket(s3_client) -> str:
    name = f"faas-test-{uuid.uuid4().hex[:12]}"
    s3_client.create_bucket(Bucket=name)
    yield name
    # Delete the bucket and everything in it. A leftover bucket is cheap, but
    # the objects inside it are the kind of cross-test state that has bitten
    # this suite's sibling before.
    objects = s3_client.list_objects_v2(Bucket=name).get("Contents", [])
    if objects:
        s3_client.delete_objects(
            Bucket=name,
            Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
        )
    s3_client.delete_bucket(Bucket=name)


@pytest.fixture
def store(s3_client, bucket):
    from faas_sdk.objectstore import S3ObjectStore

    return S3ObjectStore(bucket=bucket, client=s3_client)