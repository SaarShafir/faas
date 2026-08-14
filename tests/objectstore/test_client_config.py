"""Pointing the object store at something that is not AWS.

The local stack (compose/) runs MinIO, which needs both an endpoint override
and path-style addressing. These pin that the SDK reads them from the
environment and, more importantly, that an image with no S3 environment set
still gets a stock AWS client -- this is the one place where a local-dev
convenience could quietly change production behaviour.
"""

import pytest

from faas_sdk.objectstore import S3ObjectStore, client_kwargs_from_env

boto3 = pytest.importorskip("boto3", reason="boto3 is a [runtime] extra")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in (
        "FAAS_S3_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "FAAS_S3_ADDRESSING_STYLE",
    ):
        monkeypatch.delenv(name, raising=False)


def test_no_s3_environment_means_a_stock_aws_client():
    """The default has to stay untouched: this code ships to production, where
    there is no endpoint override and virtual-host addressing is correct."""
    assert client_kwargs_from_env() == {}


def test_endpoint_url_is_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("FAAS_S3_ENDPOINT_URL", "http://minio:9000")
    assert client_kwargs_from_env()["endpoint_url"] == "http://minio:9000"


def test_the_botocore_standard_variable_also_works(monkeypatch):
    """Newer botocore reads AWS_ENDPOINT_URL_S3 itself, but the SDK cannot
    depend on the version installed in an image it does not build."""
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://minio:9000")
    assert client_kwargs_from_env()["endpoint_url"] == "http://minio:9000"


def test_faas_variable_wins_over_the_botocore_one(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://wrong:9000")
    monkeypatch.setenv("FAAS_S3_ENDPOINT_URL", "http://right:9000")
    assert client_kwargs_from_env()["endpoint_url"] == "http://right:9000"


def test_path_style_addressing_reaches_the_client_config(monkeypatch):
    """boto3's `auto` sends http://bucket.host/key, which needs per-bucket DNS
    that no local container has."""
    monkeypatch.setenv("FAAS_S3_ADDRESSING_STYLE", "path")
    assert client_kwargs_from_env()["config"].s3["addressing_style"] == "path"


def test_an_explicit_endpoint_beats_the_environment(monkeypatch):
    """A caller that passes an endpoint means it -- the environment is the
    fallback, not an override."""
    monkeypatch.setenv("FAAS_S3_ENDPOINT_URL", "http://from-env:9000")
    store = S3ObjectStore(bucket="audio", endpoint_url="http://explicit:9000")
    assert store.client.meta.endpoint_url == "http://explicit:9000"


def test_the_environment_configures_a_real_client(monkeypatch):
    """End of the chain: the variables actually land on a boto3 client, not
    just in a dict."""
    monkeypatch.setenv("FAAS_S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("FAAS_S3_ADDRESSING_STYLE", "path")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    store = S3ObjectStore(bucket="audio")

    assert store.client.meta.endpoint_url == "http://minio:9000"
    assert store.client.meta.config.s3["addressing_style"] == "path"
