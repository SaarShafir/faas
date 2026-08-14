"""S3-compatible object store adapter (spec §11).

Lifecycle handles the 24h TTL -- there is no reaper here and there should never
be one. A missing key means lag exceeded the TTL, which is a recoverable
condition (§5.4), so it is raised as ObjectMissingError rather than a bare
ClientError.
"""

from __future__ import annotations

import os

from .errors import ObjectMissingError, TransientError


def s3_audio_handle_factory():
    """The default audio source for a process pool worker.

    Zero-arg and picklable by reference, so it survives the spawn to a worker
    process -- and it builds the S3 client *there*, which is where it belongs.
    boto3 clients are not fork-safe and sharing one across processes is a
    well-known source of intermittent corruption.
    """
    from .audio import audio_handle_factory

    return audio_handle_factory(S3ObjectStore(bucket=os.environ["FAAS_AUDIO_BUCKET"]))


def client_kwargs_from_env() -> dict:
    """boto3 arguments for talking to something that is not AWS.

    The local stack runs MinIO, and two things differ from the AWS default.
    The endpoint has to be pointed somewhere else -- newer botocore reads
    `AWS_ENDPOINT_URL_S3` itself, but the SDK should not depend on the version
    installed in an image it does not build. And addressing has to be path-style:
    boto3's `auto` sends `http://bucket.host/key`, which needs per-bucket DNS
    that no local container has.

    Empty by default, so an image with no S3 environment set behaves exactly as
    it did before -- real AWS, real endpoints, virtual-host addressing.
    """
    kwargs: dict = {}

    endpoint = os.environ.get("FAAS_S3_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL_S3")
    if endpoint:
        kwargs["endpoint_url"] = endpoint

    style = os.environ.get("FAAS_S3_ADDRESSING_STYLE", "")
    if style:
        from botocore.config import Config

        kwargs["config"] = Config(s3={"addressing_style": style})

    return kwargs


class S3ObjectStore:
    def __init__(self, bucket: str, client=None, **client_kwargs):
        if client is None:
            import boto3

            # Explicit arguments win: a caller that passes an endpoint means it.
            client = boto3.client("s3", **{**client_kwargs_from_env(), **client_kwargs})
        self.bucket = bucket
        self.client = client

    def get(self, key: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:  # noqa: BLE001 - botocore's hierarchy is dynamic
            if _is_missing(exc):
                raise ObjectMissingError(f"{self.bucket}/{key} is gone") from exc
            raise TransientError(f"s3 get {self.bucket}/{key} failed: {exc}") from exc
        return response["Body"].read()

    def put(self, key: str, body: bytes, content_type: str = "") -> None:
        kwargs = {"Bucket": self.bucket, "Key": key, "Body": body}
        if content_type:
            kwargs["ContentType"] = content_type
        try:
            self.client.put_object(**kwargs)
        except Exception as exc:  # noqa: BLE001
            raise TransientError(f"s3 put {self.bucket}/{key} failed: {exc}") from exc


class AudioApiFallback:
    """Re-fetch path for dead object keys (spec §5.4, §12).

    Rate-limited separately from live hydration on purpose: a backfill or a
    lagging consumer must not be able to starve the live stream.
    """

    def __init__(self, client, rate_limiter=None):
        self.client = client
        self.rate_limiter = rate_limiter

    def fetch(self, ref) -> bytes:
        if self.rate_limiter is not None:
            self.rate_limiter.acquire()
        return self.client.get_audio(ref.call_id)


def _is_missing(exc) -> bool:
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
    return code in {"NoSuchKey", "404", "NotFound"} or exc.__class__.__name__ == "NoSuchKey"
