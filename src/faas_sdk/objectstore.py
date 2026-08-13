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


class S3ObjectStore:
    def __init__(self, bucket: str, client=None, **client_kwargs):
        if client is None:
            import boto3

            client = boto3.client("s3", **client_kwargs)
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
