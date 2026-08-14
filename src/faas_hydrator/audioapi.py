"""Audio API client and the lazy handle the pool hands to the hydrator.

Shaped like the SDK's `AudioHandle` on purpose: fetch once, cache, and let the
pool own when it happens. Read-once is the property the claim-check pattern
exists to preserve (§3).

The rate limiter is a constructor argument because §12 requires the backfill
path to be limited separately from live hydration -- a backfill must not be
able to starve the live stream.
"""

from __future__ import annotations

from faas_sdk.errors import PoisonMessageError, TransientError

from .models import SourceRecord


class AudioApiClient:
    def __init__(self, base_url: str, session=None, timeout: float = 30.0, rate_limiter=None):
        if session is None:
            import requests

            session = requests.Session()
        self.base_url = base_url.rstrip("/")
        self.session = session
        self.timeout = timeout
        self.rate_limiter = rate_limiter

    def get_audio(self, audio_id: str) -> bytes:
        if self.rate_limiter is not None:
            self.rate_limiter.acquire()

        try:
            response = self.session.get(f"{self.base_url}/audio/{audio_id}", timeout=self.timeout)
        except Exception as exc:  # noqa: BLE001 - requests' hierarchy is broad
            raise TransientError(f"audio api unreachable: {exc}", code="AUDIO_API_ERROR") from exc

        status = response.status_code
        if status == 200:
            return response.content
        if status in (404, 410):
            # The recording is genuinely gone. No retry will conjure it, and
            # this call can never be hydrated.
            raise PoisonMessageError(
                f"audio {audio_id} not available ({status})", code="AUDIO_NOT_FOUND"
            )
        raise TransientError(f"audio api returned {status} for {audio_id}", code="AUDIO_API_ERROR")


class SourceAudioHandle:
    """Lazy, fetch-once handle over the Audio API."""

    def __init__(self, record: SourceRecord, client: AudioApiClient):
        self.record = record
        self._client = client
        self._bytes = None

    def bytes(self) -> bytes:
        if self._bytes is None:
            self._bytes = self._client.get_audio(self.record.audio_id)
        return self._bytes


def source_audio_handle_factory(client: AudioApiClient):
    def build(record: SourceRecord) -> SourceAudioHandle:
        return SourceAudioHandle(record, client)

    return build
