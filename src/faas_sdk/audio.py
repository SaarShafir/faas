"""AudioHandle -- lazy fetch and decode (spec §5.1).

Function authors never see S3 or FLAC handling. They get a handle and pull
either raw bytes or samples; both are fetched once and cached for the lifetime
of the call.

Only canonical FLAC is ever decoded here (16 kHz, 16-bit, mono, per §4.1). The
input-format zoo is ffmpeg's problem in the hydrator, and keeping it there is
what lets the SDK depend on nothing heavier than libsndfile.
"""

from __future__ import annotations

from .errors import ObjectMissingError, PoisonMessageError
from .models import AudioReference


class AudioHandle:
    def __init__(self, ref: AudioReference, object_store, fallback=None):
        self.ref = ref
        self.object_key = ref.object_key
        self.sample_rate = ref.sample_rate
        self._object_store = object_store
        self._fallback = fallback
        self._bytes: bytes | None = None
        self._samples = None

    def bytes(self) -> bytes:
        """Raw FLAC. Fetched once, cached."""
        if self._bytes is None:
            self._bytes = self._fetch()
        return self._bytes

    def samples(self):
        """Decoded float32 mono samples as a numpy array. Decoded once, cached."""
        if self._samples is None:
            self._samples = self._decode(self.bytes())
        return self._samples

    def _fetch(self) -> bytes:
        try:
            return self._object_store.get(self.object_key)
        except ObjectMissingError:
            # Spec §5.4: lag exceeded the 24h object TTL. The 48h topic
            # retention is what makes this recoverable -- we hit a live offset
            # with a dead key rather than an evicted offset and a silent skip.
            if self._fallback is None:
                raise
            return self._fallback.fetch(self.ref)

    def _decode(self, raw: bytes):
        import io

        try:
            import soundfile
        except ImportError as exc:  # pragma: no cover - packaging error, not data
            raise RuntimeError(
                "soundfile/libsndfile is missing from the function image"
            ) from exc

        try:
            samples, sample_rate = soundfile.read(io.BytesIO(raw), dtype="float32")
        except Exception as exc:  # noqa: BLE001 - libsndfile raises broadly
            raise PoisonMessageError(
                f"could not decode {self.object_key} as FLAC: {exc}",
                code="DECODE_ERROR",
            ) from exc

        if sample_rate != self.ref.sample_rate:
            raise PoisonMessageError(
                f"{self.object_key} is {sample_rate} Hz, reference says "
                f"{self.ref.sample_rate} Hz",
                code="SAMPLE_RATE_MISMATCH",
            )
        if samples.ndim > 1:
            raise PoisonMessageError(
                f"{self.object_key} is not mono", code="CHANNEL_MISMATCH"
            )
        self.sample_rate = sample_rate
        return samples


def audio_handle_factory(object_store, fallback=None):
    """Bind an object store into the per-job factory the pool expects."""

    def build(ref: AudioReference) -> AudioHandle:
        return AudioHandle(ref, object_store=object_store, fallback=fallback)

    return build
