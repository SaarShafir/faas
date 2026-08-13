"""faas-hydrator -- step 3 of the build order (spec §4.1).

Reads source metadata, fetches audio once, transcodes to canonical FLAC, stores
it under a deterministic key, and publishes a reference. Audio never travels
through Kafka; this is the one component that touches both the Audio API and
the object store, and it is deliberately the dumbest thing in the system.

It runs on the SDK's runner rather than its own loop -- same poll/work
decoupling, same low-water-mark commits, same DLQ -- with only the input
decoder and the output emitter swapped.
"""

from .emitter import ReferenceEmitter
from .hydrator import Hydrator
from .metadata import JsonSourceDecoder
from .models import SourceRecord
from .transcode import FlacAudio, Transcoder

__all__ = [
    "FlacAudio",
    "Hydrator",
    "JsonSourceDecoder",
    "ReferenceEmitter",
    "SourceRecord",
    "Transcoder",
]

__version__ = "0.1.0"
