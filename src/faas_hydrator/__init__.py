"""faas-hydrator -- step 3 of the build order (spec §4.1).

Reads source metadata, fetches audio once, stores it under a deterministic key
as it came off the Audio API, and publishes a reference. The Audio API serves
canonical FLAC already, so there is no codec work here at all. Audio never
travels through Kafka; this is the one component that touches both the Audio
API and the object store, and it is deliberately the dumbest thing in the
system.

It runs on the SDK's runner rather than its own loop -- same poll/work
decoupling, same low-water-mark commits, same DLQ -- with only the input
decoder and the output emitter swapped.
"""

from .emitter import ReferenceEmitter
from .flac import StreamInfo, read_streaminfo
from .hydrator import Hydrator
from .metadata import JsonSourceDecoder
from .models import SourceRecord

__all__ = [
    "Hydrator",
    "JsonSourceDecoder",
    "ReferenceEmitter",
    "SourceRecord",
    "StreamInfo",
    "read_streaminfo",
]

__version__ = "0.1.0"
