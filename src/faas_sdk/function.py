"""The function contract (spec §5.1).

    class Function(Protocol):
        function_id: str
        function_version: str
        def process(self, ref: AudioReference, audio: AudioHandle) -> Result: ...

One deviation, deliberate: `process` returns `FunctionResult`, not the §6
`Result`. They are different things -- §6's Result is the wire envelope, which
the SDK owns and stamps with offsets, attempt counts and timestamps. If authors
returned it, the envelope would leak into every function and the §6 split
between stable envelope and opaque payload would erode on contact.

Authors return a payload; returning None means SKIPPED.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .audio import AudioHandle
from .models import AudioReference, FunctionResult


@runtime_checkable
class Function(Protocol):
    function_id: str
    function_version: str

    def process(
        self, ref: AudioReference, audio: AudioHandle
    ) -> FunctionResult | None: ...


def validate(function) -> None:
    """Fail at startup, not on the first message."""
    for attribute in ("function_id", "function_version"):
        if not getattr(function, attribute, ""):
            raise TypeError(f"{type(function).__name__} must define {attribute}")
    if not callable(getattr(function, "process", None)):
        raise TypeError(f"{type(function).__name__} must define process(ref, audio)")
