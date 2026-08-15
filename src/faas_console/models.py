"""What the console shows.

Plain dataclasses, deliberately separate from `faas_sdk.models`. Those are the
wire types and the SDK owns them; these are view types and exist to answer a
question a person asked. Keeping them apart means a display concern -- "which
functions are missing for this call" -- never leaks into the envelope.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class ResultView:
    """One function's answer for one call."""

    function_id: str
    function_version: str
    status: str
    attempt: int
    payload: bytes | None
    payload_ref: str | None
    payload_content_type: str
    error_code: str = ""
    error_message: str = ""
    error_retryable: bool = False
    ingested_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    partition: int = -1
    offset: int = -1

    @property
    def latency_seconds(self) -> float | None:
        if not (self.ingested_at and self.completed_at):
            return None
        return (self.completed_at - self.ingested_at).total_seconds()

    @property
    def process_seconds(self) -> float | None:
        if not (self.started_at and self.completed_at):
            return None
        return (self.completed_at - self.started_at).total_seconds()


@dataclass(frozen=True)
class DeadLetter:
    """A DLQ record, read entirely from its headers.

    The body is the *input message* byte for byte so a fixed version can replay
    it (§5.4), which means everything describing the failure is in the headers.
    """

    topic: str
    function_id: str
    function_version: str
    group_id: str
    error_code: str
    error_message: str
    retryable: bool
    attempt: int
    call_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    failed_at: str
    body_bytes: int


@dataclass(frozen=True)
class Reference:
    """The hydrator's output for a call -- proof it was hydrated at all."""

    call_id: str
    object_key: str
    sample_rate: int
    channels: int
    duration_seconds: float
    ingested_at: datetime | None
    hydrated_at: datetime | None
    partition: int = -1
    offset: int = -1


@dataclass
class CallTrace:
    """Everything the platform knows about one call."""

    call_id: str
    reference: Reference | None
    results: list[ResultView] = field(default_factory=list)
    dead_letters: list[DeadLetter] = field(default_factory=list)
    # Functions that were expected to answer and did not, by either route.
    missing: list[str] = field(default_factory=list)
    # Which partitions were actually read, so the cost of the lookup is visible
    # rather than a claim -- see KafkaConsoleReader.find_call.
    partitions_scanned: list[str] = field(default_factory=list)
    scan_seconds: float = 0.0

    @property
    def hydrated(self) -> bool:
        return self.reference is not None

    @property
    def complete(self) -> bool:
        """§7's question, answered the manual way.

        There is no aggregator and no `call_complete` marker yet, so this is
        the console deciding for itself whether every expected function has
        finished. When §7 gets built this should defer to the marker rather
        than keep its own opinion.
        """
        return self.hydrated and not self.missing

    @property
    def duration_disagreement(self) -> float | None:
        """Measured duration against what the reference claims.

        A truncated upload hydrates successfully into a FLAC of the wrong
        length -- the header still claims the original duration, and the
        hydrator only copies bytes rather than re-deriving them -- so the only
        signal is a function that measured the audio disagreeing with the
        hydrator that described it.
        """
        if self.reference is None:
            return None
        import json

        for result in self.results:
            if result.function_id != "duration_rms" or not result.payload:
                continue
            try:
                payload = json.loads(result.payload)
            except ValueError:
                return None
            measured = payload.get("duration_seconds")
            claimed = payload.get("reference_duration_seconds")
            if measured is None or not claimed:
                return None
            return measured - claimed
        return None


@dataclass(frozen=True)
class GroupStatus:
    """One consumer group: one `(function, version)` pair (§4.3)."""

    group_id: str
    function_id: str
    function_version: str
    state: str
    members: int
    lag: int
    partitions_uncommitted: int

    @property
    def declared(self) -> bool:
        return bool(self.function_id)


@dataclass(frozen=True)
class TopicInfo:
    name: str
    partitions: int


@dataclass(frozen=True)
class Finding:
    """A config-lint result.

    `severity` is one of "error", "warning", "info". An error means the stack is
    misconfigured in a way that fails at run time rather than at startup, which
    is the class of problem this exists to catch.
    """

    severity: str
    subject: str
    message: str
