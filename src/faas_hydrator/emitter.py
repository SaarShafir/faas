"""Publishing the reference to the internal topic (spec §4.2).

Plugs into the runner where a function's `ResultEmitter` would go. Same
interface, different destination: an `AudioReference` keyed by `call_id` on the
internal topic, instead of a `Result` on the results topic.
"""

from __future__ import annotations

from faas_sdk.metrics import NullMetrics
from faas_sdk.models import OutboundRecord, Status

PUBLISHED = "faas.hydrator.published"
DROPPED = "faas.hydrator.dropped"


class ReferenceEmitter:
    def __init__(self, *, topic: str, producer, hydrator_version: str = "", metrics=None):
        self.topic = topic
        self.producer = producer
        self.hydrator_version = hydrator_version
        self.metrics = metrics or NullMetrics()

    def emit(self, outcome) -> None:
        if outcome.status is not Status.SUCCESS or not outcome.payload:
            # SKIPPED means the hydrator decided there is nothing to publish.
            self.metrics.counter(DROPPED, 1, reason=outcome.status.name)
            return

        call_id = outcome.job.ref.call_id.encode()
        self.producer.produce(
            OutboundRecord(
                topic=self.topic,
                # §4.2: keyed on call_id, and partitioned on it too, so a
                # reprocessed call replaces itself rather than landing twice on
                # different partitions.
                key=call_id,
                value=outcome.payload,
                partition_key=call_id,
                headers={
                    "faas.call_id": call_id,
                    "faas.hydrator_version": self.hydrator_version.encode(),
                },
            )
        )
        self.metrics.counter(PUBLISHED, 1)

    def emit_failure(self, job, error, started_at=None) -> None:
        """Deliberately publishes nothing.

        For a function, §5.4's "emit both" gives downstream a FAILED record
        alongside the DLQ entry. There is no equivalent here: the hydrator is
        upstream of every topic a consumer reads, so a call that fails
        hydration is simply invisible -- no reference, therefore no function
        ever sees it, therefore the aggregator (§7) never expects it either.

        The DLQ holds the input for replay, which is the spec's answer. But
        nothing distinguishes "not hydrated yet" from "never will be", and a
        dead-letter topic is not something downstream can reasonably watch. If
        that gap matters, the fix is a hydration-failure topic the aggregator
        honours -- a design decision, not something to invent here.
        """
        self.metrics.counter(DROPPED, 1, reason=error.code)
