"""Results envelope emission (spec §6).

Three things this owns so function authors never think about them:
  - composite key, call_id-only partitioning;
  - the 256 KB claim check;
  - a FAILED record whenever a call was attempted and produced nothing, so
    downstream can tell "no result yet" from "no result ever" (§5.4).
"""

from __future__ import annotations

from .models import (
    AudioReference,
    ErrorInfo,
    Job,
    JobOutcome,
    OutboundRecord,
    Result,
    Status,
)

INLINE_PAYLOAD_LIMIT_BYTES = 256 * 1024


class ResultEmitter:
    def __init__(self, *, config, producer, codec, object_store=None, clock=None):
        self.config = config
        self.producer = producer
        self.codec = codec
        self.object_store = object_store
        self._clock = clock or _system_clock()

    def emit(self, outcome: JobOutcome) -> Result:
        job = outcome.job
        payload, payload_ref = self._place_payload(job.ref, outcome)
        result = Result(
            call_id=job.ref.call_id,
            function_id=self.config.function_id,
            function_version=self.config.function_version,
            status=outcome.status,
            error=outcome.error,
            payload=payload,
            payload_ref=payload_ref,
            payload_schema_version=outcome.schema_version,
            payload_content_type=outcome.content_type,
            input_object_key=job.ref.object_key,
            input_offset=job.message.offset,
            attempt=job.attempt,
            ingested_at=job.ref.ingested_at,
            started_at=outcome.started_at,
            completed_at=outcome.completed_at or self._clock.now(),
        )
        self._produce(result)
        return result

    def emit_failure(
        self,
        job: Job,
        error: ErrorInfo,
        *,
        started_at=None,
    ) -> Result:
        """The §5.4 counterpart to a DLQ record: attempted, produced nothing."""
        return self.emit(
            JobOutcome(
                job=job,
                status=Status.FAILED,
                error=error,
                started_at=started_at,
                completed_at=self._clock.now(),
            )
        )

    def _place_payload(self, ref: AudioReference, outcome: JobOutcome):
        if outcome.status is not Status.SUCCESS or not outcome.payload:
            return None, None
        if len(outcome.payload) < INLINE_PAYLOAD_LIMIT_BYTES:
            return outcome.payload, None
        if self.object_store is None:
            raise RuntimeError(
                f"payload of {len(outcome.payload)} bytes needs a claim check "
                "but no object store is configured"
            )
        key = self.payload_key(ref.call_id)
        self.object_store.put(key, outcome.payload, content_type=outcome.content_type)
        return None, key

    def payload_key(self, call_id: str) -> str:
        # Namespaced by function and version: a shadow-deployed v2 writes
        # alongside v1 rather than over it (spec §4.3).
        return f"results/{self.config.function_id}/{self.config.function_version}/{call_id}"

    def _produce(self, result: Result) -> None:
        self.producer.produce(
            OutboundRecord(
                topic=self.config.results_topic,
                key=result.key,
                value=self.codec.encode_result(result),
                partition_key=result.partition_key,
                headers={
                    "faas.function_id": self.config.function_id.encode(),
                    "faas.function_version": self.config.function_version.encode(),
                    "faas.status": result.status.name.encode(),
                },
            )
        )


def _system_clock():
    from .clock import SystemClock

    return SystemClock()
