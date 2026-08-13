"""Per-function dead letter queue (spec §5.4).

The DLQ holds the *input message*, byte for byte, so a fixed function version
can replay it. Everything needed to understand and re-route the failure goes in
headers rather than in the body -- the body must stay replayable as-is.

The offset is committed once the message is here. A poison message must never
accrue unbounded lag while every other function looks healthy.
"""

from __future__ import annotations

from .models import ErrorInfo, InboundMessage, OutboundRecord


class DeadLetterQueue:
    def __init__(self, *, config, producer, clock=None):
        self.config = config
        self.producer = producer
        self._clock = clock or _system_clock()

    def send(
        self,
        message: InboundMessage,
        error: ErrorInfo,
        *,
        attempt: int = 1,
        call_id: str | None = None,
        function_id: str | None = None,
        function_version: str | None = None,
    ) -> None:
        """DLQ one record. `function_id`/`function_version` override the
        config's: a function's runner never passes them, while the sink reads
        them off the §6 composite key of a poison result -- whose producer is
        a *different* function, and the DLQ record should say which."""
        function_id = function_id or self.config.function_id
        function_version = function_version or self.config.function_version
        headers = {
            "faas.function_id": function_id.encode(),
            "faas.function_version": function_version.encode(),
            "faas.group_id": self.config.group_id.encode(),
            "faas.error.code": error.code.encode(),
            "faas.error.message": error.message.encode()[:1024],
            "faas.error.retryable": str(error.retryable).lower().encode(),
            "faas.attempt": str(attempt).encode(),
            "faas.source.topic": message.topic.encode(),
            "faas.source.partition": str(message.partition).encode(),
            "faas.source.offset": str(message.offset).encode(),
            "faas.failed_at": self._clock.now().isoformat().encode(),
        }
        if call_id:
            headers["faas.call_id"] = call_id.encode()

        self.producer.produce(
            OutboundRecord(
                topic=self.config.dlq_topic,
                key=message.key,
                value=message.value,
                partition_key=message.key,
                headers=headers,
            )
        )


def _system_clock():
    from .clock import SystemClock

    return SystemClock()
