"""The sink's poll loop (spec §4.4).

A function cannot block its poll loop on work -- seconds of audio processing
exceeds max.poll.interval.ms and the broker evicts it (the whole of §5.2). The
sink's work is a SQLite write, microseconds, so unlike a function it needs no
in-flight pool: the loop handles one record per iteration and the ledger
degenerates to commit-after-each. Everything else is the same machinery, with
the same reasons:

  - at-least-once commits; the store's upsert on the §6 composite key makes
    redelivery harmless (§5.3);
  - poison (an undecodable result -- now including a record whose status was
    never set, which decodes as DecodeError) goes to the DLQ and the offset is
    committed, so one bad producer cannot accrue unbounded lag (§5.4);
  - FAILED and SKIPPED results land like SUCCESS ones: the store is where "no
    result yet" becomes distinguishable from "no result ever".
"""

from __future__ import annotations

import logging

from faas_sdk.clock import SystemClock
from faas_sdk.codec import DecodeError
from faas_sdk.metrics import DLQ, NullMetrics
from faas_sdk.models import ErrorInfo, InboundMessage
from faas_sdk.offsets import OffsetLedger

log = logging.getLogger(__name__)

LANDED = "faas.sink.landed"


class SinkRunner:
    def __init__(
        self,
        *,
        config,
        consumer,
        store,
        codec,
        dlq,
        metrics=None,
        clock=None,
    ):
        self.config = config
        self.consumer = consumer
        self.store = store
        self.codec = codec
        self.dlq = dlq
        self.metrics = metrics or NullMetrics()
        self.clock = clock or SystemClock()

        self.ledger = OffsetLedger()
        self._running = False
        self._last_commit = self.clock.monotonic()

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        self.consumer.subscribe(
            [self.config.results_topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

    def run(self) -> None:
        self._running = True
        self.start()
        try:
            while self._running:
                self.run_once()
        finally:
            self.close()

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        self._running = False
        self._commit(force=True)
        self.consumer.close()
        self.store.close()

    # -- one iteration -----------------------------------------------------

    def run_once(self) -> None:
        message = self.consumer.poll(self.config.poll_timeout_seconds)
        if message is not None:
            self._handle(message)
        self._commit()

    # -- records -----------------------------------------------------------

    def _handle(self, message: InboundMessage) -> None:
        self.ledger.start(message.tp, message.offset)
        try:
            result = self.codec.decode_result(message.value)
        except DecodeError as exc:
            # Poison on arrival: never retried, never landed, offset committed.
            # The producer of a record that does not decode is broken; the sink
            # is not the place to discover it twice.
            self._poison(message, ErrorInfo("DECODE_ERROR", str(exc), retryable=False))
            return

        self.store.upsert(result)
        self.metrics.counter(LANDED, 1, status=result.status.name)
        self.ledger.complete(message.tp, message.offset)

    def _poison(self, message: InboundMessage, error: ErrorInfo) -> None:
        call_id, function_id, function_version = _split_key(message.key)
        self.dlq.send(
            message,
            error,
            attempt=1,
            call_id=call_id,
            function_id=function_id,
            function_version=function_version,
        )
        self.metrics.counter(DLQ, 1, reason=error.code)
        log.warning(
            "poison result on %s:%s at offset %s (%s); DLQ'd",
            message.topic,
            message.partition,
            message.offset,
            error.code,
        )
        self.ledger.complete(message.tp, message.offset)

    # -- commits -----------------------------------------------------------

    def _commit(self, force: bool = False) -> None:
        now = self.clock.monotonic()
        if not force and now - self._last_commit < self.config.commit_interval_seconds:
            return
        offsets = self.ledger.drain_committable()
        self._last_commit = now
        if offsets:
            self.consumer.commit(offsets)

    # -- rebalance ---------------------------------------------------------

    def _on_assign(self, partitions) -> None:
        log.info("assigned %s", partitions)

    def _on_revoke(self, partitions) -> None:
        # Synchronous: nothing is ever in flight here, so a revoke means only
        # committing what the ledger knows and forgetting the partitions. The
        # new owner replays whatever was not committed, and the upsert dedups.
        self._commit(force=True)
        self.ledger.revoke(partitions)
        log.info("revoked %s", partitions)


def _split_key(key) -> tuple[str, str, str]:
    """§6 composite key: {call_id}:{function_id}:{function_version}.

    Recovered from the key even when the body is poison, so the DLQ record can
    say which function's producer wrote it.
    """
    if not key:
        return "", "", ""
    parts = key.decode("utf-8", "replace").split(":", 2)
    while len(parts) < 3:
        parts.append("")
    return parts[0], parts[1], parts[2]