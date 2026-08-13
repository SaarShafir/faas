"""Results sink entrypoint.

Consumes the results topic and lands every record into the store. The one
service in the platform that owns a database connection (spec §4.4) -- a
function never does.

Scale on lag like everything else, but expect it to stay small: at ~17
files/sec and a microsecond write per record, throughput is not the
constraint.
"""

from __future__ import annotations

import logging
import os
import signal

from faas_sdk.clock import SystemClock
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.metrics import NullMetrics

from .config import SinkConfig
from .sink import SinkRunner
from .store import SqliteResultsStore

log = logging.getLogger(__name__)


def build_runner(
    config=None,
    *,
    consumer=None,
    producer=None,
    store=None,
    codec=None,
    metrics=None,
    clock=None,
    bootstrap_servers=None,
) -> SinkRunner:
    config = config or SinkConfig.from_env()
    clock = clock or SystemClock()
    metrics = metrics or NullMetrics()
    bootstrap_servers = bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")

    if codec is None:
        from faas_sdk.codec_protobuf import ProtobufCodec

        codec = ProtobufCodec()

    if store is None:
        store = SqliteResultsStore(config.db_path, clock=clock)

    if consumer is None:
        from faas_sdk.kafka import ConfluentConsumer

        consumer = ConfluentConsumer(config.consumer_config(bootstrap_servers))

    if producer is None:
        # The sink's only writes are DLQ records; the producer exists so the
        # DeadLetterQueue has something to send through.
        from faas_sdk.kafka import ConfluentProducer

        producer = ConfluentProducer(config.producer_config(bootstrap_servers))

    return SinkRunner(
        config=config,
        consumer=consumer,
        store=store,
        codec=codec,
        dlq=DeadLetterQueue(config=config, producer=producer, clock=clock),
        metrics=metrics,
        clock=clock,
    )


def main() -> None:
    logging.basicConfig(level=os.environ.get("FAAS_LOG_LEVEL", "INFO"))
    runner = build_runner()

    def _shutdown(signum, frame):
        log.info("signal %s: draining", signum)
        runner.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("landing %s -> %s", runner.config.results_topic, runner.config.db_path)
    runner.run()


if __name__ == "__main__":
    main()