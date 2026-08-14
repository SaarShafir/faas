"""Reading the console's data from event logs instead of Kafka partitions.

The second implementation of `ConsoleReader`, and the reason that interface
exists. Nothing in the views changes; the queries underneath do.

**What moves and what does not.** Call tracing and DLQ browsing move to log
queries, because they are per-call questions and a log store is built for
exactly that: filter by field, sort by time, no partition arithmetic. Fleet
status, topics and config lint stay on Kafka, and not by preference --
consumer lag is the difference between a committed offset and a high water
mark, which is broker state. No pod is in a position to emit it, so no log line
can carry it. Those calls delegate to the Kafka reader.

**What this buys over scanning partitions.** The Kafka reader answers "what
happened to this call" in two targeted partition reads, which is respectable
and was only possible because §6 partitions results on `call_id`. It cannot
answer "every call this tenant ran that failed", because that is not a
partition read at any partitioning. This can, and the §4.2 passthrough means
the tenant field is already there.

**What it gives up.** Log retention becomes the horizon. Kafka holds 48h and
the object store 24h, so if logs are kept for less than either, the console
starts forgetting calls that the platform can still tell you about.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any
from urllib import error, parse, request

from .models import CallTrace, DeadLetter, Reference, ResultView

log = logging.getLogger(__name__)

RECEIVED = "call.received"
COMPLETED = "call.completed"
FAILED = "call.failed"
RETRY_SCHEDULED = "call.retry_scheduled"
DEAD_LETTERED = "call.dead_lettered"


class OpenSearchConsoleReader:
    """Call questions from logs, broker questions from the broker."""

    def __init__(self, endpoint: str, *, index: str, kafka, timeout: float = 15.0):
        self.endpoint = endpoint.rstrip("/")
        self.index = index
        # Not a fallback: the delegate owns the questions logs structurally
        # cannot answer.
        self.kafka = kafka
        self.timeout = timeout

    # -- delegated: broker state -------------------------------------------

    def fleet(self):
        return self.kafka.fleet()

    def topics(self):
        return self.kafka.topics()

    def lint(self):
        return self.kafka.lint()

    @property
    def declarations(self):
        return self.kafka.declarations

    @declarations.setter
    def declarations(self, value):
        self.kafka.declarations = value

    # -- queries -----------------------------------------------------------

    def _search(self, body: dict) -> list[dict]:
        url = f"{self.endpoint}/{parse.quote(self.index)}/_search"
        payload = json.dumps(body).encode()
        req = request.Request(
            url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                data = json.load(response)
        except error.HTTPError as exc:
            # An index that does not exist yet is the normal state of a fresh
            # stack, not an error worth a stack trace.
            if exc.code == 404:
                return []
            log.warning("opensearch %s: %s", exc.code, exc.reason)
            return []
        except OSError as exc:
            log.warning("opensearch unreachable: %s", exc)
            return []
        return [hit.get("_source", {}) for hit in data.get("hits", {}).get("hits", [])]

    @staticmethod
    def _attributes(source: dict) -> dict:
        """Flatten the record into the fields the SDK emitted.

        `event.name` arrives as a nested object because the key contains a dot,
        so it is lifted back to a flat key here rather than every caller
        remembering which fields are nested.
        """
        attributes = dict(source.get("attributes", {}))

        # `event.name` has to be handled both ways, and the asymmetry is the
        # trap. OpenSearch expands a dotted field name into an object *in the
        # mapping* -- so a query must use the path `attributes.event.name` --
        # while `_source` is returned exactly as it was indexed, with the key
        # still flat and still containing a dot. Reading only the nested form
        # finds nothing, silently: the query matches, the events come back, and
        # every one of them parses as an unknown event type.
        name = attributes.pop("event.name", None)
        if name is None:
            event = attributes.pop("event", None)
            if isinstance(event, dict):
                name = event.get("name")
        if name is not None:
            attributes["event_name"] = name

        attributes["@timestamp"] = source.get("@timestamp", "")
        return attributes

    # -- call tracing ------------------------------------------------------

    def find_call(self, call_id: str) -> CallTrace:
        started = time.monotonic()
        # `.keyword` rather than the analysed text field: a call id contains
        # hyphens, which the standard analyser splits into tokens, so a match
        # on the text field would happily return every call sharing a segment.
        hits = self._search(
            {
                "size": 500,
                "sort": [{"@timestamp": "asc"}],
                "query": {"term": {"attributes.call_id.keyword": call_id}},
            }
        )
        events = [self._attributes(hit) for hit in hits]

        results: dict[tuple, ResultView] = {}
        dead: list[DeadLetter] = []
        reference: Reference | None = None
        answered = set()

        for event in events:
            name = event.get("event_name", "")
            function_id = event.get("function_id", "")

            if name == RECEIVED and reference is None:
                reference = Reference(
                    call_id=call_id,
                    object_key=event.get("object_key", ""),
                    sample_rate=int(event.get("sample_rate", 16000) or 16000),
                    channels=1,
                    duration_seconds=float(event.get("duration_seconds", 0) or 0),
                    ingested_at=None,
                    hydrated_at=None,
                    partition=int(event.get("partition", -1) or -1),
                    offset=int(event.get("offset", -1) or -1),
                )

            elif name == COMPLETED:
                answered.add(function_id)
                payload = event.get("payload")
                view = ResultView(
                    function_id=function_id,
                    function_version=event.get("function_version", ""),
                    status=event.get("status", ""),
                    attempt=int(event.get("attempt", 1) or 1),
                    payload=payload.encode() if isinstance(payload, str) else payload,
                    payload_ref=event.get("payload_ref"),
                    payload_content_type=event.get("payload_content_type", ""),
                    ingested_at=None,
                    started_at=None,
                    completed_at=None,
                    partition=int(event.get("partition", -1) or -1),
                    offset=int(event.get("offset", -1) or -1),
                )
                # Keyed like the §6 record key, so a redelivered call shows the
                # function once rather than once per delivery.
                results[(view.function_id, view.function_version)] = view

            elif name == DEAD_LETTERED:
                answered.add(function_id)
                dead.append(_dead_letter(event))

        expected = {f for f in self.kafka.declarations if f != "hydrator"}
        return CallTrace(
            call_id=call_id,
            reference=reference,
            results=sorted(results.values(), key=lambda r: r.function_id),
            dead_letters=dead,
            missing=sorted(expected - answered) if reference else [],
            partitions_scanned=[f"{self.index} (log query, {len(events)} events)"],
            scan_seconds=round(time.monotonic() - started, 3),
        )

    # -- dead letters ------------------------------------------------------

    def dead_letters(self, topic: str | None = None, limit: int = 50) -> list[DeadLetter]:
        must: list[dict[str, Any]] = [{"term": {"attributes.event.name.keyword": DEAD_LETTERED}}]
        if topic:
            must.append({"term": {"attributes.dlq_topic.keyword": topic}})

        hits = self._search(
            {
                "size": limit,
                "sort": [{"@timestamp": "desc"}],
                "query": {"bool": {"must": must}},
            }
        )
        return [_dead_letter(self._attributes(hit)) for hit in hits]


def _dead_letter(event: dict) -> DeadLetter:
    return DeadLetter(
        topic=event.get("dlq_topic", ""),
        function_id=event.get("function_id", ""),
        function_version=event.get("function_version", ""),
        group_id=f"{event.get('function_id', '')}:{event.get('function_version', '')}",
        error_code=event.get("error_code", "?"),
        error_message=event.get("error_message", ""),
        retryable=bool(event.get("retryable", False)),
        attempt=int(event.get("attempt", -1) or -1),
        call_id=event.get("call_id", ""),
        source_topic=event.get("source_topic", ""),
        source_partition=int(event.get("source_partition", -1) or -1),
        source_offset=int(event.get("source_offset", -1) or -1),
        failed_at=event.get("@timestamp", ""),
        body_bytes=0,
    )
