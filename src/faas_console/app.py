"""The console: a read-only window onto a running platform.

Deliberately read-only. There is no button here that changes anything, and that
is a design position rather than an unfinished one -- §8's "one PR, zero infra
tickets" only holds while git is the single source of truth for what a function
is, and every genuinely runtime action (replay, pause, backfill, erasure) needs
authentication, authorisation and an audit trail before it is safe to expose.
Those come later, on purpose.

What is here is the half Grafana is bad at: one call's whole life, the DLQ's
contents rather than its rate, which functions exist, and whether the
declarations agree with the topics they are pointed at. Time series stay in
Grafana, which this links to rather than reimplements.

    uvicorn faas_console.app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import logging
import os

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import declarations as declarations_module
from .opensearch_reader import OpenSearchConsoleReader
from .reader import KafkaConsoleReader

log = logging.getLogger(__name__)

HERE = os.path.dirname(__file__)
GRAFANA_URL = os.environ.get("FAAS_GRAFANA_URL", "http://localhost:3000")

# Where call-level questions are answered from. Kafka is the fallback and works
# with no extra infrastructure; logs answer the same questions plus the ones
# partition reads structurally cannot ("every call this tenant failed").
EVENTS_URL = os.environ.get("FAAS_EVENTS_URL", "")
EVENTS_INDEX = os.environ.get("FAAS_EVENTS_INDEX", "ss4o_logs-faas-local")

app = FastAPI(title="FaaS console", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=os.path.join(HERE, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(HERE, "templates"))

_reader: KafkaConsoleReader | None = None


def get_reader() -> KafkaConsoleReader:
    """Built lazily and kept.

    Declarations are re-read every time rather than cached: they come from a
    bind-mounted git checkout, and an operator who has just edited one expects
    the console to agree with the file on disk.
    """
    global _reader
    if _reader is None:
        kafka = KafkaConsoleReader(
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            declarations=declarations_module.load_all(),
        )
        # Fleet, topics and lint stay on Kafka either way: consumer lag is the
        # gap between a committed offset and a high water mark, which is broker
        # state that no pod can emit and therefore no log line can carry.
        _reader = (
            OpenSearchConsoleReader(EVENTS_URL, index=EVENTS_INDEX, kafka=kafka)
            if EVENTS_URL
            else kafka
        )
        log.info("reading calls from %s", "logs" if EVENTS_URL else "kafka")
    else:
        _reader.declarations = declarations_module.load_all()
    return _reader


def _page(request: Request, template: str, **context):
    return templates.TemplateResponse(
        request=request,
        name=template,
        context={"grafana": GRAFANA_URL, **context},
    )


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def overview(request: Request):
    reader = get_reader()
    try:
        groups = reader.fleet()
    except Exception as exc:  # noqa: BLE001 - a broker that is down is a thing to show
        log.warning("fleet unavailable: %s", exc)
        groups = []

    findings = reader.lint()
    declared = reader.declarations

    # A group with no declaration, or a declaration with no group, is the kind
    # of drift that only shows up when you put the two lists side by side.
    group_ids = {g.function_id for g in groups}
    undeclared = sorted(group_ids - set(declared))
    not_running = sorted(set(declared) - group_ids)

    return _page(
        request,
        "overview.html",
        groups=groups,
        findings=findings,
        declarations=declared,
        undeclared=undeclared,
        not_running=not_running,
    )


@app.get("/call", response_class=HTMLResponse)
def call(request: Request, call_id: str = Query("", alias="call_id")):
    call_id = call_id.strip()
    trace = get_reader().find_call(call_id) if call_id else None
    return _page(request, "call.html", call_id=call_id, trace=trace)


@app.get("/dlq", response_class=HTMLResponse)
def dlq(request: Request, topic: str = "", limit: int = 100):
    reader = get_reader()
    topics = [t.name for t in reader.topics() if ".dlq." in t.name]
    records = reader.dead_letters(topic=topic or None, limit=limit)

    by_code: dict[str, int] = {}
    for record in records:
        by_code[record.error_code] = by_code.get(record.error_code, 0) + 1

    return _page(
        request,
        "dlq.html",
        records=records,
        topics=topics,
        selected=topic,
        by_code=sorted(by_code.items(), key=lambda kv: -kv[1]),
    )


@app.get("/registry", response_class=HTMLResponse)
def registry(request: Request):
    reader = get_reader()
    try:
        groups = {g.function_id: g for g in reader.fleet()}
    except Exception:  # noqa: BLE001
        groups = {}

    rows = []
    for function_id, config in sorted(reader.declarations.items()):
        rows.append(
            {
                "config": config,
                "group": groups.get(function_id),
                "path": declarations_module.source_path(function_id),
            }
        )
    return _page(request, "registry.html", rows=rows, topics=reader.topics())


@app.get("/api/call/{call_id}")
def api_call(call_id: str):
    """The trace as JSON.

    Here because a support question is often "paste me the trace", and because
    it makes the console scriptable without anyone parsing HTML.
    """
    trace = get_reader().find_call(call_id)
    payload = _jsonable(
        {
            "call_id": trace.call_id,
            "hydrated": trace.hydrated,
            "complete": trace.complete,
            "scan_seconds": trace.scan_seconds,
            "partitions_scanned": trace.partitions_scanned,
            "duration_disagreement": trace.duration_disagreement,
            "reference": vars(trace.reference) if trace.reference else None,
            "results": [
                {
                    **{k: v for k, v in vars(r).items() if k != "payload"},
                    "payload": r.payload.decode("utf-8", "replace") if r.payload else None,
                }
                for r in trace.results
            ],
            "dead_letters": [vars(d) for d in trace.dead_letters],
            "missing": trace.missing,
        }
    )
    return Response(content=payload, media_type="application/json")


def _jsonable(data) -> str:
    """Timestamps are datetimes and payloads can be arbitrary bytes, neither of
    which json handles. `default=str` renders them rather than 500ing, which is
    the right trade for an endpoint whose job is to show what is there."""
    import json

    return json.dumps(data, indent=2, default=str)


@app.get("/api/lint")
def api_lint():
    """Non-empty means the stack is misconfigured. Suitable for a smoke test."""
    findings = get_reader().lint()
    return {
        "ok": not any(f.severity == "error" for f in findings),
        "findings": [vars(f) for f in findings],
    }
