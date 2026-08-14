"""The write paths, and the rules they live under.

Everything here changes something outside the console, which is why it was left
out of the first version. Three rules apply to all of it:

  - **Off unless asked.** `FAAS_CONSOLE_ALLOW_WRITES=1`. A read-only console can
    be left open on a screen; this one cannot.
  - **Audited, always.** Every attempt appends to a JSONL log before it runs,
    including the ones that fail. "Who replayed that" and "who paused this at
    3am" must be answerable, and an audit record written only on success is an
    audit record that hides the interesting cases.
  - **Never a second source of truth.** Declaration and code edits go to a git
    branch, not to a running pod. §8's "one PR, zero infra tickets" survives
    literally: the console makes the PR. What is deployed still comes from a
    reviewed commit and an image built from it.

`pause` is the exception worth reading carefully -- see `pause_function`.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

ALLOW_WRITES = os.environ.get("FAAS_CONSOLE_ALLOW_WRITES", "").strip().lower() in (
    "1",
    "true",
    "yes",
)
AUDIT_PATH = Path(os.environ.get("FAAS_CONSOLE_AUDIT_LOG", "/runs/console-audit.jsonl"))
REPO_DIR = Path(os.environ.get("FAAS_REPO_DIR", "/repo"))
COMPOSE_PROJECT = os.environ.get("FAAS_COMPOSE_PROJECT", "faas-local")


class WritesDisabled(RuntimeError):
    pass


@dataclass
class ActionResult:
    ok: bool
    message: str
    detail: str = ""


def audit(action: str, **fields) -> None:
    """Append-only, before the fact.

    Written before the action runs rather than after, so an action that hangs
    or crashes still leaves a trace. A log that only records successes answers
    the least interesting question.
    """
    record = {
        "at": datetime.now(timezone.utc).isoformat(),
        "action": action,
        # There is no authentication yet, so there is no user to record. Saying
        # so explicitly is better than an empty field that looks like a bug --
        # and it is the reminder that this must not be exposed as-is.
        "actor": os.environ.get("FAAS_CONSOLE_ACTOR", "anonymous (console has no auth)"),
        **fields,
    }
    try:
        AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with AUDIT_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
    except OSError as exc:
        # An unwritable audit log must not silently allow unaudited actions.
        raise WritesDisabled(f"cannot write the audit log at {AUDIT_PATH}: {exc}") from exc


def _guard(action: str, **fields) -> None:
    if not ALLOW_WRITES:
        raise WritesDisabled(
            "writes are off. Replay, pause and edits change things outside the "
            "console, so they are opt-in: set FAAS_CONSOLE_ALLOW_WRITES=1. There "
            "is no authentication yet, so do not turn this on anywhere the "
            "console is reachable by anyone you would not hand a shell."
        )
    audit(action, **fields)


def recent_audit(limit: int = 50) -> list[dict]:
    if not AUDIT_PATH.exists():
        return []
    lines = AUDIT_PATH.read_text(encoding="utf-8").strip().splitlines()
    out = []
    for line in reversed(lines[-limit * 2 :]):
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
        if len(out) >= limit:
            break
    return out


# -- replay ----------------------------------------------------------------


def replay_dead_letter(
    *, bootstrap: str, dlq_topic: str, partition: int, offset: int, producer=None
) -> ActionResult:
    """Re-publish a dead letter's original input to the topic it came from.

    The DLQ stores the input message byte for byte precisely so this is
    possible (§5.4), and the headers carry the topic, partition and offset it
    came from.

    **What this does that surprises people.** Replaying to the internal topic
    re-runs *every* function for that call, not just the one that failed --
    there is no per-function input topic, that is the whole point of the fan-out
    in §4.2. The others will produce a second result for a call they already
    answered. Results are keyed by (call, function, version) so a compacted
    topic collapses them, but any consumer reading the topic directly sees both.
    """
    _guard("replay", dlq_topic=dlq_topic, partition=partition, offset=offset)

    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"faas-console-replay-{time.time_ns()}",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.assign([TopicPartition(dlq_topic, partition, offset)])
        message = None
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is not None and not message.error():
                break
            message = None
        if message is None:
            return ActionResult(False, f"no record at {dlq_topic}[{partition}]@{offset}")

        headers = {k: v for k, v in (message.headers() or ())}
        source_topic = headers.get("faas.source.topic", b"").decode()
        if not source_topic:
            return ActionResult(False, "record has no faas.source.topic header; cannot replay")
        body = message.value()
        key = message.key()
    finally:
        consumer.close()

    if producer is None:
        from confluent_kafka import Producer

        producer = Producer(
            {"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"}
        )

    producer.produce(topic=source_topic, key=key, value=body)
    remaining = producer.flush(15)
    if remaining:
        return ActionResult(False, "the replayed record never left the producer")

    return ActionResult(
        True,
        f"replayed to {source_topic}",
        detail=(
            f"{len(body or b'')} bytes. Every function consuming {source_topic} will "
            f"process this call again, not only the one that failed."
        ),
    )


# -- pause -----------------------------------------------------------------


def pause_function(function_id: str, *, resume: bool = False) -> ActionResult:
    """Stop or start a function's containers.

    **This is local-stack mechanics and does not generalise.** Kafka has no
    server-side "pause this group": a group is only paused in the sense that
    nobody is polling it. Here that means stopping the container, which needs
    the Docker socket mounted into the console -- a socket that is equivalent to
    root on the host, and the single strongest reason this console must not be
    exposed without authentication.

    On OpenShift the equivalent is `oc scale --replicas=0`, and the honest
    version of this feature there is a control that talks to the API server with
    a service account scoped to one namespace. Doing it properly is its own
    piece of work; this is the local approximation, labelled as such.

    Pausing is safe for the platform: the group's offsets stay committed, its
    lag grows, and no other function is affected -- which is the isolation
    guarantee doing exactly what it exists for.
    """
    action = "resume" if resume else "pause"
    _guard(action, function_id=function_id)

    service = function_id.replace("_", "-")
    command = ["docker", "compose", "-p", COMPOSE_PROJECT, "start" if resume else "stop", service]
    try:
        completed = subprocess.run(
            command, cwd=REPO_DIR / "compose", capture_output=True, text=True, timeout=60
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return ActionResult(False, f"could not {action} {service}", detail=str(exc))

    if completed.returncode != 0:
        return ActionResult(False, f"could not {action} {service}", detail=completed.stderr[-500:])

    return ActionResult(
        True,
        f"{action}d {service}",
        detail=(
            "Its lag will grow while every other function is unaffected -- the "
            "isolation guarantee working as intended."
            if not resume
            else "It will rejoin its group and work through the backlog."
        ),
    )


# -- edits -----------------------------------------------------------------


def save_to_branch(
    *, relative_path: str, content: str, message: str, branch: str | None = None
) -> ActionResult:
    """Write a file and commit it to a new branch. Never to the working branch.

    This is what keeps §8 true. The console is a nicer editor; it is not a
    deployment mechanism. Nothing here reaches a running pod -- the change has
    to be reviewed, merged and built into an image like any other, which is
    the property that makes a bad edit recoverable.
    """
    _guard("save", path=relative_path, branch=branch)

    target = (REPO_DIR / relative_path).resolve()
    try:
        # Refuse to write outside the repo. `relative_path` comes off a form.
        target.relative_to(REPO_DIR.resolve())
    except ValueError:
        return ActionResult(False, f"{relative_path} is outside the repository")

    if not target.exists():
        return ActionResult(False, f"{relative_path} does not exist; the console only edits")

    branch = branch or f"console/{Path(relative_path).parent.name}-{int(time.time())}"

    def git(*args, check=True):
        result = subprocess.run(
            ["git", *args], cwd=REPO_DIR, capture_output=True, text=True, timeout=60
        )
        if check and result.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)}: {result.stderr.strip()}")
        return result.stdout.strip()

    try:
        original = git("rev-parse", "--abbrev-ref", "HEAD")
        git("checkout", "-b", branch)
        target.write_text(content, encoding="utf-8")
        git("add", relative_path)
        git(
            "-c",
            "user.name=faas-console",
            "-c",
            "user.email=console@faas.local",
            "commit",
            "-m",
            message,
        )
        head = git("rev-parse", "--short", "HEAD")
        # Back to where the operator was, so the console never leaves the
        # working tree on a branch nobody asked for.
        git("checkout", original)
    except RuntimeError as exc:
        return ActionResult(False, "could not commit the change", detail=str(exc))

    return ActionResult(
        True,
        f"committed {head} to {branch}",
        detail=(
            "Nothing is deployed. Push the branch and open a PR -- the running "
            "pods keep executing the image they were built from until a merged "
            "change is built and rolled out."
        ),
    )
