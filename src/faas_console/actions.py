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
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

log = logging.getLogger(__name__)

ALLOW_WRITES = os.environ.get("FAAS_CONSOLE_ALLOW_WRITES", "").strip().lower() in (
    "1",
    "true",
    "yes",
)
AUDIT_PATH = Path(os.environ.get("FAAS_CONSOLE_AUDIT_LOG", "/runs/console-audit.jsonl"))
REPO_DIR = Path(os.environ.get("FAAS_REPO_DIR", "/repo"))
COMPOSE_PROJECT = os.environ.get("FAAS_COMPOSE_PROJECT", "faas-local")

# Set by the chart. Its presence is what says "this is a cluster, scale the
# Deployment" rather than "this is compose, stop the container".
K8S_NAMESPACE = os.environ.get("FAAS_K8S_NAMESPACE", "")
K8S_API = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
K8S_PORT = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
# Where a paused function's previous replica count is kept, so resume restores
# what was there rather than guessing 1. An annotation rather than memory
# because a console restart in between must not strand a function at zero.
PAUSED_FROM = "faas.io/paused-from"
DOCKER_SOCKET = os.environ.get("FAAS_DOCKER_SOCKET", "/var/run/docker.sock")


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


def _docker(method: str, path: str):
    """One request to the Docker Engine API over its UNIX socket.

    http.client speaks HTTP over any socket; only the connection differs.
    """
    import http.client
    import socket as socket_module

    if not hasattr(socket_module, "AF_UNIX"):
        # Windows has no UNIX sockets. The console runs in a Linux container in
        # the stack, so this only bites when it is run directly on a Windows
        # host -- as an OSError rather than an AttributeError, so the caller's
        # "cannot reach the Docker socket" path handles it like any other.
        raise OSError("this platform has no UNIX sockets, so the Docker socket is unreachable")

    class UnixConnection(http.client.HTTPConnection):
        def connect(self):
            self.sock = socket_module.socket(socket_module.AF_UNIX, socket_module.SOCK_STREAM)
            self.sock.settimeout(30)
            self.sock.connect(DOCKER_SOCKET)

    connection = UnixConnection("localhost", timeout=30)
    try:
        connection.request(method, path)
        response = connection.getresponse()
        body = response.read()
        if response.status >= 400:
            detail = body[:200].decode(errors="replace")
            raise OSError(f"docker returned {response.status}: {detail}")
        return json.loads(body) if body.strip().startswith(b"[") else None
    finally:
        connection.close()


def _k8s(method: str, path: str, body: dict | None = None, content_type: str = ""):
    """One request to the API server with the pod's ServiceAccount token."""
    import http.client
    import ssl

    with open(f"{SA_DIR}/token", encoding="utf-8") as handle:
        token = handle.read().strip()

    context = ssl.create_default_context(cafile=f"{SA_DIR}/ca.crt")
    connection = http.client.HTTPSConnection(K8S_API, int(K8S_PORT), context=context, timeout=30)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = None
    if body is not None:
        payload = json.dumps(body).encode()
        headers["Content-Type"] = content_type or "application/merge-patch+json"

    try:
        connection.request(method, path, body=payload, headers=headers)
        response = connection.getresponse()
        raw = response.read()
        if response.status >= 400:
            detail = raw[:300].decode(errors="replace")
            raise OSError(f"api server returned {response.status}: {detail}")
        return json.loads(raw) if raw.strip() else None
    finally:
        connection.close()


def _scale_deployment(function_id: str, *, resume: bool) -> ActionResult:
    """Pause or resume by scaling this function's Deployment.

    Pausing records the current replica count in an annotation and sets
    replicas to zero in the same merge-patch, so the two cannot get out of step
    and a console restart between pause and resume loses nothing.
    """
    name = f"faas-{function_id.replace('_', '-')}"
    path = f"/apis/apps/v1/namespaces/{K8S_NAMESPACE}/deployments/{name}"

    try:
        deployment = _k8s("GET", path)
    except OSError as exc:
        return ActionResult(False, f"could not read {name}", detail=str(exc))
    except FileNotFoundError:
        return ActionResult(
            False,
            "no ServiceAccount token",
            detail=(
                f"{SA_DIR}/token is missing, so this is not running in a pod. "
                "Unset FAAS_K8S_NAMESPACE to use the Docker path instead."
            ),
        )

    annotations = (deployment.get("metadata") or {}).get("annotations") or {}
    current = (deployment.get("spec") or {}).get("replicas", 1)

    if resume:
        # Restore what it was, not a guess. If the annotation is missing --
        # paused by something other than the console -- one replica is the
        # least surprising fallback, and the chart's value takes over on the
        # next upgrade anyway.
        target = int(annotations.get(PAUSED_FROM) or 1) or 1
        patch = {"metadata": {"annotations": {PAUSED_FROM: None}}, "spec": {"replicas": target}}
    else:
        if current == 0:
            return ActionResult(False, f"{name} is already paused")
        target = 0
        patch = {
            "metadata": {"annotations": {PAUSED_FROM: str(current)}},
            "spec": {"replicas": 0},
        }

    try:
        _k8s("PATCH", path, patch)
    except OSError as exc:
        return ActionResult(False, f"could not scale {name}", detail=str(exc))

    if resume:
        return ActionResult(
            True,
            f"resumed {function_id} to {target} replica(s)",
            detail="It will rejoin its group and work through the backlog.",
        )
    return ActionResult(
        True,
        f"paused {function_id} (was {current} replica(s))",
        detail=(
            "Its lag will grow while every other function is unaffected -- the "
            "isolation guarantee working as intended. Resume restores the same "
            "replica count."
        ),
    )


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
    nobody is polling it. Here that means stopping the container, over the
    Docker socket -- a socket equivalent to root on the host, and the single
    strongest reason this console must not be exposed without authentication.

    The API is spoken directly rather than through the CLI: `docker.io` does
    not ship the compose v2 plugin, so `docker compose stop` is not available
    inside a container that only has the daemon package, and a stopped
    container is one HTTP POST anyway.

    On OpenShift there is no Docker socket, so pausing scales the Deployment to
    zero through the API server instead, using the pod's own ServiceAccount
    token and a Role that permits nothing else in one namespace. Which path is
    taken is decided by `FAAS_K8S_NAMESPACE`, set by the chart.

    Pausing is safe for the platform either way: the group's offsets stay
    committed, its lag grows, and no other function is affected -- which is the
    isolation guarantee doing exactly what it exists for.
    """
    action = "resume" if resume else "pause"
    _guard(action, function_id=function_id, target=K8S_NAMESPACE or COMPOSE_PROJECT)

    if K8S_NAMESPACE:
        return _scale_deployment(function_id, resume=resume)

    service = function_id.replace("_", "-")
    try:
        # Quoted: the filter is JSON, JSON has spaces, and a raw space in a
        # request line is rejected before it reaches Docker.
        filters = quote(
            json.dumps(
                {
                    "label": [
                        f"com.docker.compose.project={COMPOSE_PROJECT}",
                        f"com.docker.compose.service={service}",
                    ]
                },
                separators=(",", ":"),
            )
        )
        containers = _docker("GET", f"/containers/json?all=1&filters={filters}")
    except OSError as exc:
        return ActionResult(
            False,
            "cannot reach the Docker socket",
            detail=f"{exc}. Pause needs /var/run/docker.sock mounted into the console.",
        )

    if not containers:
        return ActionResult(False, f"no container for service {service}")

    for container in containers:
        try:
            _docker("POST", f"/containers/{container['Id']}/{'start' if resume else 'stop'}")
        except OSError as exc:
            return ActionResult(False, f"could not {action} {service}", detail=str(exc))

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
    """Commit an edit to a new branch without touching the working tree.

    This is what keeps §8 true. The console is a nicer editor; it is not a
    deployment mechanism. Nothing here reaches a running pod -- the change has
    to be reviewed, merged and built into an image like any other, which is
    the property that makes a bad edit recoverable.

    **It uses plumbing rather than checkout, and that is the important part.**
    The obvious implementation -- `checkout -b`, write the file, commit, check
    the original branch back out -- runs inside somebody's live checkout. If
    they have uncommitted work, the console is now moving HEAD underneath them,
    and a failure halfway through leaves them on a branch they never asked for.

    So: hash the content into a blob, build a tree from HEAD's with that one
    path replaced, commit that tree, and point a new ref at it. HEAD does not
    move, the index is a throwaway file, and the file on disk is untouched --
    what you edited in the browser is on a branch, and your working tree is
    exactly as you left it.
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

    def git(*args, stdin: str | None = None, env: dict | None = None) -> str:
        result = subprocess.run(
            [
                "git",
                # The repo arrives as a bind mount owned by the host user while
                # this process runs as another, which git refuses by default
                # ("dubious ownership"). That check exists to stop someone
                # else's repo config being executed; a mount we were handed
                # deliberately is the case it is not aimed at.
                "-c",
                f"safe.directory={REPO_DIR}",
                *args,
            ],
            cwd=REPO_DIR,
            input=stdin,
            capture_output=True,
            text=True,
            timeout=60,
            env={**os.environ, **(env or {})},
        )
        if result.returncode != 0:
            raise RuntimeError(f"git {args[0]}: {result.stderr.strip()}")
        return result.stdout.strip()

    try:
        head = git("rev-parse", "HEAD")
        # `--path` matters: without it the content skips the clean filters git
        # would normally apply, and this repo has .gitattributes rules forcing
        # LF on exactly these files. The blob then differs from what `git add`
        # would produce, so an unchanged file looks changed and every save
        # commits a spurious line-ending diff.
        blob = git("hash-object", "-w", "--path", relative_path, "--stdin", stdin=content)

        # A scratch index, so the real one is never read or written.
        with tempfile.TemporaryDirectory(prefix="faas-console-git-") as scratch:
            index = {"GIT_INDEX_FILE": str(Path(scratch) / "index")}
            git("read-tree", head, env=index)
            git(
                "update-index",
                "--add",
                "--cacheinfo",
                f"100644,{blob},{relative_path}",
                env=index,
            )
            tree = git("write-tree", env=index)

        if tree == git("rev-parse", f"{head}^{{tree}}"):
            return ActionResult(False, "nothing changed; the content is identical to HEAD")

        commit = git(
            "-c",
            "user.name=faas-console",
            "-c",
            "user.email=console@faas.local",
            "commit-tree",
            tree,
            "-p",
            head,
            "-m",
            message,
        )
        git("update-ref", f"refs/heads/{branch}", commit)
    except RuntimeError as exc:
        return ActionResult(False, "could not commit the change", detail=str(exc))

    return ActionResult(
        True,
        f"committed {commit[:9]} to {branch}",
        detail=(
            "Your working tree and current branch are untouched. Nothing is "
            "deployed either: push the branch and open a PR -- the running pods "
            "keep executing the image they were built from until a merged change "
            "is built and rolled out."
        ),
    )
