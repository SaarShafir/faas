"""The write paths and the sandbox, and the gates in front of them.

These features change things outside the console or execute arbitrary Python,
so the tests worth having are less about the happy path than about the guards:
off by default, audited before the fact, and never writing to the branch
someone is working on.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("fastapi", reason="console is an optional extra")

from faas_console import actions, sandbox  # noqa: E402
from faas_console.payloads import render  # noqa: E402

# -- gates -----------------------------------------------------------------


def test_writes_are_off_unless_asked_for(monkeypatch):
    """A read-only console can be left open on a screen. This one cannot, so it
    is not on by accident."""
    monkeypatch.setattr(actions, "ALLOW_WRITES", False)
    with pytest.raises(actions.WritesDisabled, match="FAAS_CONSOLE_ALLOW_WRITES"):
        actions.pause_function("duration_rms")


def test_the_sandbox_is_off_unless_asked_for(monkeypatch):
    monkeypatch.setattr(sandbox, "ENABLED", False)
    with pytest.raises(sandbox.SandboxDisabled, match="arbitrary Python"):
        sandbox.run("class X: pass", "anything")


def test_an_unwritable_audit_log_blocks_the_action(monkeypatch, tmp_path):
    """An audit log that cannot be written must stop the action, not proceed
    unaudited -- otherwise the failure mode is silent and permanent."""
    monkeypatch.setattr(actions, "ALLOW_WRITES", True)
    monkeypatch.setattr(actions, "AUDIT_PATH", tmp_path / "nope" / "audit.jsonl")
    (tmp_path / "nope").write_text("not a directory")

    with pytest.raises(actions.WritesDisabled, match="audit"):
        actions.pause_function("duration_rms")


def test_every_action_is_audited_before_it_runs(monkeypatch, tmp_path):
    """Written before the fact, so an action that hangs or crashes still leaves
    a trace. A log that only records successes answers the least interesting
    question."""
    audit_path = tmp_path / "audit.jsonl"
    monkeypatch.setattr(actions, "ALLOW_WRITES", True)
    monkeypatch.setattr(actions, "AUDIT_PATH", audit_path)
    monkeypatch.setattr(actions, "REPO_DIR", tmp_path)

    # Fails -- there is no compose project here -- and must be audited anyway.
    actions.pause_function("duration_rms")

    (record,) = [json.loads(line) for line in audit_path.read_text().splitlines()]
    assert record["action"] == "pause"
    assert record["function_id"] == "duration_rms"
    assert "no auth" in record["actor"]


# -- edits go to git, never to a pod --------------------------------------


def test_an_edit_outside_the_repo_is_refused(monkeypatch, tmp_path):
    """`relative_path` comes off a form."""
    monkeypatch.setattr(actions, "ALLOW_WRITES", True)
    monkeypatch.setattr(actions, "AUDIT_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(actions, "REPO_DIR", tmp_path)

    result = actions.save_to_branch(relative_path="../../etc/passwd", content="x", message="nope")
    assert not result.ok
    assert "outside the repository" in result.message


def test_the_console_only_edits_files_that_exist(monkeypatch, tmp_path):
    """Creating a function from the console would mean the console decides what
    exists, which is the second source of truth §8 rules out."""
    monkeypatch.setattr(actions, "ALLOW_WRITES", True)
    monkeypatch.setattr(actions, "AUDIT_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(actions, "REPO_DIR", tmp_path)

    result = actions.save_to_branch(
        relative_path="functions/ghost/function.py", content="x", message="nope"
    )
    assert not result.ok
    assert "only edits" in result.message


def test_an_edit_lands_on_a_new_branch_and_leaves_the_original_checked_out(monkeypatch, tmp_path):
    """The property that keeps §8 true: the console is an editor, not a
    deployment mechanism, and it does not move the working branch under
    whoever is using it."""
    import subprocess

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=tmp_path, capture_output=True, text=True, check=True
        ).stdout.strip()

    git("init", "-b", "main")
    git("config", "user.email", "t@example.com")
    git("config", "user.name", "t")
    target = tmp_path / "functions" / "duration_rms"
    target.mkdir(parents=True)
    (target / "function.py").write_text("original\n")
    git("add", "-A")
    git("commit", "-m", "initial")

    monkeypatch.setattr(actions, "ALLOW_WRITES", True)
    monkeypatch.setattr(actions, "AUDIT_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(actions, "REPO_DIR", tmp_path)

    result = actions.save_to_branch(
        relative_path="functions/duration_rms/function.py",
        content="edited\n",
        message="Console edit",
        branch="console/test",
    )

    assert result.ok, result.detail
    assert git("rev-parse", "--abbrev-ref", "HEAD") == "main"
    # The working tree is untouched: the console commits with plumbing rather
    # than checking a branch out inside somebody's live checkout.
    assert (target / "function.py").read_text() == "original\n"
    assert "console/test" in git("branch", "--list", "console/test")
    assert git("show", "console/test:functions/duration_rms/function.py") == "edited"
    assert "Nothing is deployed" in result.detail


def test_an_edit_does_not_disturb_uncommitted_work(monkeypatch, tmp_path):
    """The reason this uses git plumbing rather than a checkout.

    Committing by checking out a branch runs inside somebody's live checkout:
    it moves HEAD underneath them, and a failure halfway through strands them
    on a branch they never asked for. This ran against a working tree with
    uncommitted changes during development, which is how the problem surfaced.
    """
    import subprocess

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=tmp_path, capture_output=True, text=True, check=True
        ).stdout.strip()

    git("init", "-b", "main")
    git("config", "user.email", "t@example.com")
    git("config", "user.name", "t")
    target = tmp_path / "functions" / "duration_rms"
    target.mkdir(parents=True)
    (target / "function.py").write_text("original\n")
    git("add", "-A")
    git("commit", "-m", "initial")

    # Uncommitted work, exactly as someone mid-change would have.
    (tmp_path / "scratch.txt").write_text("work in progress\n")
    (target / "function.py").write_text("locally modified\n")

    monkeypatch.setattr(actions, "ALLOW_WRITES", True)
    monkeypatch.setattr(actions, "AUDIT_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(actions, "REPO_DIR", tmp_path)

    result = actions.save_to_branch(
        relative_path="functions/duration_rms/function.py",
        content="from the console\n",
        message="Console edit",
        branch="console/edit",
    )

    assert result.ok, result.detail
    # Every local change survives exactly as it was.
    assert (target / "function.py").read_text() == "locally modified\n"
    assert (tmp_path / "scratch.txt").read_text() == "work in progress\n"
    assert git("rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert git("show", "console/edit:functions/duration_rms/function.py") == "from the console"


def test_saving_identical_content_is_refused(monkeypatch, tmp_path):
    """An empty commit is noise in a branch list and a PR nobody can review."""
    import subprocess

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=tmp_path, capture_output=True, text=True, check=True
        ).stdout.strip()

    git("init", "-b", "main")
    git("config", "user.email", "t@example.com")
    git("config", "user.name", "t")
    target = tmp_path / "functions" / "duration_rms"
    target.mkdir(parents=True)
    (target / "function.py").write_text("original\n")
    git("add", "-A")
    git("commit", "-m", "initial")

    monkeypatch.setattr(actions, "ALLOW_WRITES", True)
    monkeypatch.setattr(actions, "AUDIT_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(actions, "REPO_DIR", tmp_path)

    result = actions.save_to_branch(
        relative_path="functions/duration_rms/function.py",
        content="original\n",
        message="No-op",
    )

    assert not result.ok
    assert "nothing changed" in result.message


# -- payload rendering -----------------------------------------------------


def test_a_rendering_never_hides_the_raw_payload():
    """Additive, always. A visualisation that quietly disagreed with the data
    would be worse than none."""
    rendering = render("duration_rms", b'{"duration_seconds": 42.0, "dbfs": -20.0}')
    assert "42" in rendering.raw
    assert rendering.figures


def test_a_duration_disagreement_is_called_out_in_the_figures():
    rendering = render(
        "duration_rms",
        b'{"duration_seconds": 150.0, "reference_duration_seconds": 300.0, "dbfs": -20}',
    )
    labels = {f.label: f for f in rendering.figures}
    assert "Disagreement" in labels
    assert labels["Disagreement"].tone == "bad"


def test_clipping_is_toned_as_bad_and_clean_audio_as_good():
    clipped = render("clipping_detect", b'{"clipped": true, "clipped_sample_ratio": 0.7}')
    clean = render("clipping_detect", b'{"clipped": false, "clipped_sample_ratio": 0.0}')
    assert clipped.figures[0].tone == "bad"
    assert clean.figures[0].tone == "good"


def test_an_unknown_function_still_renders_its_numbers():
    """A function written next week must never be unviewable."""
    rendering = render("brand_new_thing", b'{"score": 0.5, "label": "ok", "flag": true}')
    labels = [f.label for f in rendering.figures]
    assert "score" in labels and "label" in labels and "flag" in labels


def test_a_payload_that_is_not_json_is_shown_rather_than_swallowed():
    rendering = render("duration_rms", b"\x00\x01 not json")
    assert rendering.headline == "Not JSON"
    assert rendering.raw


# -- the sandbox's contract ------------------------------------------------


def test_the_sandbox_rejects_audio_it_does_not_have(monkeypatch):
    monkeypatch.setattr(sandbox, "ENABLED", True)
    monkeypatch.setattr(sandbox, "available_audio", lambda: [])
    result = sandbox.run("class X: pass", "no-such-audio")
    assert not result.ok
    assert "no corpus audio" in result.error


def test_the_realtime_multiple_is_reported_against_the_floor():
    """§8's onboarding contract is >=25x. Worth knowing while editing rather
    than at the capacity review."""
    fast = sandbox.RunResult(ok=True, seconds=1.0, audio_seconds=300.0)
    slow = sandbox.RunResult(ok=True, seconds=30.0, audio_seconds=300.0)
    assert fast.realtime_multiple == 300
    assert slow.realtime_multiple == 10
    assert sandbox.RunResult(ok=True).realtime_multiple is None
