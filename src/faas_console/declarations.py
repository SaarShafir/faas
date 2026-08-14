"""Loading function declarations from the repo.

§8 is emphatic that adding a function is one PR and zero infra tickets, which
makes `functions/*/function.yaml` in git the only source of truth for what a
function is. The console reads them and never writes them: a UI that could edit
a declaration would be a second source of truth, and the two would drift the
first time someone was in a hurry.

Everything here is therefore read-only by construction, and reuses
`FunctionConfig.from_yaml` so the console's idea of a declaration cannot drift
from the runner's either.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from faas_sdk.config import FunctionConfig

log = logging.getLogger(__name__)

FUNCTIONS_DIR = Path(os.environ.get("FAAS_FUNCTIONS_DIR", "functions"))
HYDRATOR_DECLARATION = Path(os.environ.get("FAAS_HYDRATOR_DECLARATION", "hydrator.yaml"))


def load_all(
    functions_dir: Path | None = None, hydrator: Path | None = None
) -> dict[str, FunctionConfig]:
    """Every declaration, keyed by function_id.

    A declaration that will not parse is skipped and logged rather than raised:
    the console's job is to show the state of the world, and refusing to render
    anything because one function.yaml is malformed is the opposite of that --
    especially since a malformed declaration is exactly what an operator would
    be trying to look at.
    """
    functions_dir = Path(functions_dir or FUNCTIONS_DIR)
    hydrator = Path(hydrator if hydrator is not None else HYDRATOR_DECLARATION)

    found: dict[str, FunctionConfig] = {}

    for path in sorted(functions_dir.glob("*/function.yaml")):
        try:
            config = FunctionConfig.from_yaml(path)
        except Exception as exc:  # noqa: BLE001 - any parse or validation error
            log.warning("skipping %s: %s", path, exc)
            continue
        found[config.function_id] = config

    if hydrator.exists():
        try:
            config = FunctionConfig.from_yaml(hydrator)
            found[config.function_id] = config
        except Exception as exc:  # noqa: BLE001
            log.warning("skipping %s: %s", hydrator, exc)

    return found


def source_path(function_id: str, functions_dir: Path | None = None) -> Path | None:
    """Where a declaration lives, so the console can name the file to edit
    rather than pretending it could edit it."""
    functions_dir = Path(functions_dir or FUNCTIONS_DIR)
    candidate = functions_dir / function_id / "function.yaml"
    return candidate if candidate.exists() else None


def source_code(function_id: str, functions_dir: Path | None = None) -> str:
    """A function's business logic, as it exists in git.

    Read fresh every time rather than cached: the whole point of the editor is
    that what you see is what the file says right now.
    """
    functions_dir = Path(functions_dir or FUNCTIONS_DIR)
    path = functions_dir / function_id / "function.py"
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def relative_source_path(function_id: str, kind: str = "function.py") -> str:
    """Repo-relative path, which is what a commit needs."""
    return f"functions/{function_id}/{kind}"
