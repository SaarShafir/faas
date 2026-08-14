"""Build the images the declarations name.

The tags are not passed in. They are read out of `functions/*/function.yaml`,
because §8 makes the declaration the source of truth for what a function is,
and an image tag typed separately on a command line is a second place for that
truth to live -- one that drifts silently the first time someone bumps a version
in git and forgets the build, or the reverse.

    python scripts/build_images.py --registry registry.example.com/faas --push

Each function gets its own image, per the declarations. The layers before the
function differ only in one build argument, so the base, the dependencies and
the source are shared across all of them in the local cache and in the registry.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def declarations() -> list[tuple[str, str, str]]:
    """(function_id, image, target) for everything that gets an image."""
    import yaml

    found = []
    for path in sorted((ROOT / "functions").glob("*/function.yaml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not data.get("image"):
            raise SystemExit(f"{path} has no image; §8 requires one")
        found.append((data["function_id"], data["image"], "function"))

    hydrator = ROOT / "hydrator.yaml"
    if hydrator.exists():
        data = yaml.safe_load(hydrator.read_text(encoding="utf-8")) or {}
        found.append((data["function_id"], data["image"], "hydrator"))

    return found


def retag(image: str, registry: str | None) -> str:
    """Point a declared image at a real registry.

    The declarations say `registry/faas-duration-rms:1.0.0`, where `registry`
    is a placeholder rather than a host. Replacing the first path segment keeps
    the name and the tag -- which are the parts under review -- and changes only
    where it is pushed.
    """
    if not registry:
        return image
    return f"{registry.rstrip('/')}/{image.split('/', 1)[-1]}"


def build(
    *, registry: str | None, push: bool, only: list[str], dry_run: bool
) -> int:
    targets = [d for d in declarations() if not only or d[0] in only]
    if not targets:
        raise SystemExit(f"no declarations matched {only}")

    for function_id, declared, stage in targets:
        image = retag(declared, registry)
        command = [
            "docker",
            "build",
            "-f",
            str(ROOT / "docker" / "Dockerfile"),
            "--target",
            stage,
            "--build-arg",
            f"FUNCTION={function_id}",
            "-t",
            image,
            str(ROOT),
        ]
        print(f"\n=== {function_id} -> {image}")
        if dry_run:
            print("  " + " ".join(command))
            continue
        if subprocess.call(command) != 0:
            return 1

        if push:
            if subprocess.call(["docker", "push", image]) != 0:
                return 1

    print(f"\n{len(targets)} image(s) {'pushed' if push else 'built'}")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry",
        default="",
        help="replaces the placeholder host in the declared image name",
    )
    parser.add_argument("--push", action="store_true")
    parser.add_argument(
        "--only",
        nargs="*",
        default=[],
        help="function ids to build; default is every declaration",
    )
    parser.add_argument("--dry-run", action="store_true", help="print the commands only")
    args = parser.parse_args(argv)

    return build(
        registry=args.registry, push=args.push, only=args.only, dry_run=args.dry_run
    )


if __name__ == "__main__":
    sys.exit(main())
