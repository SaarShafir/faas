"""Generate Python from the .proto files.

`buf generate` (see proto/buf.gen.yaml) is the declared toolchain -- it is what
enforces lint and breaking-change rules in CI. This script is the fallback for
environments without the buf binary, using the protoc bundled in grpcio-tools.
Both produce the same output tree.

    python scripts/gen_proto.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "proto"
OUT_DIR = ROOT / "src"


def main() -> int:
    # protoc wants forward slashes even on Windows, or it cannot relativise the
    # path against --proto_path.
    protos = sorted(p.relative_to(PROTO_DIR).as_posix() for p in PROTO_DIR.rglob("*.proto"))
    if not protos:
        print("no .proto files found", file=sys.stderr)
        return 1

    if _have_buf():
        return subprocess.call(["buf", "generate"], cwd=PROTO_DIR)

    print("buf not found; falling back to protoc from grpcio-tools")
    OUT_DIR.mkdir(exist_ok=True)
    result = subprocess.call(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"--proto_path={PROTO_DIR}",
            f"--python_out={OUT_DIR}",
            f"--pyi_out={OUT_DIR}",
            *protos,
        ]
    )
    if result != 0:
        return result

    # protoc emits no package markers; without them the generated modules are
    # unimportable from an installed wheel.
    for package in (OUT_DIR / "faas", OUT_DIR / "faas" / "v1"):
        marker = package / "__init__.py"
        if not marker.exists():
            marker.write_text('"""Generated from proto/. Do not edit by hand."""\n')

    print(f"generated {len(protos)} file(s) into {OUT_DIR}")
    return 0


def _have_buf() -> bool:
    try:
        subprocess.run(["buf", "--version"], capture_output=True, check=True)
        return True
    except (OSError, subprocess.CalledProcessError):
        return False


if __name__ == "__main__":
    raise SystemExit(main())
