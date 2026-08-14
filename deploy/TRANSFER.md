# Transferring this into the closed network

## What is in the bundle

Everything tracked in git, plus `.git` itself. That is the source, the
declarations, the Helm chart, the Dockerfiles, the compose stack, the tests and
the docs — about 3 MB.

`.git` is included on purpose. It gives you provenance for what you are running,
and the console's "commit an edit to a branch" needs a real repository to write
into.

**Not included, and why:**

| | |
|---|---|
| `.venv/` | 202 MB of platform-specific wheels. Recreate with `pip install -e ".[dev,runtime,metrics,events,console]"`. |
| `corpus/` | 25 MB of generated audio for the local stack. Regenerate with `python -m stress.corpus --out corpus`, which needs ffmpeg. |
| `runs/` | Local stress-run artefacts. Nothing depends on them. |
| **Container images** | See below. This is the part that needs a decision. |

## The images are the hard part

Nothing in this bundle is a container image, and `docker build` in a closed
network usually fails three times over:

1. `FROM python:3.12-slim` — needs a registry that can serve it
2. `apt-get install libsndfile1 ffmpeg git` — needs a Debian mirror
3. `pip install` — needs a PyPI index

If the network has mirrors for all three, build inside it:

```bash
python scripts/build_images.py --registry <internal-registry>/faas --push
```

If it does not, build outside and carry the images in:

```bash
# outside
python scripts/build_images.py --registry <internal-registry>/faas
docker save $(python - <<'PY'
import subprocess, yaml, pathlib
for p in sorted(pathlib.Path("functions").glob("*/function.yaml")) + [pathlib.Path("hydrator.yaml")]:
    print(yaml.safe_load(p.read_text())["image"].split("/", 1)[-1])
PY
) -o faas-images.tar

# inside
docker load -i faas-images.tar
# then retag to the internal registry and push
```

The twelve images share every layer below the function itself, so the tar is far
smaller than twelve times one image — but it is still the large half of the
transfer, and it is worth checking whether the cluster's registry can mirror the
three upstreams instead.

**Do not skip `scripts/build_images.py` and build by hand.** It reads the image
name and tag out of each declaration, which is what keeps the tag in git and the
tag in the registry from drifting.

## Once it is inside

Follow [`AGENT_GUIDE.md`](AGENT_GUIDE.md). Its first step is
`python -m pytest` — that is also the fastest check that the transfer arrived
intact and the Python environment is usable.

## Two things the chart does not mount

Both degrade quietly rather than failing, so they are worth knowing before
someone reports them as bugs:

- **The console's sandbox has no corpus in-cluster.** `FAAS_CORPUS_DIR` is not
  mounted by the chart, so the audio list is empty and "run against this audio"
  has nothing to offer. It works in compose. Mounting a PVC with a generated
  corpus would fix it.
- **The console's editor cannot commit in-cluster.** Saving writes to a git
  checkout at `FAAS_REPO_DIR`, which the chart does not mount either. Editing
  and running still work; saving reports that the file does not exist. A
  git-sync sidecar or a PVC checkout would fix it.

Neither is wired up because both are decisions about giving a pod a writable
copy of the repository, which is not something to do by default.
