# Images for the local stack. One file, three targets, because the only real
# differences are which extras get installed and whether ffmpeg is present.
#
#   docker build --target function -t faas-function .
#   docker build --target hydrator -t faas-hydrator .
#   docker build --target harness  -t faas-harness  .
#
# The UID/GID handling is not local-stack theatre. §11's target is OpenShift,
# which assigns a random UID at run time and puts it in group 0 -- so anything
# the process writes has to be group-writable, and nothing may assume it can
# write to $HOME or to a path owned by a specific user. Getting that wrong shows
# up first in the hydrator, where ffmpeg writes scratch files per call. Building
# it in here means the local stack runs under the same constraint rather than
# discovering it during a deployment.

FROM python:3.12-slim AS base

# libsndfile is what soundfile binds to; there is no wheel that vendors it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    # Scratch space, group-writable, for a process whose UID is not known until
    # it starts. TMPDIR is what tempfile.TemporaryDirectory honours, which is
    # what the transcoder uses.
    TMPDIR=/scratch \
    PYTHONPATH=/app/src:/app

WORKDIR /app

# Dependencies first, so a change to a function does not reinstall the world.
COPY pyproject.toml README.md ./
COPY src/ ./src/
# `metrics` alongside `runtime`: the §5.5 set is emitted by every runner, and
# without the exporter installed FAAS_METRICS=prometheus degrades to NullMetrics
# and the pod is silently invisible to the dashboards.
RUN pip install --no-cache-dir ".[runtime,metrics]"

# `functions/` is deliberately not packaged: a function lives beside its
# declaration in the repo (§8), not inside the SDK distribution.
COPY functions/ ./functions/

RUN mkdir -p /scratch \
    && chgrp -R 0 /app /scratch \
    && chmod -R g=u /app /scratch

# 1001 is a placeholder. The point is the group: OpenShift overrides the UID
# and keeps GID 0, so every path above is already writable by whoever we become.
USER 1001:0


# -- a function -----------------------------------------------------------
#
# One image, ten services. FAAS_FUNCTION picks which function this container
# runs and FAAS_DECLARATION points at its function.yaml -- so adding a function
# is still one PR with no infrastructure work (§1, §8), and the local stack does
# not need ten near-identical Dockerfiles to prove it.

FROM base AS function

ENV FAAS_FUNCTION=duration_rms
CMD ["sh", "-c", "exec python -m functions.${FAAS_FUNCTION}.function"]


# -- the hydrator ---------------------------------------------------------

FROM base AS hydrator

USER root
# The whole input-format zoo is ffmpeg's problem, and keeping it here is what
# lets the function image depend on nothing heavier than libsndfile.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir ".[hydrator,metrics]"
USER 1001:0

COPY hydrator.yaml ./hydrator.yaml

ENV FAAS_DECLARATION=/app/hydrator.yaml
CMD ["faas-hydrator"]


# -- the stress harness ---------------------------------------------------
#
# Corpus generator, Audio API stub, seeder, monitor. Needs ffmpeg to build the
# corpus and requests to serve it; it is not a production image and never runs
# outside compose.

FROM hydrator AS harness

USER root
RUN pip install --no-cache-dir ".[hydrator,metrics]" boto3
USER 1001:0

COPY stress/ ./stress/

CMD ["python", "-m", "stress.audio_api"]
