# FaaS — build status

Working notes against [`faas-spec.md`](faas-spec.md). [`README.md`](README.md)
explains how the thing works; this is what is done, what was decided, and what
comes next.

Last updated: 2026-08-14.

---

## Where we are in the build order (§13)

| # | Step | State |
|---|---|---|
| 1 | SDK — poll/work decoupling, in-flight pool, low-water-mark commits, DLQ, metrics | **done** |
| 2 | Protobuf schemas — `AudioReference`, `Result`, Buf setup | **done** |
| 3 | Hydrator | **done** |
| 4 | One trivial reference function, end to end | **done** |
| — | Ten functions on a local stack that mimics production | **done** |
| — | Metrics, dashboards and a read-only console | **done** |
| — | Helm chart and images for OpenShift | **done** |
| 5 | Results sink service | not started |
| 6 | Autoscaling on lag | not started |
| 7 | Aggregator + `call_complete` | not started |
| 8 | Deletion path | not started |

The spec's gate — "do not build functions 2..N until step 4 is stable" — is met:
`tests/functions/test_contract.py` runs a call end to end through a fake Audio
API serving real encoded audio, a real object-store round trip, real
libsndfile decoding and the real protobuf wire format.

**The hydrator no longer transcodes.** The Audio API serves canonical FLAC
already, so `Transcoder` and its ffmpeg subprocess are gone; the hydrator
fetches, checks the STREAMINFO header against canonical form, and stores the
bytes unchanged. ffmpeg survives only where it always played a second role: as
the thing that *makes* test input and the stress corpus, standing in for
whatever encodes calls upstream in production.

## Tests

319 total. The unit suite runs in ~25s and is the default; the broker suite
needs Docker and runs in ~4min.

```bash
pytest                 # 307 unit tests
pytest -m kafka        # 12 broker tests, needs Docker
```

| Area | Tests |
|---|---|
| SDK core (offsets, pool, runner, failure handling, results, config, codecs) | 115 |
| Hydrator (flac, metadata, hydrator, pipeline) | 48 |
| Functions (ten of them) + contract | 43 |
| Object store and Audio API stub | 12 |
| Broker (poll interval, commits, partitioner) | 12 |

The function tests run against the generated corpus, so they need it present:
`python -m stress.corpus --out corpus`. They skip rather than fail without it.

## Environment

Everything below is installed and working on this machine.

- `.venv` — pytest, ruff, PyYAML, protobuf, grpcio-tools, soundfile, numpy,
  confluent-kafka, testcontainers (unused, see below).
- **ffmpeg 9.0** via winget (`Gyan.FFmpeg`). On PATH for new shells; older shells
  need a restart. Nothing in the platform itself uses it any more, but the
  contract test and the stress corpus need it to make input, and skip rather
  than fail without it.
- **buf 1.72** on PATH. `buf lint` and `buf breaking` both run; `scripts/gen_proto.py`
  now takes the `buf generate` path, which is why the generated files carry
  managed-mode options the earlier protoc fallback did not emit.
- **Docker** required only for `pytest -m kafka`. Pulls `apache/kafka:latest`.
- Source is 3.10-compatible although `requires-python` says `>=3.12`, so the
  suite runs on whatever interpreter is to hand. Local Python is 3.10.

---

## The local stack, and what the stress run found

`compose/` brings up the production topology minus the parts that do not exist
yet: Kafka, MinIO, an Audio API stub serving a generated corpus, two hydrators
and ten functions, each its own consumer group with its own DLQ.

```bash
docker compose --profile stack up -d --build
docker compose --profile seed  run --rm seeder  --calls 300 --rate 25
docker compose --profile watch run --rm monitor --idle-seconds 60
python -m stress.chaos --service spectral-centroid --signal SIGKILL
```

Run of 2026-08-14: 300 calls, 19,140 seconds of audio, seeded at 25/s, with one
function SIGKILLed and a hydrator SIGTERMed mid-flight.

| | |
|---|---|
| Results | 2,800 across 10 functions, 0 lost |
| Duplicates | 1, in the SIGKILLed function — correct at-least-once redelivery |
| Realtime multiple | 76–130x per file for the eight analyzers (§8 floor is 25) |
| Hydration failures | 26, all deliberate: 12 `AUDIO_NOT_FOUND`, 14 `TRANSCODE_FAILED` |
| Final lag | 0 on nine functions, 309 on `slow_burner` — isolation holds |

### Bugs the stress run found

1. **A per-file timeout crash-looped the pod.** The headline. `_enforce_deadlines`
   dropped the job from `_in_flight` and called `pool.cancel`, but
   `ProcessWorkerPool` cannot interrupt a worker already inside `process()` — it
   abandons the *outcome* and the slot stays occupied. The runner then saw a
   free slot, resumed its partitions, polled the next file and submitted into a
   full pool, which raised `RuntimeError: pool at capacity` out of `run_once` and
   killed the process — dropping every other in-flight file and leaving queued
   results undelivered. `slow_burner` completed 2 of 100 files before the fix.

   The whole unit suite missed it because `ManualPool.cancel` frees the slot it
   is asked to cancel and the real pool cannot. Capacity decisions now consult
   the pool's own occupancy, and work that does not fit is deferred rather than
   submitted. `tests/test_timeout_capacity.py` models the real semantics in
   `StubbornPool`; four of its seven tests fail without the fix.

2. **A named volume defeated the group-writable image.** Corpus generation died
   with `Permission denied`: Docker creates a named volume root-owned at 755,
   and the image runs as a non-root user in group 0. This is precisely the §11
   random-UID problem — a PVC in an OpenShift pod behaves the same way unless
   the pod sets `fsGroup`. The local stack uses a bind mount; **the deployment
   manifests, when they get written, will need `fsGroup: 0` for any writable
   mount.**

3. **A truncated FLAC hydrates successfully.** Written into the corpus expecting
   a decode failure; it is not one. The header survives a mid-file cut and still
   claims the original duration, so a half-uploaded object becomes a stored
   object with a reference that lies about its length. That got sharper once the
   hydrator stopped re-encoding: the corrupt bytes now reach the object store
   verbatim rather than being resynced away by an intermediate transcode.
   Nothing in the pipeline compares measured duration against the reference's
   claim — except `duration_rms`, which reports both, which is now the
   end-to-end test of that decision.

### Observations that are not bugs, but will bite

- **A function cannot see its own attempt count.** §5.1 hands `process` only
  `(ref, audio)`, and the attempt lives on the §6 envelope. So a function cannot
  say "this is my last attempt, return something degraded rather than nothing".
  `flaky_analyzer` needed a marker file on disk to fail once and then succeed.
- **A dead producer loses queued records.** The crash above logged
  `Producer terminating with 1 message still in queue`. Whatever the crash, the
  shutdown path should flush.

## Monitoring

`compose/` now brings up Prometheus, Grafana and a console alongside the stack.

- **Grafana** (`:3000`) — fleet and per-function dashboards, provisioned from
  JSON in the repo, plus §12's alert rules at 1h and 3h.
- **The console** (`:8000`) — read-only, and the half Grafana is bad at: trace one
  `call_id` through its whole life, browse the DLQ's *contents* rather than its
  rate, see which declarations exist and whether they agree with the topics.
- **The console can act, behind two flags.** A sandbox that runs a function
  against real corpus audio (arbitrary code execution, off by default), and
  write actions — replay, pause, and committing edits to a branch — audited
  before the fact. Edits use git plumbing so the working tree and HEAD are never
  touched, and nothing reaches a running pod.
- **Per-call events** — the SDK emits one event per lifecycle transition over
  OTLP; a collector puts them in OpenSearch, and the console reads them. The
  collector is the swap point: replacing OpenSearch is an exporter change in
  `compose/init/otel-collector.yaml`, not an image rebuild.

There is no results sink and there will not be one: Kafka is the sink, and some
other service reads the results topic later. That makes log retention the de
facto history of what the platform did, which is a product decision rather than
an ops one — Kafka keeps 48h and the object store 24h.

**Metrics and events answer different questions and neither substitutes for the
other.** A metric cannot carry `call_id` — the label would multiply the series
count by the number of calls — and a log pipeline drops lines under pressure by
design, so §12's paging thresholds stay on the metrics path. Consumer lag in
particular can never come from logs: it is the gap between a committed offset
and a high water mark, which no pod is in a position to emit.

Two things it deliberately does not do: edit declarations (§8's "one PR, zero
infra tickets" only holds while git is the single source of truth) and replay,
pause or backfill (write paths need authentication and an audit trail first).

### What this turned up

1. **The §5.5 metrics were going nowhere.** `OTelMetrics` was implemented and
   never instantiated: `bootstrap.py` defaulted to `NullMetrics`, no image
   installed the `metrics` extra, and there was no Prometheus. Every runner had
   been computing lag, latency, realtime multiple, DLQ rate, in-flight depth and
   retry count and discarding all of it. `metrics.from_env` fixes it, with
   `NullMetrics` still the default.
2. **kafka-exporter reports lag as -1 for uncommitted partitions.** Its encoding
   of "unknown", not a measurement. On a 200-partition topic most partitions are
   empty on any given run, so a naive sum goes negative — and a negative backlog
   never crosses 3600, so every lag alert would have sat silently at zero for
   ever. Clamped in the recording rules and in the console's `fleet()`.
3. **The console's first scan implementation could truncate a trace.** Treating a
   `None` poll as end-of-partition conflates "no more records" with "the
   assignment is not ready yet", and the second is normal for the first few
   hundred milliseconds. It now reads to the high water mark, which also took a
   call lookup from 20s to 4s because empty DLQ partitions stopped costing a
   poll timeout each.

### What the log path turned up

4. **Every event was stamped 1970-01-01.** `LogRecord(timestamp=None)` sends no
   timestamp, and the backend then indexes at the epoch — so every time-range
   query excluded everything and "most recent first" was arbitrary. Invisible
   while events were only being counted; obvious the moment they were queried.
5. **`event.name` comes back flat and is queried nested.** OpenSearch expands a
   dotted field name into an object *in the mapping*, so a query uses
   `attributes.event.name`, but `_source` returns the key exactly as indexed —
   still flat, still containing a dot. Reading only the nested form failed
   silently: the query matched, 25 events came back, and every one parsed as an
   unknown event type, so a healthy call rendered as "does not exist".

Also emitted for the first time: `faas.max_poll_exceeded`. `ConfluentConsumer`
has always counted evictions and `kafka.py` said the number "belongs on a
dashboard next to consumer lag"; nothing carried it out of the process.

## Deploying (deploy/)

A Helm chart whose per-function values are **generated** from the declarations
by `scripts/generate_values.py`, and one image per function built by
`scripts/build_images.py` from the tags the declarations name. Neither is
tidiness: §8's "one PR, zero infra tickets" ends at the first hand-written
Deployment or hand-typed image tag. [`deploy/AGENT_GUIDE.md`](deploy/AGENT_GUIDE.md)
is the step-by-step procedure; [`deploy/README.md`](deploy/README.md) is the
reference.

Three things the SDK gained for it:

- **Readiness and liveness** (`health.py`). Liveness asks whether the poll loop
  is turning; readiness asks whether the pod holds partitions. Conflating them
  restarts a pod that is legitimately grinding through a 5-minute file, which
  turns one slow call into a redelivered one.
- **A per-function termination grace period**, derived from the declaration's
  own timeout and drain budget. OpenShift's 30s default would SIGKILL the
  hydrator mid-file on every rollout.
- **The `assigned` log line is a summary now** — `faas.audio.internal[0-199]
  (200)` instead of two hundred repr'd objects per rebalance, which was
  recorded as a known annoyance and is fixed.

## Decisions worth knowing about

**The hydrator runs on the SDK's runner, not its own loop.** Fetching a file
and storing it again is seconds of work between polls — the same §5.2 problem a
function has. Only the two ends differ: `JsonSourceDecoder` replaces the
reference decoder and `ReferenceEmitter` publishes to the internal topic. Cost:
one constructor argument (`decoder=`) on `FunctionRunner`.

**The hydrator does not transcode.** Encoding to canonical FLAC happens
upstream of the Audio API now, so `process()` is fetch, verify the STREAMINFO
header against canonical form, store, publish — no subprocess, no scratch
files, no codec dependency at all. The verification step still exists and still
rejects anything non-canonical as poison: the Audio API is a service the
hydrator does not control, and a regression there should die at the hydrator's
DLQ rather than reach every function silently.

**`process()` returns `FunctionResult`, not §5.1's `Result`.** §6's `Result` is
the wire envelope the SDK stamps with offsets, attempts and timestamps.
Returning it from `process` would leak the envelope into every function and
erode the §6 envelope/payload split.

**Wire types are dataclasses behind a `Codec` seam.** `ProtobufCodec` is the
production path and `bootstrap.py` defaults to it; `JsonCodec` stays for local
dev. `tests/test_codec_swap.py` runs the whole runner over both.

**`run()` takes the function class, not an instance** — see bug 2 under
"Bugs found by testing".

**`Status.SUCCESS` is 1, deviating from §6's 0.** The one place the wire does
not match the document, and deliberate: proto3 has no presence for enums, so
with the spec's numbering an unset `status` decodes as `SUCCESS` and a partial
write reads as "this call succeeded" — the wrong failure direction for the field
gating whether downstream trusts a payload. `STATUS_UNSPECIFIED = 0` now holds
the zero value, both codecs treat unspecified *and* unrecognised statuses as
poison rather than guessing, and neither will encode one. Done 2026-08-14 while
the topic was empty; it is a wire-breaking migration from here.

## Bugs found by testing, and fixed

Three came out of the broker suite, one out of the hydrator work. Recorded
because the reasoning matters more than the diffs.

1. **`ConfluentConsumer.poll` crashed the pod on eviction.** It raised
   `RuntimeError` for any non-EOF error, but `_MAX_POLL_EXCEEDED` is an event,
   not a failure — librdkafka rejoins by itself. Turning a recoverable eviction
   into a crash drops in-flight work. Now counted (`max_poll_exceeded`) and
   logged loudly.
2. **`bootstrap.py` defaulted to a pool that reintroduced the §5.2 bug.**
   `InlineWorkerPool` runs `process()` synchronously inside `submit()`, so work
   happens on the poll thread. Against a real broker it is evicted. Default is
   now `ProcessWorkerPool`. The hydrator keeps an inline pool deliberately: its
   work is one HTTP GET and one S3 PUT, each under its own client timeout, so
   the block is provably inside `max.poll.interval.ms`, whereas a function's
   timeout is enforced *between* poll iterations and cannot fire while inline
   work blocks.
3. **The murmur2 test compared against the wrong hash.** Implementation was
   right; librdkafka's `consistent` partitioner is CRC32, and only
   `murmur2`/`murmur2_random` are Java-compatible. Agrees on all 69 keys now.
4. **The hydrator's store-before-publish invariant had no real test.** The one
   that existed could not fail — moving the PUT inside `process()` does not
   break anything, because the guarantee is structural. The meaningful version
   (a *failed* PUT publishes nothing) was missing and is now in
   `tests/hydrator/test_pipeline.py`.

Load-bearing logic was mutation-checked rather than trusted: removing
`min(pending)` from the ledger fails 4 tests, disabling backpressure fails 3.

---

## Next

### P0 — close before step 5

- **A real rebalance test.** Two consumers in a group, partitions moving,
  `_on_revoke` draining in-flight work under a live coordinator. Still on fakes
  in the suite, but no longer unobserved: the stress run SIGKILLed a function
  and SIGTERMed a hydrator mid-flight, and the accounting came back with one
  duplicate and no losses. That is evidence, not a test -- it is not
  reproducible in CI, and it did not cover partitions moving between two live
  consumers of the same group.
- **CI.** Nothing runs automatically. Needs: unit suite on every push, `buf lint`
  and `buf breaking`, broker suite on changes to `kafka.py`/`runner.py` or
  nightly. Both buf checks now run clean locally against buf 1.72 (`buf breaking`
  flags the enum renumbering above against `master`, as it should — that is the
  one intended break).

### P1 — the next build-order step

- **Step 5, results sink.** Functions do not own database connections (§4.4).
- **Decide the hydration-failure gap.** A call that fails hydration is invisible
  downstream: no reference, so no function sees it and the aggregator never
  expects it. The DLQ holds the input for replay — the spec's answer — but
  nothing distinguishes "not hydrated yet" from "never will be", and a
  dead-letter topic is not something downstream can watch. Closing it means a
  hydration-failure topic the aggregator honours. Design decision, deliberately
  not invented. See `ReferenceEmitter.emit_failure`.
- **Point `JsonSourceDecoder` at the real input schema.** §4.1 defines no format,
  so the field names are constructor arguments and currently guessed.

### P2 — known limits, none blocking

- **Timeout cancellation is best-effort for a running job.** `ProcessWorkerPool`
  cannot interrupt a worker mid-file without killing the process; it abandons the
  outcome so the runner can retry or DLQ, but the worker burns CPU until it
  returns. Until 2026-08-14 this note understated it: the runner treated the
  abandoned slot as free and crashed on the next submit. Fixed (see the stress
  run above); the wasted CPU remains, and a saturated `slow_burner` still holds
  its own partitions' lag while every other function stays at zero.
- **A retry in backoff holds its partition's low-water mark.** Correct — the file
  is unfinished — but a long backoff delays commits for everything behind it on
  that partition. Watch once real retry rates exist.
- **Consumer lag is not emitted by the SDK.** `kafka-exporter` is the source of
  truth and feeds the autoscaler (§5.5, §11); the SDK-side gauge is a cross-check
  that needs a broker to be worth writing.
- **Decoding is lossy for unknown fields.** Messages land in dataclasses, so a
  field added by a newer producer survives the read but not a re-encode. Fine for
  the SDK — the runner only decodes references and only encodes results it built
  — but anything proxying a message must work on the protobuf object.
- **`test_the_slow_function_burns_roughly_what_it_promises` is timing-sensitive.**
  It failed once during a full-suite run on 2026-08-14 while the compose stack --
  including a CPU-burning `slow_burner` container -- was running on the same
  machine, and passed on every run since, in isolation and in the full suite.
  It asserts on wall-clock work, so it is inherently contention-sensitive.
  Recorded rather than papered over: the next time it fails, the useful thing is
  knowing it has happened before and under what conditions.
- **The broker suite takes ~4min**, dominated by idle-detection waits in the
  redelivery helpers. Worth tightening if it becomes a CI annoyance.
- **OpenShift gotchas (§11) are unaddressed.** No Dockerfiles yet. The random-UID
  scratch-path problem now lands on the console sandbox rather than the
  hydrator — its child-process temp files need a directory group-writable by
  GID 0.
